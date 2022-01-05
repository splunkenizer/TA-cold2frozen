#!/usr/bin/env python3

# Purpose:
# This is a frozen script for Splunk which purpose is to archive frozen buckets into a shared directory
# In a context of clustered indexers, this avoids archiving buckets that were archived by another peer already due to bucket replication

# To use this script you need to configure in a local/cold2frozen.conf the following information:
# ARCHIVE_DIR = Full qualified directory path, where the buckets should be archived to

# exit code:
# exit 0 if copying of bucket was successfull, Splunk proceed to the purge of the frozen bucket
# exit 1 if copying of bucket has failed, Splunk will re-attempt continously to achive the bucket

# To set the logging level higher add this to /opt/splunk/etc/log-local.cfg
# [python]
# splunk.cold2frozen = DEBUG

import sys, os, gzip, shutil, subprocess
import socket
import re
import configparser
import logging, logging.handlers
import time
from datetime import datetime, timedelta
import atexit
import splunk

# Create logger
def setup_logging():
    INSTANCE_NAME = os.path.basename(__file__).split(".")[0].lower()
    logger = logging.getLogger('splunk.%s' % INSTANCE_NAME)
    SPLUNK_HOME = os.environ['SPLUNK_HOME']
    LOGGING_DEFAULT_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log.cfg')
    LOGGING_LOCAL_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log-local.cfg')
    LOGGING_STANZA_NAME = 'python'
    LOGGING_FILE_NAME = "%s.log" % INSTANCE_NAME
    BASE_LOG_PATH = os.path.join('var', 'log', 'splunk')
    splunk_log_handler = logging.handlers.RotatingFileHandler(os.path.join(SPLUNK_HOME, BASE_LOG_PATH, LOGGING_FILE_NAME), mode='a')
    # Use the same format as splunkd.log
    LOGGING_FORMAT = logging.Formatter("%(asctime)s %(levelname)-s  %(module)s - %(message)s", "%m-%d-%Y %H:%M:%S.%03d %z")
    splunk_log_handler.setFormatter(LOGGING_FORMAT)
    logger.addHandler(splunk_log_handler)
    splunk.setupSplunkLogger(logger, LOGGING_DEFAULT_CONFIG_FILE, LOGGING_LOCAL_CONFIG_FILE, LOGGING_STANZA_NAME)
    return logger

logger = setup_logging()

# Enable for debugging
#logger.setLevel(logging.DEBUG)

# Logging examples
# logger.debug('debug message')
# logger.info('info message')
# logger.warning('warn message')
# logger.error('error message')
# logger.critical('critical message')

class logDict(dict):
    # __init__ function 
    def __init__(self): 
        self = dict() 
          
    # Function to add key:value 
    def add(self, key, value): 
        self[key] = value 

    # Function to return kv list of fields
    def kvout(self):
        kvarray = []
        for key, value in self.items():
            kvarray.append("%s=%s" % (key, value))
        return ", ".join(kvarray)

logFields = logDict()

# Verify SPLUNK_HOME environment variable is available, the script is expected to be launched by Splunk which
#  will set this for debugging or manual run, please set this variable manually
try:
    os.environ["SPLUNK_HOME"]
except KeyError:
    print('The environment variable SPLUNK_HOME could not be verified, if you want to run this script '
                  'manually you need to export it before processing')
    sys.exit(1)
SPLUNK_HOME = os.environ['SPLUNK_HOME']

# Discover app path
# app name
APP = os.path.basename(os.path.dirname(os.path.dirname(__file__)))
logger.debug('Appname: %s' % APP)
CONFIG_FILE = "cold2frozen.conf"

# Get config
config = configparser.RawConfigParser()
default_config_inifile = os.path.join(os.path.dirname(os.path.dirname(__file__)), "default", CONFIG_FILE)
config_inifile = os.path.join(os.path.dirname(os.path.dirname(__file__)), "local", CONFIG_FILE)

# First read default config
logger.debug('Reading config file: %s' % default_config_inifile)
config.read(default_config_inifile)

# Get default allowed custom values
ARCHIVE_DIR = config.get("cold2frozen", "ARCHIVE_DIR")

# Check config exists
if not os.path.isfile(config_inifile):
    msg = 'Please configure your setting by creating and configuring %s' % (config_inifile)
    logger.error(msg)
    sys.exit(msg)    

# Then read local config
logger.debug('Reading config file: %s' % config_inifile)
config.read(config_inifile)

# Handles values
ARCHIVE_DIR = config.get("cold2frozen", "ARCHIVE_DIR")

# Check directory exists
if not os.path.isdir(ARCHIVE_DIR):
    msg = 'Directory %s for frozen buckets does not exist' % ARCHIVE_DIR
    print(msg)
    sys.exit(1)

# Check permissions of the directory
if not os.access(ARCHIVE_DIR, os.W_OK):
    msg = 'Cannot write to directory %s' % ARCHIVE_DIR
    logger.error(msg)
    sys.exit(msg)

def getHostName():
    # Get the local hostname from the networking stack and split of the domainname if it exists
    localHostname = socket.gethostname().split(".")[0]
    return localHostname

def getBucketSize(bucketPath):
    size = 0
    for path, dirs, files in os.walk(bucketPath):
        for file in files:
            filepath = os.path.join(path, file)
            logger.debug("Getting size for file %s" % filepath)
            size += os.path.getsize(filepath)
    return size

def getLock(lock_file, timeout=2):
    """ False if lock_file was locked, True otherwise """
    giveUp = datetime.utcnow() + timedelta(seconds=timeout)
    while datetime.utcnow() < giveUp:
        if not os.path.isfile(lock_file):
            with open(lock_file, 'w') as file:
                file.write(getHostName())
                logger.debug("Created lockfile %s" % lock_file)
                return True
        else:
            time.sleep(1)
    with open(lock_file, "r") as f:
            lock_host = f.read().rstrip()
    logger.debug("Lock aquire timed out for lockfile (lockhost: %s) %s" % (lock_host,lock_file))
    lock_file_age = os.path.getmtime(lock_file)
    max_age = 3600 # 1h
    if time.time() - lock_file_age > max_age:
        msg = 'Found a very old (>%s secs) lockfile from host %s: %s' % (max_age,lock_host,lock_file)
        logger.error(msg)
        sys.exit(msg)        
    return False

def releaseLock(lock_file):
    if os.path.isfile(lock_file):
        os.remove(lock_file)
        logger.debug("Removed lockfile %s" % lock_file)

# Always release lock on exit
def exitCleanup(lock_file):
    releaseLock(lock_file)

# For new style buckets (v4.2+), we can remove all files except for the rawdata.
# We can later rebuild all metadata and tsidx files with "splunk rebuild"
def handleNewBucket(base, files):
    logger.debug('Cleanup bucket=%s, type=normal' % base)
    # the only non file is the rawdata folder, which we want to archive
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full):
            logger.debug('Removing file %s' % full)
            os.remove(full)


# For buckets created before 4.2, simply gzip the tsidx files
# To thaw these buckets, be sure to first unzip the tsidx files
def handleOldBucket(base, files):
    logger.debug('Cleanup bucket=%s, type=old-style' % base)
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full) and (f.endswith('.tsidx') or f.endswith('.data')):
            fin = open(full, 'rb')
            fout = gzip.open(full + '.gz', 'wb')
            fout.writelines(fin)
            fout.close()
            fin.close()
            logger.debug('Removing file %s' % full)
            os.remove(full)


# This function is not called, but serves as an example of how to do
# the previous "flatfile" style export. This method is still not
# recommended as it is resource intensive
def handleOldFlatfileExport(base, files):
    command = ['exporttool', base, os.path.join(base, 'index.export'), 'meta::all']
    retcode = subprocess.call(command)
    if retcode != 0:
        sys.exit('exporttool failed with return code: ' + str(retcode))

    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full):
            os.remove(full)
        elif os.path.isdir(full):
            shutil.rmtree(full)
        else:
            print('Warning: found irregular bucket file: ' + full)

if __name__ == "__main__":
    searchable = False
    if len(sys.argv) < 2:
        sys.exit('usage: python3 %s <bucket_dir_to_archive> [--search-files-required]' % os.path.basename(__file__))

    bucket = sys.argv[1]

    if not os.path.isdir(bucket):
        msg = 'Given bucket is not a valid directory: %s' % bucket
        logger.error(msg)
        sys.exit(msg)

    logFields.add('status', None)

    ##
    # Whether search files are required to be preserved for this bucket ("False" if not present)
    #
    # Search Files are required for bucket that doesn't have any usable rawdata. For instance, "metric"
    # type buckets that have rawdata journal files stubbed-out, DO NOT contain a usable journal file,
    # and hence must have their search files preserved for data recovery.
    #
    # For more details, look at "metric.stubOutRawdataJournal" config in indexes.conf.spec
    ##
    searchFilesRequired = False
    if len(sys.argv) > 2:
        if '--search-files-required' in sys.argv[2:]:
            searchFilesRequired = True

    rawdatadir = os.path.join(bucket, 'rawdata')
    if not os.path.isdir(rawdatadir):
        msg = 'No rawdata directory, given bucket is likely invalid: ' + bucket
        logger.error(msg)
        sys.exit(msg)

    # Strip off ending /
    if bucket.endswith('/'):
        logger.debug("bucket=%s has trailing /" % bucket)
        bucket = bucket[:-1]

    logFields.add('bucket', bucket)

    indexname = os.path.basename(os.path.dirname(os.path.dirname(bucket)))
    logFields.add('indexname', indexname)
    logger.debug("indexname is %s" % indexname)

    peer_name = getHostName()
    logger.debug("peername is %s" % peer_name)

    bucket_name = bucket.split("/")[-1]
    logFields.add('bucketname', bucket_name)
    logger.debug("bucketname is %s" % bucket_name)

    # Get bucket UTC epoch start and UTC epoch end
    buckets_info = bucket_name.split('_')
    bucket_epoch_start, bucket_epoch_end = "null", "null"
    bucket_epoch_end = buckets_info[1]
    logFields.add('bucketend', bucket_epoch_end)
    bucket_epoch_start = buckets_info[2]
    logFields.add('bucketstart', bucket_epoch_start)
    logger.debug("bucket_epoch_start is %s" % bucket_epoch_start)
    logger.debug("bucket_epoch_end is %s" % bucket_epoch_end)    

    bucket_name_prefix = bucket_name.split("_")[0]
    logFields.add('bucketprefix', bucket_name_prefix)
    logger.debug("bucket_name_prefix is %s" % bucket_name_prefix)
    normalized_bucket_name_array = bucket_name.split("_")[1:]

    clustered=1
    if len(normalized_bucket_name_array) == 3:
        # This means it's a non replicated bucket, so need to grab the GUID from instance.cfg
        logger.debug("This means it's a non replicated bucket, so need to grab the GUID from instance.cfg")
        with open(os.path.join(SPLUNK_HOME, 'etc/instance.cfg'), "r") as f:
            read_data = f.read()
            match = re.search(r'^guid = (.*)', read_data, re.MULTILINE)
            original_peer_guid = match.group(1)
            normalized_bucket_name_array.append(original_peer_guid)
            clustered=0
    elif len(normalized_bucket_name_array) == 4:
        logger.debug("This means it's a replicated bucket, we'll grab the GUID from the bucket name")
        # This means it's a replicated bucket, we'll grab the GUID from the bucket name
        original_peer_guid = normalized_bucket_name_array[3]
    else:
        msg = 'Bucket directory naming not correct: %s' + bucket_name
        logger.error(msg)
        sys.exit(msg)

    logFields.add('peerguid', original_peer_guid)
    logger.debug("original_peer_guid is %s" % original_peer_guid)

    bucket_id = normalized_bucket_name_array[2]
    logFields.add('bucketid', bucket_id)

    logFields.add('searchfiles', searchFilesRequired)

    normalized_bucket_name = "_".join(normalized_bucket_name_array)
    logFields.add('buckename_norm', normalized_bucket_name)
    logger.debug("normalized_bucket_name is %s" % normalized_bucket_name)

    destdir = os.path.join(ARCHIVE_DIR, indexname, "_".join([bucket_name_prefix] + normalized_bucket_name_array))
    logFields.add('destdir', destdir)
    logger.debug("destdir is %s" % destdir)

    lock_file = os.path.join(os.path.dirname(destdir), normalized_bucket_name + ".lock")
    logger.debug("lock_file is %s" % lock_file)

    # Create index directory, if needed
    if not os.path.isdir(os.path.join(ARCHIVE_DIR, indexname)):
        logger.debug("Creating index directory %s" % logger.debug("lock_file is %s" % lock_file))
        os.mkdir(os.path.join(ARCHIVE_DIR, indexname))

    # Get the lock
    if getLock(lock_file, timeout=10):
        atexit.register(exitCleanup, lock_file)

        # Strip of unneeded metadata files
        files = os.listdir(bucket)
        logger.debug("Filelist %s" % files)
        journal_gz = os.path.join(rawdatadir, 'journal.gz')
        journal_zst = os.path.join(rawdatadir, 'journal.zst')
        logger.debug("is it gz? %s" % os.path.isfile(journal_gz))
        logger.debug("is it zst? %s" % os.path.isfile(journal_zst))

        # Bucket size in bytes        
        bucket_size_full = getBucketSize(bucket)
        logFields.add('bucketsize_full_b', bucket_size_full)
        stripstart = time.time() * 1000
        if os.path.isfile(journal_zst) or os.path.isfile(journal_gz):
            if not searchFilesRequired:
                handleNewBucket(bucket, files)
            else:
                logger.debug('Argument "--search-files-required" is specified. Skipping deletion of search files !')
        else:
            handleOldBucket(bucket, files)

        stripend = time.time() * 1000
        logFields.add('striptime_ms', round(stripend - stripstart,3))

        # Bucket size in bytes
        bucket_size = getBucketSize(bucket) 
        logFields.add('bucketsize_b', bucket_size)

        # Check if bucket has been transfered already, we need to cover both db and rb prefixes
        bucket_exists = False
        destdir_db = os.path.join(ARCHIVE_DIR, indexname, "_".join(['db'] + normalized_bucket_name_array))
        destdir_rb = os.path.join(ARCHIVE_DIR, indexname, "_".join(['rb'] + normalized_bucket_name_array))
        if os.path.isdir(destdir_db):
            bucket_exists = destdir_db
        elif os.path.isdir(destdir_rb):
            bucket_exists = destdir_rb

        logFields.add('copytime_ms', 0)
        if bucket_exists:
            logger.debug('Warning: This bucket already exists as %s' % bucket_exists)
            logFields.add('status', 'existed')

            # Bucket size in bytes
            bucket_size_target = getBucketSize(bucket_exists)
            logger.debug("bucket_size is %s, bucket_size_target is %s" % (bucket_size, bucket_size_target))

            if bucket_size != bucket_size_target:
                msg = 'Bucket exists but sizes differ bucket=%s (size=%s) targetbucket=%s (targetsize=%s)' % (bucket, bucket_size, destdir, bucket_size_target)
                logger.error(msg)
                sys.exit(msg)

        else:
            copystart = time.time() * 1000
            try:
                shutil.copytree(bucket, destdir)
                logFields.add('status', 'archived')
            except OSError:
                msg = 'Failed to copy bucket %s to destination %s' % (bucket, destdir)
                logger.error(msg)
                sys.exit(msg)
            
            copyend = time.time() * 1000
            logFields.add('copytime_ms', round(copyend - copystart, 3))
            
    else:
        logFields.add('status', 'lock_timeout')

    logger.info(logFields.kvout())
    sys.exit()

