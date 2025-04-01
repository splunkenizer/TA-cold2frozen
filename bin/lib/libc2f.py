from __future__ import print_function
from lib import libdir
from lib import libs3
import sys, os, gzip, shutil, subprocess
import socket
import time
from datetime import datetime, timedelta
import logging
from io import open
logger = logging.getLogger('splunk.cold2frozen')

def verifySplunkHome():
    # Verify SPLUNK_HOME environment variable is available, the script is expected to be launched by Splunk which
    #  will set this for debugging or manual run, please set this variable manually
    try:
        os.environ["SPLUNK_HOME"]
    except KeyError:
        print('The environment variable SPLUNK_HOME could not be verified, if you want to run this script '
                    'manually you need to export it before processing')
        sys.exit(1)

def readConfig(app_path,config_file="cold2frozen.conf"):
    import configparser

    # Discover app path
    # app name
    APP = os.path.basename(app_path)
    logger.debug('Appname: %s' % APP)

    # Check if there is a path given
    if config_file.find('/')!=-1:
        msg = 'Custom config file (%s) must reside in the app path. Please specify filename only.' % (config_file)
        logger.error(msg)
        sys.exit(msg)  

    # Get config
    config = configparser.RawConfigParser()
    default_config_inifile = os.path.join(app_path, "default", config_file)
    config_inifile = os.path.join(app_path, "local", config_file)

    # First read default config
    logger.debug('Reading config file: %s' % default_config_inifile)
    config.read(default_config_inifile)

    # Check config exists
    if not os.path.isfile(config_inifile):
        msg = 'Please configure your setting by creating and configuring %s' % (config_inifile)
        logger.error(msg)
        sys.exit(msg)    

    # Then read local config
    logger.debug('Reading config file: %s' % config_inifile)
    config.read(config_inifile)

    # Add the app_path to it
    config.add_section('Internal')
    config.set('Internal', 'APP_PATH', app_path)

    return config

def connStorage(config):
    CONFIG_SECTION = "cold2frozen"
    ARCHIVE_TYPE = config.get(CONFIG_SECTION, "ARCHIVE_TYPE")
    APP_PATH = config.get("Internal", "APP_PATH")
    if ARCHIVE_TYPE == "dir":
        ARCHIVE_DIR = config.get(CONFIG_SECTION, "ARCHIVE_DIR")
        storage = libdir.c2fDir(ARCHIVE_DIR)
    elif ARCHIVE_TYPE == "s3":
        kwargs = {}
        kwargs['s3_bucket'] = config.get(CONFIG_SECTION, "S3_BUCKET")
        if "s3_endpoint" in dict(config.items(CONFIG_SECTION)):
            kwargs['s3_endpoint'] = config.get(CONFIG_SECTION, "S3_ENDPOINT")
        if "s3_verify_cert" in dict(config.items(CONFIG_SECTION)):
            s3_verify_cert = config.get(CONFIG_SECTION, "S3_VERIFY_CERT")
            if s3_verify_cert != "False":
                if s3_verify_cert.find('/')==-1:
                    s3_verify_cert = os.path.join(APP_PATH, "certs", s3_verify_cert)
                if not os.path.isfile(s3_verify_cert):
                    msg = "Value '%s' for S3_VERIFY_CERT not supported, must be 'False' or a readable pem file." % s3_verify_cert
                    logger.error(msg)
                    raise Exception(msg)
                else:
                    kwargs['s3_verify_cert'] = s3_verify_cert
            else:
                kwargs['s3_verify_cert'] = eval(s3_verify_cert)
        kwargs['access_key'] = config.get(CONFIG_SECTION, "ACCESS_KEY")
        kwargs['secret_key'] = config.get(CONFIG_SECTION, "SECRET_KEY")
        kwargs['archive_dir'] = config.get(CONFIG_SECTION, "ARCHIVE_DIR")
        storage = libs3.c2fS3(**kwargs)
    else:
        msg = 'Given ARCHIVE_TYPE=%s is not supported' % ARCHIVE_TYPE
        logger.error(msg)
        sys.exit(msg)
    
    return storage

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

def getBucketSizeTarget(storage, bucketPath):
    size = storage.bucket_size(bucketPath)
    return size

def getBucketSizeRaw(bucketPath):
    size = -1
    rawSizeFile = os.path.join(bucketPath,".rawSize")
    if os.path.isfile(rawSizeFile):
        with open(rawSizeFile, "r") as f:
            logger.debug("Getting raw size for bucket %s" % bucketPath)
            size = f.read().rstrip()
            size = size if size.isdigit() else -1
    return int(size)

def bucketDir(storage, bucketPath):
    full_bucket_path = storage.bucket_dir(bucketPath)
    return full_bucket_path

def getLock(storage, lock_file, timeout=2):
    """ False if lock_file was locked, True otherwise """
    giveUp = datetime.utcnow() + timedelta(seconds=timeout)
    while datetime.utcnow() < giveUp:
        if not storage.check_lock_file(lock_file):
            if storage.write_lock_file(lock_file, getHostName()):
                return True
        else:
            time.sleep(1)
    lock_host = storage.read_lock_file(lock_file)
    logger.debug("Lock aquire timed out for lockfile (lockhost: %s) %s" % (lock_host,lock_file))
    lock_file_age = storage.lock_file_age(lock_file)
    max_age = 3600 # 1h
    if time.time() - lock_file_age > max_age:
        msg = 'Found a very old (>%s secs) lockfile from host %s: %s' % (max_age,lock_host,lock_file)
        logger.error(msg)
        sys.exit(msg)        
    return False

def releaseLock(storage, lock_file):
    storage.remove_lock_file(lock_file)

# Always release lock on exit
def exitCleanup(storage, lock_file):
    releaseLock(storage, lock_file)

# For new style buckets (v4.2+), we can remove all files except for the rawdata.
# We can later rebuild all metadata and tsidx files with "splunk rebuild"
def handleNewBucket(base, files):
    logger.debug('Cleanup bucket=%s, type=normal' % base)
    # the only non file is the rawdata folder, which we want to archive
    for f in files:
        full = os.path.join(base, f)
        if os.path.isfile(full) and not os.path.basename(f).startswith('journal.'):
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

def indexExists(storage, indexname):
    if storage.index_exists(indexname):
        return True
    else:
        return False

def createIndex(storage, indexname):
    storage.create_index_dir(indexname)

def bucketExists(storage, bucket_dir):
    if storage.bucket_exists(bucket_dir):
        return True
    else:
        return False

def copyBucket(storage, bucket, destdir):
    storage.bucket_copy(bucket, destdir)    

def listIndexes(storage):
    return storage.list_indexes() 

def listBuckets(storage, index):
    return storage.list_buckets(index)   

def restoreBucket(storage, index, bucket_name, destdir):
    storage.restore_bucket(index,bucket_name,destdir)

def removeBucket(storage, index, bucket_name):
    storage.remove_bucket(index,bucket_name)

class logDict(dict):
    # __init__ function 
    def __init__(self): 
        self.__logevent = dict() 
          
    # Function to add key:value 
    def add(self, key, value): 
        self.__logevent[key] = value 

    # Function to return kv list of fields
    def kvout(self):
        kvarray = []
        for key, value in list(self.__logevent.items()):
            kvarray.append("%s=%s" % (key, value))
        return ", ".join(kvarray)

