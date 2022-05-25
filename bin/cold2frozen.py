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

from lib import libc2f
from lib import libdir
import sys, os
import re
import logging
import time
import atexit

# Verify SPLUNK_HOME
libc2f.verify_splunk_home()
SPLUNK_HOME = os.environ['SPLUNK_HOME']

# Create Logger
from lib import liblogger
logger = liblogger.setup_logging('splunk.cold2frozen')

def main():

    # Define the App Path
    app_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    #TODO: Remove for prod
    logger.setLevel(logging.DEBUG)

    logger.debug('Starting main()')

    # Argument Parser
    if len(sys.argv) < 2:
        sys.exit('usage: python3 %s <bucket_dir_to_archive> [--search-files-required]' % os.path.basename(__file__))

    bucket = sys.argv[1]

    # Check Arguments
    if not os.path.isdir(bucket):
        msg = 'Given bucket is not a valid directory: %s' % bucket
        logger.error(msg)
        sys.exit(msg)

    # Create logFields Object
    logFields = libc2f.logDict()

    # Read in config file
    config = libc2f.read_config(app_path)
    ARCHIVE_TYPE = config.get("cold2frozen", "ARCHIVE_TYPE")

    if ARCHIVE_TYPE == "dir":
        ARCHIVE_DIR = config.get("cold2frozen", "ARCHIVE_DIR")
        storage = libdir.c2fDir(ARCHIVE_DIR)

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

    peer_name = libc2f.getHostName()
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

    if len(normalized_bucket_name_array) == 3:
        # This means it's a non replicated bucket, so need to grab the GUID from instance.cfg
        logger.debug("This means it's a non replicated bucket, so need to grab the GUID from instance.cfg")
        with open(os.path.join(SPLUNK_HOME, 'etc/instance.cfg'), "r") as f:
            read_data = f.read()
            match = re.search(r'^guid = (.*)', read_data, re.MULTILINE)
            original_peer_guid = match.group(1)
            normalized_bucket_name_array.append(original_peer_guid)
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

    destdir = os.path.join(indexname, "_".join([bucket_name_prefix] + normalized_bucket_name_array))
    full_destdir = libc2f.bucketDir(storage, destdir)
    logFields.add('destdir', full_destdir)
    logger.debug("destdir is %s" % full_destdir)

    lock_file = os.path.join(os.path.dirname(destdir), normalized_bucket_name + ".lock")

    # Create index directory, if needed
    libc2f.createIndex(storage, indexname)

    # Get the lock
    if libc2f.getLock(storage, lock_file, timeout=10):
        atexit.register(libc2f.exitCleanup, storage, lock_file)

        # Strip of unneeded metadata files
        files = os.listdir(bucket)
        rawdatafiles = os.listdir(os.path.join(bucket,rawdatadir))
        logger.debug("Filelist %s" % files)
        journal_gz = os.path.join(rawdatadir, 'journal.gz')
        journal_zst = os.path.join(rawdatadir, 'journal.zst')
        logger.debug("is it gz? %s" % os.path.isfile(journal_gz))
        logger.debug("is it zst? %s" % os.path.isfile(journal_zst))

        # Bucket raw size in bytes
        bucket_size_raw = libc2f.getBucketSizeRaw(bucket)
        if bucket_size_raw >= 0:
            logFields.add('bucketsize_raw_b', bucket_size_raw)

        # Bucket size in bytes        
        bucket_size_full = libc2f.getBucketSize(bucket)
        logFields.add('bucketsize_full_b', bucket_size_full)
        stripstart = time.time() * 1000
        if os.path.isfile(journal_zst) or os.path.isfile(journal_gz):
            if not searchFilesRequired:
                libc2f.handleNewBucket(bucket, files)
                libc2f.handleNewBucket(os.path.join(bucket,rawdatadir), rawdatafiles)
            else:
                logger.debug('Argument "--search-files-required" is specified. Skipping deletion of search files !')
        else:
            libc2f.handleOldBucket(bucket, files)

        stripend = time.time() * 1000
        logFields.add('striptime_ms', round(stripend - stripstart,3))

        # Bucket size in bytes
        bucket_size = libc2f.getBucketSize(bucket) 
        logFields.add('bucketsize_b', bucket_size)

        # Check if bucket has been transfered already, we need to cover both db and rb prefixes
        bucket_exists = False
        destdir_db = os.path.join(indexname, "_".join(['db'] + normalized_bucket_name_array))
        destdir_rb = os.path.join(indexname, "_".join(['rb'] + normalized_bucket_name_array))
        if libc2f.bucketExists(storage, destdir_db):
            bucket_exists = destdir_db
        elif libc2f.bucketExists(storage, destdir_rb):
            bucket_exists = destdir_rb

        logFields.add('copytime_ms', 0)
        if bucket_exists:
            full_bucket_exists = libc2f.bucketDir(storage, bucket_exists)
            logger.debug('Warning: This bucket already exists as %s' % full_bucket_exists)
            logFields.add('status', 'existed')

            # Bucket size in bytes
            bucket_size_target = libc2f.getBucketSizeTarget(storage, bucket_exists)
            logger.debug("bucket_size is %s, bucket_size_target is %s" % (bucket_size, bucket_size_target))

            if bucket_size != bucket_size_target:
                msg = 'Bucket exists but sizes differ bucket=%s (size=%s) targetbucket=%s (targetsize=%s)' % (bucket, bucket_size, full_bucket_exists, bucket_size_target)
                logger.error(msg)
                sys.exit(msg)

        else:
            copystart = time.time() * 1000
            libc2f.copyBucket(storage, bucket, destdir)        
            copyend = time.time() * 1000
            logFields.add('status', 'archived') 
            logFields.add('copytime_ms', round(copyend - copystart, 3))
            
    else:
        logFields.add('status', 'lock_timeout')

    logger.info(logFields.kvout())


if __name__ == "__main__":
    main()
    sys.exit()