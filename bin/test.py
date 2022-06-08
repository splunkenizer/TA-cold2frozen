#!/usr/bin/env python3

# Purpose:
# Test script to test the functions of the storage library

from lib import libc2f
from lib import libdir
from lib import libs3
import sys, os
import logging

# Verify SPLUNK_HOME
libc2f.verify_splunk_home()
SPLUNK_HOME = os.environ['SPLUNK_HOME']

# Create Logger
from lib import liblogger
logger = liblogger.setup_logging('splunk.cold2frozen')

#TODO: Remove for prod
logger.setLevel(logging.DEBUG)

def main():

    # Define the App Path
    app_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    logger.debug('Starting main()')

    # Argument Parser
    if len(sys.argv) < 2:
        sys.exit('usage: python3 %s <bucket_dir_to_archive>' % os.path.basename(__file__))

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

    if ARCHIVE_TYPE == "s3":
        S3_BUCKET = config.get("cold2frozen", "S3_BUCKET")
        ACCESS_KEY = config.get("cold2frozen", "ACCESS_KEY")
        SECRET_KEY = config.get("cold2frozen", "SECRET_KEY")
        ARCHIVE_DIR = config.get("cold2frozen", "ARCHIVE_DIR")
        storage = libs3.c2fS3(ACCESS_KEY, SECRET_KEY, S3_BUCKET, ARCHIVE_DIR)


    # Create index dir
    indexname = "testindex"
    libc2f.createIndex(storage, indexname)

    # Set bucket settings
    bucket_name = "This_is_my_Bucket"
    normalized_bucket_name = bucket_name
    destdir = os.path.join(indexname, bucket_name)
    full_destdir = libc2f.bucketDir(storage, destdir)
    logger.debug("destdir is %s" % full_destdir)

    # Create lockfile
    lock_file = os.path.join(os.path.dirname(destdir), normalized_bucket_name + ".lock")
    libc2f.getLock(storage, lock_file, timeout=10)

    # Try to create another lock file
    libc2f.getLock(storage, lock_file, timeout=10)

    # Copy bucket
    libc2f.copyBucket(storage, bucket, destdir)

    # Remove lockfile
    libc2f.releaseLock(storage, lock_file)

    # Get Bucket Size
    bucket_size_target = libc2f.getBucketSizeTarget(storage, 'testindex/This_is_my_Bucket')
    print("Stored Bucket Size: %s" % bucket_size_target)
    bucket_size = libc2f.getBucketSize(bucket) 
    print("Source Bucket Size: %s" % bucket_size)

if __name__ == "__main__":
    main()
    sys.exit()