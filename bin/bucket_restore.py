#!/usr/bin/env python3

from lib import libc2f
from lib import libbuckets
import os, sys
import argparse
import logging, logging.handlers
import time
from datetime import datetime

# Verify SPLUNK_HOME
libc2f.verifySplunkHome()
SPLUNK_HOME = os.environ['SPLUNK_HOME']

# Create Logger
from lib import liblogger
logger = liblogger.setup_logging('splunk.cold2frozen')

# To enable debugging
#logger.setLevel(logging.DEBUG)

def main():

    # Define the App Path
    app_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    logger.debug('Starting main()')

    # Argument Parser
    parser = argparse.ArgumentParser(description='Restore Frozen Buckets')
    parser.add_argument('-i','--index', metavar='index', dest='index', type=str, help='Index Name', required=True)
    parser.add_argument('-s','--start', metavar='startdate', dest='startdate', type=str, help='start day: DDMMYYYY', required=True)
    parser.add_argument('-e','--end', metavar='enddate', dest='enddate', type=str, help='end day: DDMMYYYY', required=True)
    parser.add_argument('-t','--target', metavar='targetdir', dest='targetdir', type=str, help='target directory', required=True)
    parser.add_argument('-c','--config', metavar='configfile', dest='configfile', type=str, help='config file', default='cold2frozen.conf', required=False)


    args = parser.parse_args()

    # Check Arguments
    start_tstamp = int(datetime.strptime(args.startdate + " 00:00:00", "%d%m%Y %H:%M:%S").timestamp())
    end_tstamp = int(datetime.strptime(args.enddate + " 23:59:59", "%d%m%Y %H:%M:%S").timestamp() + 1)

    # Validate permissions on destination dir
    if not os.access(args.targetdir, os.W_OK):
        msg = 'Cannot write to directory %s' % args.targetdir
        logger.error(msg)
        raise Exception(msg)
    
    # Create logFields Object
    logFields = libc2f.logDict()

    # Read in config file
    config = libc2f.readConfig(app_path,args.configfile)
    # Get the storage handler
    storage = libc2f.connStorage(config)

    logFields.add('status', None)

    # Check if index exists
    if not libc2f.indexExists(storage, args.index):
        msg = 'Index %s does not exists in storage location' % args.index
        sys.exit(msg)

    # Initialize bucket container object
    buckets = libbuckets.BucketIndex(index=args.index)

    # Add all the buckets to the container
    bucket_list = libc2f.listBuckets(storage, args.index)
    for bucket_name in bucket_list:
        buckets.add(bucket_name)

    # Loop through the filtered objects
    for bucket_obj in buckets.filter(start_tstamp, end_tstamp):
        logFields.add('bucketname', bucket_obj.name)
        logFields.add('indexname', buckets.index)
        sourcedir = libc2f.bucketDir(storage, os.path.join(buckets.index,bucket_obj.name))
        logFields.add('sourcedir', sourcedir)
        targetdir = os.path.join(args.targetdir,bucket_obj.name)
        logFields.add('targetdir', targetdir)
        bucket_size_source = libc2f.getBucketSizeTarget(storage, os.path.join(buckets.index,bucket_obj.name))

        if os.path.isdir(targetdir):
            bucket_size = libc2f.getBucketSize(targetdir)
            if bucket_size == bucket_size_source:
                logFields.add('restoretime_ms', 0)
                logFields.add('bucketsize_b', bucket_size)
                logFields.add('status', 'existed')
                msg = "Found existing bucket %s" % sourcedir
                print(msg)
                logger.info(logFields.kvout())
                continue
            else:
                msg = "Found existing bucket with different size (will extract again) %s" % sourcedir
                print(msg)
        logFields.add('status', 'restored')
        msg = "Restoring bucket %s" % sourcedir
        print(msg)
        restorestart = time.time() * 1000
        libc2f.restoreBucket(storage, buckets.index, bucket_obj.name, args.targetdir)
        restoreend = time.time() * 1000
        logFields.add('restoretime_ms', round(restoreend - restorestart,3))
        bucket_size = libc2f.getBucketSize(targetdir) 
        logFields.add('bucketsize_b', bucket_size)
        if bucket_size != bucket_size_source:
            logFields.add('status', 'failed_size')
            logger.info(logFields.kvout())
            msg = 'Restored bucket sizes differ sourcebucket=%s (sourcesize=%s) targetbucket=%s (targetsize=%s)' % (sourcedir, bucket_size_source, targetdir, bucket_size)
            logger.error(msg)
            sys.exit(msg)
        logger.info(logFields.kvout())

if __name__ == "__main__":
    main()
    sys.exit()
