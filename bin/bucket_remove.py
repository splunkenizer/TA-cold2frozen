#!/usr/bin/env python3

from lib import libc2f
from lib import libbuckets
import os, sys
import argparse
import datetime
import time
import logging

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
    parser = argparse.ArgumentParser(description='Remove Frozen Buckets')
    parser.add_argument('-i','--index', metavar='index', dest='index', type=str, help='Index(es)', action='append', nargs='*', required=False)
    parser.add_argument('-d','--days', metavar='days', dest='days', type=str, help='older than days', required=True)
    parser.add_argument('-t','--usectime', action="store_true", help='Filesystem creation date')
    parser.add_argument('-r','--dryrun', action="store_true", help='Do not delete the buckets')
    parser.add_argument("-v", "--verbose", action="store_true", help='increase output verbosity')

    args = parser.parse_args()

    # Check Arguments
    if not args.days.isdigit():
        msg = "ERROR: Argument days=%s must be a number!" % args.days
        print(msg)
        sys.exit(1)

    # Define check date
    check_tstamp = datetime.datetime.today() - datetime.timedelta(days=int(args.days))
    check_date = datetime.datetime.strftime(check_tstamp, "%d.%m.%Y %H:%M:%S")

    # Create logFields Object
    logFields = libc2f.logDict()

    # Read in config file
    config = libc2f.readConfig(app_path)
    # Get the storage handler
    storage = libc2f.connStorage(config)

    if args.usectime and storage.type != 'dir':
        print("ERROR: Bucket remove based on ctime for storage type %s is not supported" % storage.type)
        sys.exit(1)

    logFields.add('status', None)

    index_list = libc2f.listIndexes(storage)

    # Verify index arguments
    if args.index:
        for index in args.index[0]:
            if index not in index_list:
                print("ERROR: Index '%s' does not exist on storage" % index)
                sys.exit(1)

    if not args.usectime:
        for index in index_list:
            if args.index and index not in args.index[0]:
                continue
            index_dir = os.path.join(storage.archive_dir, index)
            logger.debug("Scanning Index %s" % index)
            # Initialize bucket container object
            buckets = libbuckets.BucketIndex(index=index)
            # Add all the buckets to the container
            bucket_list = libc2f.listBuckets(storage, index)
            for bucket_name in bucket_list:
                buckets.add(bucket_name)

            # Loop through the filtered objects
            for bucket_obj in buckets.older(int(args.days)):
                logFields.add('bucketname', bucket_obj.name)
                logger.debug("bucketname is %s" % bucket_obj.name)
                logFields.add('indexname', buckets.index)
                logger.debug("indexname is %s" % buckets.index)
                bucket_epoch_end = bucket_obj.end
                logFields.add('bucketend', bucket_epoch_end)
                logger.debug("bucket_epoch_end is %s" % bucket_epoch_end)
                bucket_epoch_start = bucket_obj.start
                logFields.add('bucketstart', bucket_epoch_start)
                logger.debug("bucket_epoch_start is %s" % bucket_epoch_start)
                bucket_name_prefix = bucket_obj.prefix
                logFields.add('bucketprefix', bucket_name_prefix)
                logger.debug("bucket_name_prefix is %s" % bucket_name_prefix)
                logFields.add('peerguid', bucket_obj.peer)
                logger.debug("peer_guid is %s" % bucket_obj.peer)
                bucket_id = bucket_obj.id
                logFields.add('bucketid', bucket_id)
                normalized_bucket_name_array = bucket_name.split("_")[1:]
                normalized_bucket_name = "_".join(normalized_bucket_name_array)
                logFields.add('buckename_norm', normalized_bucket_name)
                logger.debug("normalized_bucket_name is %s" % normalized_bucket_name)

                destdir = libc2f.bucketDir(storage, os.path.join(buckets.index,bucket_obj.name))
                logFields.add('destdir', destdir)
                bucket_size_source = libc2f.getBucketSizeTarget(storage, os.path.join(buckets.index,bucket_obj.name))
                logFields.add('bucketsize_b', bucket_size_source)

                if not args.dryrun:
                    rmstart = time.time() * 1000
                    libc2f.removeBucket(storage, index, bucket_obj.name)
                    rmend = time.time() * 1000
                    logFields.add('rmtime_ms', round(rmend - rmstart,3))
                    logFields.add('status', 'removed')
                    logger.debug("status is %s" % 'removed')
                    logger.info(logFields.kvout())
                else:
                    bucket_enddate = datetime.datetime.strftime(datetime.datetime.fromtimestamp(bucket_obj.end), "%d.%m.%Y %H:%M:%S")
                    print("(Dryrun) Remove bucket (bucket_end: %s, size_kb: %s) %s" % (bucket_enddate,bucket_size_source,destdir))


    else:
        for index in index_list:
            if args.index and index not in args.index[0]:
                continue
            index_dir = os.path.join(storage.archive_dir, index)
            for object in os.scandir(index_dir):
                bucket_name = object.name
                if not bucket_name.startswith('db_') and not bucket_name.startswith('rb_'):
                    continue
                bucket_dir = os.path.join(index_dir, object.name)
                bucket_stats = os.stat(bucket_dir)
                logger.debug("bucketname=%s, destdir=%s %s" % (bucket_name, bucket_dir, bucket_stats))
                
                if datetime.datetime.fromtimestamp(bucket_stats.st_ctime) < check_tstamp:
                    destdir = os.path.join(index_dir,bucket_name)
                    logFields.add('destdir', destdir)
                    logger.debug("destdir is %s" % destdir)
                    logFields.add('indexname', index)
                    logger.debug("indexname is %s" % index)
                    logFields.add('bucket_create_date', int(bucket_stats.st_ctime))
                    logger.debug("bucket_create_date is %s" % int(bucket_stats.st_ctime))
                    logFields.add('check_date', int(datetime.datetime.timestamp(check_tstamp)))
                    logger.debug("check_date is %s" % int(datetime.datetime.timestamp(check_tstamp)))
                    if not args.dryrun:
                        libc2f.removeBucket(storage, index, bucket_name)
                        logFields.add('status', 'removed')
                        logger.debug("status is %s" % 'removed')
                        logger.info(logFields.kvout())
                    else:
                        bucket_date = datetime.datetime.strftime(datetime.datetime.fromtimestamp(bucket_stats.st_ctime), "%d.%m.%Y %H:%M:%S")
                        print("(Dryrun) Remove bucket (ctime: %s) %s" % (bucket_date,bucket_dir))

if __name__ == "__main__":
    main()
    sys.exit()