#!/usr/bin/env python3

from __future__ import print_function
from lib import libc2f
from lib import libbuckets
import os, sys
import argparse
import logging, logging.handlers
import datetime
from math import floor, log

# Verify SPLUNK_HOME
libc2f.verifySplunkHome()
SPLUNK_HOME = os.environ['SPLUNK_HOME']

# Create Logger
from lib import liblogger
logger = liblogger.setup_logging('splunk.cold2frozen')

#TODO: Remove for prod
#logger.setLevel(logging.DEBUG)

def format_bytes(size):
  log_num = 0 if size <= 0 else floor(log(size, 1024))
  return f"{round(size / 1024 ** log_num, 2)} {['B', 'KB', 'MB', 'GB', 'TB'][int(log_num)]}"

def main():

    # Define the App Path
    app_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    logger.debug('Starting main()')

    # Argument Parser
    parser = argparse.ArgumentParser(description='Logs index statistics')
    parser.add_argument('-i','--index', metavar='index', dest='index', type=str, help='Index(es)', action='append', nargs='*', required=False)
    parser.add_argument('-v','--verbose', action="store_true", help='Output on CLI also')

    args = parser.parse_args()

    # Read in config file
    config = libc2f.readConfig(app_path)
    # Get the storage handler
    storage = libc2f.connStorage(config)

    index_list = libc2f.listIndexes(storage)

    # Verify index arguments
    if args.index:
        for index in args.index[0]:
            if index not in index_list:
                print("ERROR: Index '%s' does not exist on storage" % index)
                sys.exit(1)

    for index in index_list:
        # Create logFields Object
        logFields = libc2f.logDict()
        logFields.add('status', None)
        if args.index and index not in args.index[0]:
            continue
        logger.debug("Scanning Index %s" % index)
        # Initialize bucket container object
        buckets = libbuckets.BucketIndex(index=index)
        # Add all the buckets to the container
        bucket_list = libc2f.listBuckets(storage, index)
        for bucket_name in bucket_list:
            buckets.add(bucket_name)

        logFields.add('indexname', index)
        logger.debug("indexname is %s" % index)
        destdir = libc2f.bucketDir(storage, index)
        logFields.add('destdir', destdir)
        logger.debug("destdir is %s" % destdir)
        # Loop through the buckets
        index_size = 0
        bucket_count = 0
        earliest = 9999999999999
        latest = 0
        for bucket_obj in buckets:
            bucket_size_source = libc2f.getBucketSizeTarget(storage, os.path.join(index,bucket_obj.name))
            index_size += bucket_size_source
            bucket_count += 1
            if bucket_obj.end > latest:
                latest = bucket_obj.end
            if bucket_obj.start < earliest:
                earliest = bucket_obj.start

        logFields.add('indexsize_b', index_size)
        logger.debug("indexsize_b is %s" % index_size)
        logFields.add('bucketcount', bucket_count)
        logger.debug("bucketcount is %s" % bucket_count)
        if bucket_count > 0:
            logFields.add('earliest', earliest)
            logger.debug("earliest is %s" % earliest)
            logFields.add('latest', latest)
            logger.debug("latest is %s" % latest)
        else:
            earliest = 0

        logFields.add('status', 'indexstats')
        logger.debug("status is %s" % 'indexstats')
        logger.info(logFields.kvout())
        if args.verbose:
            earliest_date = datetime.datetime.fromtimestamp(earliest).strftime("%d.%m.%Y %H:%M:%S")
            latest_date = datetime.datetime.fromtimestamp(latest).strftime("%d.%m.%Y %H:%M:%S")
            print('Index: %s, Buckets: %s, Size: %s, Earliest: %s, Latest: %s, Destdir: %s' % (index, bucket_count, format_bytes(index_size), earliest_date, latest_date, destdir))


if __name__ == "__main__":
    main()
    sys.exit()

