#!/usr/bin/env python3

# This script is not used right now. It is a preparation of a future restore process to restore buckets from storage
# like S3.

from lib import libc2f
from lib import libbuckets
import os, sys
import argparse
import logging, logging.handlers
import time
from datetime import datetime, timedelta

def main():

    libc2f.verify_splunk_home()

    # Create Handler
    c2f = libc2f.c2f()

    # Define the App Path
    app_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    #TODO: Remove for prod
    c2f.logger.setLevel(logging.DEBUG)

    c2f.logger.debug('Starting main()')

    # Argument Parser
    parser = argparse.ArgumentParser(description='Restore Frozen Buckets')
    parser.add_argument('-i','--index', metavar='index', dest='index', type=str, help='Index Name', required=True)
    parser.add_argument('-s','--start', metavar='startdate', dest='startdate', type=str, help='start day: DDMMYYYY', required=True)
    parser.add_argument('-e','--end', metavar='enddate', dest='enddate', type=str, help='end day: DDMMYYYY', required=True)
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")

    args = parser.parse_args()

    # Check Arguments
    start_tstamp = int(datetime.strptime(args.startdate + " 00:00:00", "%d%m%Y %H:%M:%S").timestamp())
    end_tstamp = int(datetime.strptime(args.enddate + " 23:59:59", "%d%m%Y %H:%M:%S").timestamp() + 1)

    # Create logFields Object
    logFields = libc2f.logDict()

    # Read in config file
    config = c2f.read_config(app_path)
    ARCHIVE_DIR = config.get("cold2frozen", "ARCHIVE_DIR")
    RESTORE_DIR = config.get("cold2frozen", "RESTORE_DIR")
    INDEX_DIR = os.path.join(ARCHIVE_DIR, args.index)

    # Check directory exists
    if not os.path.isdir(ARCHIVE_DIR):
        msg = 'Directory %s for frozen buckets does not exist' % INDEX_DIR
        print(msg)
        sys.exit(1)

    # Check permissions of the directory
    if not os.access(INDEX_DIR, os.R_OK):
        msg = 'Cannot read directory %s' % INDEX_DIR
        c2f.logger.error(msg)
        sys.exit(msg)

    buckets = libbuckets.BucketIndex(index=args.index)
    for object in os.scandir(INDEX_DIR):
        bucket_name = object.name
        print("Found bucket: ", bucket_name)
        buckets.add(bucket_name)

    print(buckets)
    filtered_buckets = buckets.filter(start_tstamp, end_tstamp)
    for bucket_obj in buckets.filter(start_tstamp, end_tstamp):
        print(bucket_obj)
    
    print("got here")
    print(buckets.filter(start_tstamp, end_tstamp))

if __name__ == "__main__":
    main()
    sys.exit()
