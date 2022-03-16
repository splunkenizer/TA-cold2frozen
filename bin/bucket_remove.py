#!/usr/bin/env python3

from lib import libc2f
import os, sys
import argparse
import datetime
import shutil
import logging

def main():

    libc2f.verify_splunk_home()

    # Create Handler
    c2f = libc2f.c2f()

    # Define the App Path
    app_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    #TODO: Remove for prod
    #c2f.logger.setLevel(logging.DEBUG)

    c2f.logger.debug('Starting main()')

    # Argument Parser
    parser = argparse.ArgumentParser(description='Remove Frozen Buckets')
    parser.add_argument('-d','--days', metavar='days', dest='days', type=str, help='Older than days', required=True)
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")

    args = parser.parse_args()

    # Check Arguments
    if not args.days.isdigit():
        msg = "ERROR: Argument days=%s must be a number!" % args.days
        print(msg)
        sys.exit(1)

    # Create logFields Object
    logFields = libc2f.logDict()

    # Define check date
    check_tstamp = datetime.datetime.today() - datetime.timedelta(days=int(args.days))
    check_date = datetime.datetime.strftime(check_tstamp, "%d.%m.%Y %H:%M:%S")

    # Read in config file
    config = c2f.read_config(app_path)
    ARCHIVE_DIR = config.get("cold2frozen", "ARCHIVE_DIR")

    # Check directory exists
    if not os.path.isdir(ARCHIVE_DIR):
        msg = 'Directory %s for frozen buckets does not exist' % ARCHIVE_DIR
        print(msg)
        sys.exit(1)

    # Check permissions of the directory
    if not os.access(ARCHIVE_DIR, os.W_OK):
        msg = 'Cannot write to directory %s' % ARCHIVE_DIR
        c2f.logger.error(msg)
        sys.exit(msg)

    # loop through the buckets and remove the once olther than the given days
    for index in os.scandir(ARCHIVE_DIR):
        index_dir = os.path.join(ARCHIVE_DIR, index.name)
        if not os.path.isdir(index_dir) or index.name.startswith('.'):
            continue
        c2f.logger.debug('Scanning Index Directory %s for frozen buckets to delete' % index_dir)
        for object in os.scandir(index_dir):
            bucket_name = object.name
            if not bucket_name.startswith('db_') and not bucket_name.startswith('rb_'):
                continue
            bucket_dir = os.path.join(index_dir, object.name)
            bucket_stats = os.stat(bucket_dir)
            c2f.logger.debug("bucketname=%s, destdir=%s %s" % (bucket_name, bucket_dir, bucket_stats))
            
            if datetime.datetime.fromtimestamp(bucket_stats.st_ctime) < check_tstamp:
                destdir = os.path.join(index_dir,bucket_name)
                logFields.add('status', 'removed')
                c2f.logger.debug("status is %s" % 'removed')
                logFields.add('destdir', destdir)
                c2f.logger.debug("destdir is %s" % destdir)
                logFields.add('indexname', index.name)
                c2f.logger.debug("indexname is %s" % index.name)
                logFields.add('bucket_create_date', int(bucket_stats.st_ctime))
                c2f.logger.debug("bucket_create_date is %s" % int(bucket_stats.st_ctime))
                logFields.add('check_date', int(datetime.datetime.timestamp(check_tstamp)))
                c2f.logger.debug("check_date is %s" % int(datetime.datetime.timestamp(check_tstamp)))
                #bucket_date = datetime.datetime.strftime(datetime.datetime.fromtimestamp(bucket_stats.st_ctime), "%d.%m.%Y %H:%M:%S")
                c2f.logger.info(logFields.kvout())
                try:
                    shutil.rmtree(destdir)
                except OSError as ex:
                    msg = 'Cannot remove bucket=%s' % destdir
                    c2f.logger.error(msg)

if __name__ == "__main__":
    main()
    sys.exit()