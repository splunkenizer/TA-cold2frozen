#!/usr/bin/env python3

from lib import libc2f
from lib import libbuckets
import os, sys
import argparse
import subprocess
import re
import logging

def parse_output(c2f,output):
    output = output.split("\n")
    for line in output:
        for level in [r'INFO',r'WARN',r'ERROR']:
            p = re.compile(level)
            if p.findall(line):
                msg = line
                if level == 'INFO':
                    c2f.logger.info(msg)
                if level == 'WARN':
                    c2f.logger.warning(msg)
                    print(msg)
                if level == 'ERROR':
                    c2f.logger.error(msg)
                    print(msg, file=sys.stderr)

def main():

    libc2f.verify_splunk_home()

    # Create Handler
    c2f = libc2f.c2f()

    # Define the App Path
    app_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    #TODO: Remove for prod
    #c2f.logger.setLevel(logging.DEBUG)

    # Argument Parser
    parser = argparse.ArgumentParser(description='Restore Frozen Buckets')
    parser.add_argument('-i','--index', metavar='index', dest='index', type=str, help='Target Index Name', required=True)
    parser.add_argument('-t','--thaweddb', metavar='thaweddb', dest='thaweddb', type=str, help='Thaweddb Directory', required=True)
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")

    args = parser.parse_args()

    # Define static vars
    THAWED_DIR = args.thaweddb
    INDEX_NAME = args.index

    # Check directory exists
    if not os.path.isdir(THAWED_DIR):
        msg = 'Directory %s with thawed buckets does not exist' % THAWED_DIR
        print(msg)
        sys.exit(1)

    # Check permissions of the directory
    if not os.access(THAWED_DIR, os.W_OK):
        msg = 'Cannot write to directory %s' % THAWED_DIR
        c2f.logger.error(msg)
        sys.exit(msg)

    buckets = libbuckets.BucketIndex(index=INDEX_NAME)
    for object in os.scandir(THAWED_DIR):
        if not os.path.isdir(object):
            continue
        bucket_name = object.name
        buckets.add(bucket_name)

    for bucket_obj in buckets:
        msg = 'Rebuilding bucket %s ... ' % bucket_obj.name
        print(msg, end = '', flush=True)

        # Run the rebuild command
        process = subprocess.run(['splunk', 'rebuild', os.path.join(THAWED_DIR,bucket_obj.name), INDEX_NAME],capture_output=True)

        # Check the exit status
        if process.returncode != 0:
            output = process.stderr.decode('utf-8')
            print('ERROR')
            parse_output(c2f,output)
            sys.exit(process.returncode)
        else:
            print('done')

        # Parse the output
        # Unfortunately, the command outputs all to stderr
        output = process.stderr.decode('utf-8')
        parse_output(c2f,output)

if __name__ == "__main__":
    main()
    sys.exit()
