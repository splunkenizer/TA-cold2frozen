#!/usr/bin/env python3

from lib import libc2f
from lib import libbuckets
import os, sys
import argparse
import subprocess
import re
import logging
import time
from multiprocessing import Process, Queue

# Verify SPLUNK_HOME
libc2f.verifySplunkHome()
SPLUNK_HOME = os.environ['SPLUNK_HOME']

# Create Logger
from lib import liblogger
logger = liblogger.setup_logging('splunk.cold2frozen')

# To enable debugging
#logger.setLevel(logging.DEBUG)

# Create queue
queue = Queue()

def parse_output(output):
    output = output.split("\n")
    for line in output:
        for level in [r'INFO',r'WARN',r'ERROR']:
            p = re.compile(level)
            if p.findall(line):
                msg = line
                if level == 'INFO' or level == 'WARN':
                    return msg
                if level == 'ERROR':
                    skip = re.compile(r'IndexConfig - Asked to check if idx= is an index')
                    if not skip.findall(line):
                        return msg

def worker(workerid: int, thaweddir: str):
    logger.debug('Starting worker(), workerid: %s', workerid)
    global queue
    while not queue.empty():
        bucketname = queue.get()
        rebuild_bucket(workerid, bucketname, thaweddir)


def rebuild_bucket(workerid: int, bucketname: str, thaweddir: str):
    logFields = libc2f.logDict()
    logFields.add('status', 'rebuilt')
    logger.debug('Starting rebuild_bucket(), workerid: %s, bucketname: %s' % (workerid, bucketname))
    logFields.add('bucketname', bucketname)
    logger.debug("bucketname is %s" % bucketname)
    destdir = os.path.join(thaweddir, bucketname)
    logFields.add('destdir', destdir)
    logger.debug("destdir is %s" % destdir)
    msg = '%s: START - Rebuilding bucket %s' % (workerid, bucketname)
    print(msg, flush=True)
    # Run the rebuild command
    rebuildstart = time.time() * 1000
    process = subprocess.run(['splunk', 'rebuild', os.path.join(thaweddir,bucketname)],capture_output=True)
    rebuildend = time.time() * 1000
    logFields.add('rebuildtime_ms', round(rebuildend - rebuildstart,3))

    if process.returncode == 0:
        status = 'SUCCESS'
        logFields.add('status', 'rebuilt')
    else:
        status = 'ERROR'
        logFields.add('status', 'failed_rebuilt')
    # Parse the output
    # Unfortunately, the command outputs all to stderr
    output = process.stderr.decode('utf-8')
    outmsg = parse_output(output)
    msg = '%s: %s - Rebuilding bucket %s\n%s' % (workerid, status, bucketname, outmsg)
    print(msg, flush=True)
    if len(outmsg) > 0:
        logFields.add('output', "'" + outmsg + "'")
    logger.info(logFields.kvout())


def main():
    logger.debug('Starting main()')

    # Argument Parser
    parser = argparse.ArgumentParser(description='Rebuild Frozen Buckets')
    parser.add_argument('-t','--thaweddb', metavar='thaweddb', dest='thaweddb', type=str, help='Thaweddb Directory', required=True)
    parser.add_argument('-p','--numprocs', metavar='numprocs', dest='numprocs', type=int, help='Number of processes', required=False, default=1)

    args = parser.parse_args()

    # Define static vars
    THAWED_DIR = args.thaweddb

    # Check directory exists
    if not os.path.isdir(THAWED_DIR):
        msg = 'Directory %s with thawed buckets does not exist' % THAWED_DIR
        print(msg)
        sys.exit(1)

    # Check permissions of the directory
    if not os.access(THAWED_DIR, os.W_OK):
        msg = 'Cannot write to directory %s' % THAWED_DIR
        logger.error(msg)
        sys.exit(msg)

    buckets = libbuckets.BucketIndex(index='restored')
    for object in os.scandir(THAWED_DIR):
        if not os.path.isdir(object):
            continue
        bucket_name = object.name
        buckets.add(bucket_name)
   
    # Put all the bucketnames to the queue
    for bucket_obj in buckets:
        queue.put(bucket_obj.name)

    jobs = []
    for workerid in range(args.numprocs):
        process = Process(target=worker, args=(workerid,THAWED_DIR))
        jobs.append(process)

    for job in jobs:
        job.start()

    for job in jobs:
        job.join()

if __name__ == "__main__":
    main()
    sys.exit()
