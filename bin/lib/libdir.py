import sys, os, shutil
import logging
logger = logging.getLogger('splunk.cold2frozen')

class c2fDir:

    def __init__(self, archive_dir):
        self._type = 'dir'
        self._archive_dir = self._is_valid_dir(archive_dir)
        self._is_writable_dir(archive_dir)

    @property
    def type(self):
        return self._type

    @property
    def archive_dir(self):
        return self._archive_dir

    def _is_valid_dir(self, archive_dir):
        # Check directory exists
        if not os.path.isdir(archive_dir):
            msg = 'Directory %s for frozen buckets does not exist' % archive_dir
            logger.error(msg)
            raise Exception(msg)
        return archive_dir

    def _is_writable_dir(self, archive_dir):
    # Check permissions of the directory
        if not os.access(archive_dir, os.W_OK):
            msg = 'Cannot write to directory %s' % archive_dir
            logger.error(msg)
            raise Exception(msg)

    def _full_path(self, path: str) -> None:
        full_path = os.path.join(self._archive_dir, path)
        return full_path

    def create_index_dir(self, indexname):
        indexdir = self._full_path(indexname)
        if not os.path.isdir(indexdir):
            logger.debug("Creating index directory %s" % indexname)
            os.mkdir(indexdir)

    def check_lock_file(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        logger.debug("Checking for lockfile %s" % full_lock_file)
        if os.path.isfile(full_lock_file):
            logger.debug("Found existing lockfile %s" % full_lock_file)
            return True
        else:
            logger.debug("No lockfile %s" % full_lock_file)
            return False

    def lock_file_age(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        lock_age = os.path.getmtime(full_lock_file)
        return lock_age

    def write_lock_file(self, lock_file, hostname):
        full_lock_file = self._full_path(lock_file)
        with open(full_lock_file, 'w') as file:
            file.write(hostname)
            logger.debug("Created lockfile %s" % full_lock_file)
            return True

    def read_lock_file(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        with open(full_lock_file, "r") as f:
            lock_host = f.read().rstrip()
        return lock_host

    def remove_lock_file(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        if os.path.isfile(full_lock_file):
            os.remove(full_lock_file)
            logger.debug("Removed lockfile %s" % full_lock_file)

    def bucket_dir(self, bucket_dir):
        full_bucket_dir = self._full_path(bucket_dir)
        return full_bucket_dir

    def bucket_exists(self, bucket_dir):
        full_bucket_dir = self._full_path(bucket_dir)
        if os.path.isdir(full_bucket_dir):
            return True
        else:
            return False

    def bucket_size(self, bucketPath):
        size = 0
        full_bucket_dir = self._full_path(bucketPath)
        for path, dirs, files in os.walk(full_bucket_dir):
            for file in files:
                filepath = os.path.join(path, file)
                logger.debug("Getting size for file %s" % filepath)
                size += os.path.getsize(filepath)
        return size

    def bucket_copy(self, bucket, destdir):
        full_bucket_dir = self._full_path(destdir)
        try:
            shutil.copytree(bucket, full_bucket_dir)
        except OSError:
            msg = 'Failed to copy bucket %s to destination %s' % (bucket, full_bucket_dir)
            logger.error(msg)
            sys.exit(msg)