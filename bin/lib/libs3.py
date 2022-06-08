import sys, os
import time
from datetime import timezone
import boto3
import botocore
import logging
logger = logging.getLogger('splunk.cold2frozen')

class c2fS3:

    def __init__(self, access_key, secret_key, s3_bucket, archive_dir):
        self._type = 's3'
        # To connect to on-premise S3 check this out:
        # https://stackoverflow.com/questions/60709034/connect-to-s3-compatible-storage-with-boto3
        self._access_key = access_key
        self._secret_key = secret_key
        self._s3_bucket_name = s3_bucket
        self._s3_resource = self._resource_s3(self._access_key, self._secret_key)
        self._s3_client = self._client_s3(self._access_key, self._secret_key)
        self._is_valid_s3bucket(self._s3_bucket_name)
        self._is_writable_s3bucket(self._s3_bucket_name)
        self._archive_dir = self._is_valid_archive_dir(self._s3_bucket_name, archive_dir)

    @property
    def type(self):
        return self._type

    @property
    def s3_bucket(self):
        return self._s3_bucket_name

    @property
    def archive_dir(self):
        return self._archive_dir

    def _resource_s3(self, access_key, secret_key):
        s3_resource = boto3.resource('s3', aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
        return s3_resource

    def _client_s3(self, access_key, secret_key):
        s3_client = boto3.client('s3', aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
        return s3_client

    def _is_valid_s3bucket(self, s3_bucket_name):
        try:
            self._s3_resource.meta.client.head_bucket(Bucket=s3_bucket_name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = e.response['Error']['Code']
            if error_code == '404':
                msg = 'Bucket %s does not exist' % s3_bucket_name
                logger.error(msg)
                raise Exception(msg)

    def _is_writable_s3bucket(self, bucket):
        bucket_acl = self._s3_client.get_bucket_acl(Bucket=bucket)
        permissions = (bucket_acl['Grants'][0]['Permission'])
        if not (permissions == 'FULL_CONTROL' or permissions == 'WRITE'):
            msg = 'Cannot write to bucket %s' % bucket
            logger.error(msg)
            raise Exception(msg)

    def _is_dir(self, bucket, s3_path):
        s3_path = os.path.join(s3_path.strip('/'), '')
        try:
            logger.debug("Checking path s3://%s/%s" % (bucket, s3_path))
            self._s3_client.head_object(Bucket=bucket, Key=os.path.join(s3_path, ''))
        except:
            return False
        return True

    def _is_valid_archive_dir(self, bucket, s3_path):
        try:
            self._is_dir(bucket, s3_path)
        except:
            msg = 'Directory %s/%s for frozen buckets does not exist' % (bucket, s3_path)
            logger.error(msg)
            raise Exception(msg)
        return s3_path

    def _full_path(self, path):
        full_path = os.path.join(self._archive_dir, path).strip('/')
        return full_path

    def create_index_dir(self, indexname):
        indexdir = os.path.join(self._archive_dir, indexname)
        if not self._is_dir(self._s3_bucket_name, indexdir):
            logger.debug("Creating index directory %s" % indexdir)
            self._s3_client.put_object(Bucket=self._s3_bucket_name, Key=(indexdir+'/'))

    def check_lock_file(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        try:
            self._s3_client.head_object(Bucket=self._s3_bucket_name, Key=full_lock_file)
        except:
            return False
        return True

    def lock_file_age(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        logger.debug("Checking age for lockfile %s" % full_lock_file)
        obj = self._s3_resource.Object(self._s3_bucket_name, full_lock_file).get()
        print(obj)
        lock_age_datetime = (obj["LastModified"])
        from datetime import timezone
        lock_age = lock_age_datetime.replace(tzinfo=timezone.utc).timestamp()
        return lock_age

    def write_lock_file(self, lock_file, hostname):
        full_lock_file = self._full_path(lock_file)
        try:
            self._s3_resource.Object(self._s3_bucket_name, full_lock_file).put(Body=hostname)
        except:
            return False
        logger.debug("Created lockfile %s" % full_lock_file)
        return True

    def read_lock_file(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        logger.debug("Reading lockfile %s" % full_lock_file)
        obj = self._s3_resource.Object(self._s3_bucket_name, full_lock_file).get()
        hostname = obj["Body"].read().decode("utf-8")
        return hostname

    def remove_lock_file(self, lock_file):
        full_lock_file = self._full_path(lock_file)
        if self.check_lock_file(lock_file):
            self._s3_client.delete_object(Bucket=self._s3_bucket_name, Key=full_lock_file)
            logger.debug("Removed lockfile %s" % full_lock_file)

    def bucket_dir(self, bucket_dir):
        full_bucket_dir = "s3://%s/%s" % (self._s3_bucket_name, self._full_path(bucket_dir))
        return full_bucket_dir

    def bucket_exists(self, bucket_dir):
        full_bucket_dir = self._full_path(bucket_dir)
        if self._is_dir(self._s3_bucket_name, full_bucket_dir):
            return True
        else:
            return False

    def bucket_size(self, bucketPath):
        size = 0
        full_bucket_dir = self._full_path(bucketPath)
        for obj in self._s3_resource.Bucket(self._s3_bucket_name).objects.filter(Prefix=full_bucket_dir):
            logger.debug("Getting size for file %s" % obj)
            size += obj.size
        return size

    def bucket_copy(self, bucket, destdir):
        full_bucket_dir = self._full_path(destdir)
        try:
            for root,dirs,files in os.walk(bucket):
                for file in files:
                    source_file = os.path.join(root,file)
                    relative_path = os.path.relpath(source_file, bucket)
                    dest_file = os.path.join(full_bucket_dir, relative_path)
                    logger.debug("Uploading file %s to %s" % (source_file,dest_file))
                    self._s3_client.upload_file(source_file,self._s3_bucket_name,dest_file)
        except Exception:
            msg = 'Failed to copy bucket %s to destination %s' % (bucket, full_bucket_dir)
            logger.error(msg)
            sys.exit(msg)
