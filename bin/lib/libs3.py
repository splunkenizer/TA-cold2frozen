import sys, os
import time
from datetime import timezone
import boto3
import botocore
import logging
logger = logging.getLogger('splunk.cold2frozen')

class c2fS3:

    def __init__(self, access_key: str, secret_key: str, s3_bucket: str, archive_dir: str, **kwargs):
        self._type = 's3'
        # To connect to on-premise S3 check this out:
        # https://stackoverflow.com/questions/60709034/connect-to-s3-compatible-storage-with-boto3
        self._access_key = access_key
        self._secret_key = secret_key
        self._s3_endpoint = kwargs.get('s3_endpoint', None)
        self._s3_bucket_name = s3_bucket
        self._s3_resource = self._resource_s3(access_key=self._access_key, secret_key=self._secret_key, s3_endpoint=self._s3_endpoint)
        self._s3_bucket = self._s3_resource.Bucket(self._s3_bucket_name)
        self._s3_client = self._client_s3(access_key=self._access_key, secret_key=self._secret_key, s3_endpoint=self._s3_endpoint)
        self._is_valid_s3bucket(self._s3_bucket_name)
        self._is_writable_s3bucket(self._s3_bucket_name)
        if self._is_valid_archive_dir(archive_dir):
            self._archive_dir = os.path.join(archive_dir.strip('/'), '')

    @property
    def type(self):
        return self._type

    @property
    def s3_bucket(self):
        return self._s3_bucket_name

    @property
    def archive_dir(self):
        return self._archive_dir

    def _resource_s3(self, access_key, secret_key: str, s3_endpoint: str):
        s3_resource = boto3.resource('s3', aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key, endpoint_url=s3_endpoint)
        return s3_resource

    def _client_s3(self, access_key, secret_key: str, s3_endpoint: str):
        s3_client = boto3.client('s3', aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key, endpoint_url=s3_endpoint)
        return s3_client

    def _is_valid_s3bucket(self, s3_bucket_name: str) -> None:
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

    def _is_writable_s3bucket(self, bucket: str) -> None:
        bucket_acl = self._s3_client.get_bucket_acl(Bucket=bucket)
        permissions = (bucket_acl['Grants'][0]['Permission'])
        if not (permissions == 'FULL_CONTROL' or permissions == 'WRITE'):
            msg = 'Cannot write to bucket %s' % bucket
            logger.error(msg)
            raise Exception(msg)

    def _is_dir(self, s3_path: str) -> bool:
        """Returns T/F whether the directory exists."""
        s3_path = os.path.join(s3_path.strip('/'), '')
        logger.debug("Checking path s3://%s/%s" % (self._s3_bucket_name, s3_path))
        objects = list(self._s3_bucket.objects.filter(Prefix=s3_path, MaxKeys=1, limit=1))
        logger.debug("Checking path s3://%s/%s - done" % (self._s3_bucket_name, s3_path))
        return len(objects) >= 1

    def _is_valid_archive_dir(self, s3_path: str) -> bool:
        if not self._is_dir(s3_path):
            s3_path = os.path.join(s3_path.strip('/'), '')
            msg = 'Directory s3://%s/%s for frozen buckets does not exist' % (self._s3_bucket_name, s3_path)
            logger.error(msg)
            raise Exception(msg)
        return True

    def _full_path(self, path: str) -> None:
        full_path = os.path.join(self._archive_dir, path).strip('/')
        return full_path

    def index_exists(self, indexname: str) -> bool:
        indexdir = self._full_path(indexname)
        logger.debug("Checking for index directory %s" % indexdir)
        if self._is_dir(indexdir):
            return True
        else:
            return False

    #TODO: replace code to include index_exists function
    def create_index_dir(self, indexname: str) -> None:
        indexdir = self._full_path(indexname)
        if not self.index_exists(indexname):
            logger.debug("Creating index directory %s" % indexdir)
            self._s3_client.put_object(Bucket=self._s3_bucket_name, Key=(indexdir+'/'))

    def check_lock_file(self, lock_file: str) -> bool:
        full_lock_file = self._full_path(lock_file)
        try:
            self._s3_client.head_object(Bucket=self._s3_bucket_name, Key=full_lock_file)
        except:
            return False
        return True

    def lock_file_age(self, lock_file: str) -> str:
        full_lock_file = self._full_path(lock_file)
        logger.debug("Checking age for lockfile %s" % full_lock_file)
        obj = self._s3_resource.Object(self._s3_bucket_name, full_lock_file).get()
        lock_age_datetime = (obj["LastModified"])
        from datetime import timezone
        lock_age = lock_age_datetime.replace(tzinfo=timezone.utc).timestamp()
        return lock_age

    def write_lock_file(self, lock_file: str, hostname: str) -> bool:
        full_lock_file = self._full_path(lock_file)
        try:
            self._s3_resource.Object(self._s3_bucket_name, full_lock_file).put(Body=hostname)
        except:
            return False
        logger.debug("Created lockfile %s" % full_lock_file)
        return True

    def read_lock_file(self, lock_file: str) -> str:
        full_lock_file = self._full_path(lock_file)
        logger.debug("Reading lockfile %s" % full_lock_file)
        obj = self._s3_resource.Object(self._s3_bucket_name, full_lock_file).get()
        hostname = obj["Body"].read().decode("utf-8")
        return hostname

    def remove_lock_file(self, lock_file: str) -> None:
        full_lock_file = self._full_path(lock_file)
        if self.check_lock_file(lock_file):
            self._s3_client.delete_object(Bucket=self._s3_bucket_name, Key=full_lock_file)
            logger.debug("Removed lockfile %s" % full_lock_file)

    def bucket_dir(self, bucket_dir: str) -> str:
        full_bucket_dir = "s3://%s/%s" % (self._s3_bucket_name, self._full_path(bucket_dir))
        return full_bucket_dir

    def bucket_exists(self, bucket_dir: str) -> bool:
        full_bucket_dir = self._full_path(bucket_dir)
        if self._is_dir(full_bucket_dir):
            return True
        else:
            return False

    def bucket_size(self, bucketPath: str) -> int:
        size = 0
        full_bucket_dir = self._full_path(bucketPath)
        for obj in self._s3_resource.Bucket(self._s3_bucket_name).objects.filter(Prefix=full_bucket_dir):
            logger.debug("Getting size for file %s" % obj)
            size += obj.size
        return size

    def bucket_copy(self, bucket: str, destdir: str) -> None:
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

    def list_buckets(self, index: str):
        full_bucket_dir = self._full_path(index) + str('/')
        logger.debug("Listing buckets for path s3://%s/%s" % (self._s3_bucket_name, full_bucket_dir))
        buckets = list(self._s3_bucket.objects.filter(Prefix=full_bucket_dir))
        logger.debug("Listing buckets for path s3://%s/%s - done" % (self._s3_bucket_name, full_bucket_dir))
        bucket_list = []
        for bucket in buckets:
            bucket_name = os.path.relpath(bucket.key, full_bucket_dir).split('/', 1)[0]
            if (bucket_name.startswith('db') or bucket_name.startswith('rb')) and not bucket_name in bucket_list:
                bucket_list.append(bucket_name)
        return bucket_list

    def restore_bucket(self, index: str, bucket_name: str, destdir: str):
        full_bucket_dir = self._full_path(os.path.join(index,bucket_name))
        objects = self._s3_bucket.objects.filter(Prefix=full_bucket_dir)
        for object in objects:
            object_partdir = object.key.split(os.path.join(self._archive_dir,index), 1)[1].split('/', 1)[1]
            path, filename = os.path.split(object_partdir)
            
            object_dir = os.path.join(destdir,path)
            if not os.path.isdir(object_dir):
                os.makedirs(object_dir)
            
            self._s3_client.download_file(self._s3_bucket_name, object.key, os.path.join(destdir,object_partdir))
