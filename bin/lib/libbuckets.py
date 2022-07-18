import datetime

class Bucket:
    def __init__(self, name=None):
        self.__name = name
        self.__prefix = None
        self.__start = None
        self.__end = None
        self.__id = None
        self.__peerguid = None
        if name != None:
            self.__info = name.split('_')
            self.__prefix = self.__info[0]
            self.__start = int(self.__info[2])
            self.__end = int(self.__info[1])
            self.__id = int(self.__info[3])
            self.__peerguid = self.__info[4]

    def __str__(self) -> str:
        return ("Bucket Name: " + self.__name + ", prefix: " + self.__prefix + ", start: " + str(datetime.fromtimestamp(self.__start)) + ", end: " + str(datetime.fromtimestamp(self.__end)) + ", id: " + str(self.__id) + ", peerguid: " + self.__peerguid)

    def __repr__(self) -> str:
        return self.__name

    @property
    def name(self):
        return self.__name

    @property
    def prefix(self):
        return self.__prefix

    @property
    def start(self):
        return int(self.__start)

    @property
    def end(self):
        return int(self.__end)

    @property
    def id(self):
        return self.__id

    @property
    def peer(self):
        return self.__peerguid

    @name.setter
    def name(self, name):
        self.__name = name

class BucketIndex:
    def __init__(self, index=None, name=None):
        self.__name = name
        self.__index = index
        self._buckets = []

    def __str__(self) -> str:
        return ("index: " + self.__index + " buckets: " + str(self.len()))

    def __repr__(self) -> str:
        return self.__name

    def __iter__(self):
        return iter(self._buckets)

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):
        self.__name = name

    @property
    def index(self):
        return self.__index

    @index.setter
    def name(self, index):
        self.__index = index

    def len(self):
        return len(self._buckets)

    def add(self, bucket_name):
        self._buckets.append(Bucket(name=bucket_name))

    def append(self, bucket):
        self._buckets.append(bucket)

    def buckets(self):
        return self._buckets

    def filter(self, epocstart, epocend):
        self._filtered_buckets = BucketIndex(index=self.__index, name="filtered")
        for bucket in self._buckets:
            # Check if we need this bucket
            # bucket start between
            if bucket.start >= epocstart and bucket.start <= epocend:
                self._filtered_buckets.append(bucket)
                #print("Bucket selected:", bucket)
            # bucket end in between
            elif bucket.end >= epocstart and bucket.end <= epocend:
                self._filtered_buckets.append(bucket)
                #print("Bucket selected:", bucket)
            # bucket start before and end after
            elif bucket.start <= epocstart and bucket.end >= epocend:
                self._filtered_buckets.append(bucket)
                #print("Bucket selected:", bucket)
            #else:
                #print("### Bucket not selected:", bucket)

        return self._filtered_buckets

    def older(self, retention: int):
        self._filtered_buckets = BucketIndex(index=self.__index, name="olderthan")
        check_tstamp = datetime.datetime.today() - datetime.timedelta(days=retention)
        check_date = datetime.datetime.strftime(check_tstamp, "%d.%m.%Y %H:%M:%S")
        for bucket in self._buckets:
            # Check if we need this bucket
            # Bucket end must be older than the retention
            if datetime.datetime.fromtimestamp(bucket.end) <= check_tstamp:
                self._filtered_buckets.append(bucket)
                #print("Bucket selected:", bucket.name)

        return self._filtered_buckets