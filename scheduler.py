from confluent_kafka.cimpl import TopicPartition
from scrapy import signals
from scrapy.utils.misc import load_object

from . import connection
from . import kafka_default_settings as defaults
from . import picklecompat


class Scheduler(object):
    def __init__(self, server,
                 persist=False,
                 flush_on_start=False,
                 dupefilter_key=defaults.SCHEDULER_DUPEFILTER_KEY,
                 dupefilter_cls=defaults.SCHEDULER_DUPEFILTER_CLASS,
                 queue_cls=defaults.SCHEDULER_QUEUE_CLASS,
                 topic=defaults.KAFKA_DEFAULTS_TOPIC,
                 serializer=None,
                 idle_before_close=0,
                 ):
        self.server = server
        self.consumer = None
        self.producer = None
        self.topic = topic
        self.dupefilter_cls = dupefilter_cls
        self.queue_cls = queue_cls
        self.persist = persist
        self.flush_on_start = flush_on_start
        self.dupefilter_cls = dupefilter_cls
        self.dupefilter_key = dupefilter_key
        self.stats = None
        self.queue = None
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        self.serializer = serializer
        self.idle_before_close = idle_before_close
        self.check_times = 0
        self.consume_offset = None
        self.consumers_offset = []

    @classmethod
    def from_settings(cls, settings):
        kwargs = {
            'persist': settings.getbool('SCHEDULER_PERSIST'),
            'flush_on_start': settings.getbool('SCHEDULER_FLUSH_ON_START'),
            'idle_before_close': settings.getint('SCHEDULER_IDLE_BEFORE_CLOSE'),
        }

        optional = {
            'dupefilter_key': 'SCHEDULER_DUPEFILTER_KEY',
            'dupefilter_cls': 'DUPEFILTER_CLASS',
        }
        for name, setting_name in optional.items():
            val = settings.get(setting_name)
            if val:
                kwargs[name] = val

        server = connection.get_redis(settings)
        server.ping()

        return cls(server=server, **kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls.from_settings(crawler.settings)
        instance.stats = crawler.stats
        idle_number = crawler.settings.getint('IDLE_NUMBER', 360)
        ext = cls(idle_number, crawler)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        return instance

    def open(self, spider):
        self.spider = spider
        self.settings = self.spider.settings
        self.create_topic = self.settings.getbool('KAFKA_TOPIC_CREATE_AUTO', defaults.KAFKA_TOPIC_CREATE_AUTO)
        if self.create_topic:
            connection.create_topic_client(self.topic.format(self.spider.name),
                                           bootstrap_servers=self.settings.get('KAFKA_DEFAULTS_HOST',
                                                                               defaults.KAFKA_DEFAULTS_HOST),
                                           partitions=self.settings.get('KAFKA_DEFAULTS_PARTITIONS',
                                                                        defaults.KAFKA_DEFAULTS_PARTITIONS),
                                           replication_factor=self.settings.get('KAFKA_DEFAULTS_REPLICATION',
                                                                                defaults.KAFKA_DEFAULTS_REPLICATION))
        self.consumer = connection.create_consumer(self.spider.name,
                                                   bootstrap_servers=self.settings.get('KAFKA_DEFAULTS_HOST',
                                                                                       defaults.KAFKA_DEFAULTS_HOST))
        self.producer = connection.create_producer(bootstrap_servers=self.settings.get('KAFKA_DEFAULTS_HOST',
                                                                                       defaults.KAFKA_DEFAULTS_HOST))
        try:
            self.df = load_object(self.dupefilter_cls)(
                server=self.server,
                key=self.dupefilter_key.format(self.spider.name),
                debug=spider.settings.getbool('DUPEFILTER_DEBUG'),
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate dupefilter clasPs '%s': %s",
                             self.dupefilter_cls, e)
        try:
            self.queue = load_object(self.queue_cls)(
                producer=self.producer,
                consumer=self.consumer,
                spider=spider,
                topic=[self.topic.format(self.spider.name)],
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate queue class '%s': %s",
                             self.queue_cls, e)

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        self.queue.push(request)
        return True

    def next_request(self):
        request = self.queue.pop()
        if not request:
            return
        if self.df._request_seen(request):
            return None
        if request and self.stats:
            self.stats.inc_value('scheduler/dequeued/kafka', spider=self.spider)
        return request

    def close(self):
        self.df.clear()
        self.producer.close()
        self.consumer.close()

    def has_pending_requests(self):
        return False

    def spider_idle(self):
        if isinstance(self.queue.partition, int):
            self._consume_offset = self.queue.consumer.get_watermark_offsets(
                partition=TopicPartition(topic=defaults.KAFKA_DEFAULTS_TOPIC,
                                         partition=self.queue.partition))[1]
            if self.consume_offset == self._consume_offset:
                self.check_times += 1
                if self.check_times >= 2:
                    self.server.set(self.spider.name, self.check_times)
            else:
                self.consume_offset = self._consume_offset
        elif isinstance(self.queue.partition, list):
            self._consumers_offset = []
            for partition in self.queue.partition:
                self._consumers_offset.append(self.queue.consumer.get_watermark_offsets(
                    partition=TopicPartition(topic=defaults.KAFKA_DEFAULTS_TOPIC, partition=partition))[1])
            if self.consumers_offset == self._consumers_offset:
                self.check_times += 1
                if self.check_times >= 2:
                    self.server.set(self.spider.name, self.check_times)
            else:
                self.consumers_offset = self._consumers_offset
