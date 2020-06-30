from scrapy import signals, Request
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider

from . import connection
from . import kafka_default_settings as defaults


class Kafka_Scrapy_Mixin(object):
    kafka_topic = None

    def setup_kafka(self, crawler=None):
        if crawler is None:
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings = crawler.settings
        self.kafka_topic = settings.get(
            'KAFKA_START_URLS_KEY', defaults.START_URLS_TOPIC,
        )
        if settings.get('KAFKA_START_URLS_KEY'):
            pass
        else:
            self.kafka_topic = settings.get('START_TOPIC', self.kafka_topic.format(crawler.settings.get('BOT_NAME')))
        self.create_topic = self.settings.getbool('KAFKA_START_TOPIC_CREATE_AUTO',
                                                  defaults.KAFKA_START_TOPIC_CREATE_AUTO)
        if self.create_topic:
            connection.create_topic_client(self.kafka_topic, bootstrap_servers=settings.get('KAFKA_DEFAULTS_HOST',
                                           defaults.KAFKA_DEFAULTS_HOST),
                                           partitions=settings.get('KAFKA_DEFAULTS_START_PARTITIONS',
                                           defaults.KAFKA_DEFAULTS_START_PARTITIONS),
                                           replication_factor=settings.get('KAFKA_DEFAULTS_START_REPLICATION',
                                           defaults.KAFKA_DEFAULTS_START_REPLICATION))
        self.consumer_ = connection.create_start_request_consumer(name=settings.get('KAFKA_START_GROUP', self.name),
                                                                  bootstrap_servers=settings.get('KAFKA_DEFAULTS_HOST',
                                                                  defaults.KAFKA_DEFAULTS_HOST))
        self.consumer_.subscribe([self.kafka_topic])
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def start_requests(self):
        return self.next_requests()

    def next_requests(self):
        found = 0
        self.logger.info("Wait a start URL from kafka topic '%(kafka_topic)s' ", self.__dict__)
        while found < 1:
            data = None
            msg = self.consumer_.poll(0)
            if msg:
                data = msg.value().decode('utf-8')
            if not data:
                break
            req = self.make_request_from_data(data)
            if req:
                found += 1
                yield req

    def make_request_from_data(self, data):
        return self.make_request_from_url(data)

    def schedule_next_requests(self):
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        self.schedule_next_requests()
        raise DontCloseSpider

    def make_request_from_url(self, data):
        return Request(data, dont_filter=True)


class Kafka_Scrapy(Kafka_Scrapy_Mixin, Spider):
    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(Kafka_Scrapy, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_kafka(crawler)
        return obj
