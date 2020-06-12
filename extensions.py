import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured

from . import connection

logger = logging.getLogger(__name__)


class IdleClosedExensions(object):

    def __init__(self, idle_number, crawler):
        self.crawler = crawler
        self.idle_number = idle_number
        self.idle_list = []
        self.idle_count = 0
        kwargs = {
            'persist': crawler.settings.getbool('SCHEDULER_PERSIST'),
            'flush_on_start': crawler.settings.getbool('SCHEDULER_FLUSH_ON_START'),
            'idle_before_close': crawler.settings.getint('SCHEDULER_IDLE_BEFORE_CLOSE'),
        }
        optional = {
            'dupefilter_key': 'SCHEDULER_DUPEFILTER_KEY',
            'dupefilter_cls': 'DUPEFILTER_CLASS',
        }
        for name, setting_name in optional.items():
            val = crawler.settings.get(setting_name)
            if val:
                kwargs[name] = val

        self.server = connection.get_redis(crawler.settings)

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('MYEXT_ENABLED'):
            raise NotConfigured

        idle_number = crawler.settings.getint('IDLE_NUMBER', 360)

        ext = cls(idle_number, crawler)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        return ext

    def spider_opened(self, spider):
        logger.info("opened spider %s redis spider Idle, Continuous idle limit： %d", spider.name, self.idle_number)

    def spider_closed(self, spider):
        logger.info("closed spider %s, idle count %d , Continuous idle count %d",
                    spider.name, self.idle_count, len(self.idle_list))

    def spider_idle(self, spider):
        if self.server.get(spider.name) == 2:
            self.server.close()
            self.crawler.engine.close_spider(spider, 'closespider_pagecount')
