import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured
from .offset_monitor import check

logger = logging.getLogger(__name__)


# this example show a plan to close the spider
class IdleClosedExensions(object):

    def __init__(self, idle_number, crawler):
        self.crawler = crawler
        self.idle_number = idle_number
        self.idle_list = []
        self.idle_count = 0

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
        res = check('sh kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group_id')
        if res == 'finished':
            self.crawler.engine.close_spider(spider, 'closespider_pagecount')
