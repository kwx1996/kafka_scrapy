import logging
import time

import mmh3
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint

from . import kafka_default_settings as defaults
from .connection import get_redis

logger = logging.getLogger(__name__)


# TODO: Rename class to RedisDupeFilter.

class RFPDupeFilter(BaseDupeFilter):
    """Redis-based request duplicates filter.

    This class can also be used with default Scrapy's scheduler.

    """

    logger = logger

    def __init__(self, server, key, debug=False, **kwargs):
        """Initialize the duplicates filter.
        blockNum, bit_size, seeds
        Parameters
        ----------
        server : redis.StrictRedis
            The redis server instance.
        key : str
            Redis key Where to store fingerprints.
        debug : bool, optional
            Whether to log filtered requests.

        """
        self.server = server
        self.key = key
        self.debug = debug
        self.seeds = kwargs.get("seeds", defaults.BLOOMFILTER_SEED)
        self.blockNum = kwargs.get("blockNum", defaults.BLOOMFILTER_BLOCK)
        self.bit_size = 1 << kwargs.get("bit_size", defaults.BLOOMFILTER_SIZE)
        self.logdupes = True

    @classmethod
    def from_settings(cls, settings):
        """Returns an instance from given settings.

        This uses by default the key ``dupefilter:<timestamp>``. When using the
        ``scrapy_redis.scheduler.Scheduler`` class, this method is not used as
        it needs to pass the spider name in the key.

        Parameters
        ----------
        settings : scrapy.settings.Settings

        Returns
        -------
        RFPDupeFilter
            A RFPDupeFilter instance.


        """
        server = get_redis(settings)
        # XXX: This creates one-time key. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        # TODO: Use SCRAPY_JOB env as default and fallback to timestamp.
        key = defaults.DUPEFILTER_KEY % {'timestamp': int(time.time())}
        block_num = settings.getint('BLOOMFILTER_BLOCK', defaults.BLOOMFILTER_BLOCK)
        bit_size = settings.getint('BLOOMFILTER_SIZE', defaults.BLOOMFILTER_SIZE)
        seeds_num = settings.getint('BLOOMFILTER_SEED', defaults.BLOOMFILTER_SEED)
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(server, key=key, blockNum=block_num, bit_size=bit_size, seeds=seeds_num, debug=debug)

    @classmethod
    def from_crawler(cls, crawler):
        """Returns instance from crawler.

        Parameters
        ----------
        crawler : scrapy.crawler.Crawler

        Returns
        -------
        RFPDupeFilter
            Instance of RFPDupeFilter.

        """
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        """Returns True if request was already seen.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        bool

        """
        fp = self.request_fingerprint(request)
        if self.isContains(fp):
            return True
        self.insert(fp)
        return False

    def request_fingerprint(self, request):
        """Returns a fingerprint for a given request.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        str

        """
        return request_fingerprint(request)

    def isContains(self, str_input):
        if not str_input:
            return False

        name = self.key + str(sum(map(ord, str_input)) % self.blockNum)
        for seed in range(self.seeds):
            loc = mmh3.hash(str_input, seed, signed=False)
            if self.server.getbit(name, loc % self.bit_size) == 0:
                break
        else:
            return True
        return False

    def insert(self, str_input):
        name = self.key + str(sum(map(ord, str_input)) % self.blockNum)
        for seed in range(self.seeds):
            loc = mmh3.hash(str_input, seed, signed=False)
            self.server.setbit(name, loc % self.bit_size, 1)

    @classmethod
    def from_spider(cls, spider):
        settings = spider.settings
        server = get_redis(settings)
        dupefilter_key = settings.get("SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY)
        key = dupefilter_key.format(spider.name)
        debug = settings.getbool('DUPEFILTER_DEBUG')
        block_num = settings.getint('BLOOMFILTER_BLOCK', defaults.BLOOMFILTER_BLOCK)
        bit_size = settings.getint('BLOOMFILTER_SIZE', defaults.BLOOMFILTER_SIZE)
        seeds_num = settings.getint('BLOOMFILTER_SEED', defaults.BLOOMFILTER_SEED)
        return cls(server, key=key, blockNum=block_num, bit_size=bit_size, seeds=seeds_num, debug=debug)

    def close(self, reason=''):
        """Delete data on close. Called by Scrapy's scheduler.

        Parameters
        ----------
        reason : str, optional

        """
        self.clear()

    def clear(self):
        """Clears fingerprints data."""
        self.server.delete(self.key)

    def log(self, request, spider):
        """Logs given request.

        Parameters
        ----------
        request : scrapy.http.Request
        spider : scrapy.spiders.Spider

        """
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

    def _request_seen(self, request):
        """Returns True if request was already seen.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        bool

        """
        fp = self.request_fingerprint(request)
        if self._isContains(fp):
            return True
        self._insert(fp)
        return False

    def _isContains(self, str_input):
        if not str_input:
            return False

        name = self.key + str(sum(map(ord, str_input)) % self.blockNum)
        for seed in range(self.seeds):
            loc = mmh3.hash(str_input, seed, signed=False)
            if self.server.getbit(name+'_crawled', loc % self.bit_size) == 0:
                break
        else:
            return True
        return False

    def _insert(self, str_input):
        name = self.key + str(sum(map(ord, str_input)) % self.blockNum)
        for seed in range(self.seeds):
            loc = mmh3.hash(str_input, seed, signed=False)
            self.server.setbit(name+'_crawled', loc % self.bit_size, 1)
