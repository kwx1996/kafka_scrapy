START_URLS_TOPIC = 'start-topic-{}'

SCHEDULER_DUPEFILTER_KEY = '{}:dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'kafka_scrapy.dupefilter.RFPDupeFilter'
REDIS_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'encoding': 'utf-8',
}

DUPEFILTER_KEY = 'dupefilter:%(timestamp)s'

topic = 'kafka-for-spider-test'
SCHEDULER_QUEUE_CLASS = 'kafka_scrapy.queues.KafkaQueue'
KAFKA_DEFAULTS_HOST = 'localhost:9092'
KAFKA_DEFAULTS_TOPIC = 'topic-{}'
KAFKA_DEFAULTS_PARTITIONS = 3
KAFKA_DEFAULTS_REPLICATION = 1
KAFKA_TOPIC_CREATE_AUTO = False
KAFKA_START_TOPIC_CREATE_AUTO = False
BLOOMFILTER_BLOCK = 1
BLOOMFILTER_SIZE = 31
BLOOMFILTER_SEED = 6
KAFKA_DEFAULTS_START_PARTITIONS = 1
KAFKA_DEFAULTS_START_REPLICATION = 1
