from confluent_kafka import Consumer, Producer
from scrapy.utils.reqser import request_to_dict, request_from_dict


class Base(object):
    """Per-spider base queue class"""

    def __init__(self, producer: Producer, consumer: Consumer,
                 spider, topic, serializer, server):
        self.producer = producer
        self.consumer = consumer
        self.consumer.subscribe(topic)
        self.spider = spider
        self.serializer = serializer
        self.topic = topic
        self.partition = None
        self.server = server

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def close(self):
        """Clear queue/stack"""
        self.consumer.close()


class KafkaQueue(Base):

    def __len__(self):
        """Return the length of the queue"""
        return 0

    def push(self, request):
        """Push a request"""
        self.producer.poll(0)
        self.producer.produce(self.topic[0], self._encode_request(request))
        self.producer.flush()

    def pop(self, timeout=0):
        """Pop a request"""
        request = self.consumer.poll(0)
        if request:
            data = request.value()
            return self._decode_request(data)
