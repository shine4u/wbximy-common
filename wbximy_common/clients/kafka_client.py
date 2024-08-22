# encoding=utf8

import re
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError
from wbximy_common.clients.tunnel import TunnelMixin

logger = logging.getLogger(__name__)


class KafkaProducerClient(TunnelMixin):

    def __init__(self, bootstrap_servers: str, kafka_topic: str):
        super().__init__()
        self.kafka_topic: str = kafka_topic
        self.tunnel = False  # 公司内部KAFKA需要挂VPN连接 所以 强制设置 不建隧道
        mo = re.fullmatch(r'([^:]+):(\d+)', bootstrap_servers)
        if mo:
            self.host, self.port = mo.group(1), int(mo.group(2))
        self.mix()
        if mo:
            bootstrap_servers = '{}:{}'.format(self.host, self.port)
        logger.info('init kafka producer %s ...', bootstrap_servers)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        logger.info('init kafka producer done. %s %s', bootstrap_servers, self.kafka_topic)

    def write(self, message: str, **kwargs) -> bool:
        key = kwargs.pop('key', None)
        try:
            self.producer.send(
                topic=self.kafka_topic,
                key=key,
                value=message.encode('utf8'),
            )
        except KafkaError as e:
            logger.warning('fail write message=%s e=%s', message, e)
            return False
        self.producer.flush()
        return True

    def close(self):
        self.producer.close()


class KafkaConsumerClient(TunnelMixin):
    def __init__(self, bootstrap_servers: str, kafka_topic: str, **kwargs):
        super().__init__()
        self.kafka_topic: str = kafka_topic
        self.tunnel = False  # 公司内部KAFKA需要挂VPN连接 所以 强制设置 不建隧道
        mo = re.fullmatch(r'([^:]+):(\d+)', bootstrap_servers)
        if mo:
            self.host, self.port = mo.group(1), int(mo.group(2))
        self.mix()
        if mo:
            bootstrap_servers = '{}:{}'.format(self.host, self.port)
        logger.info('init kafka consumer %s ...', bootstrap_servers)

        self.group_id: str = kwargs.pop('group_id', 'default')
        self.earliest_offset: bool = kwargs.pop('earliest_offset', True)
        self.auto_commit: bool = kwargs.pop('auto_commit', True)
        auto_offset_reset = 'smallest' if self.earliest_offset else 'largest'
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=self.auto_commit,
            auto_commit_interval_ms=100,
            auto_offset_reset=auto_offset_reset,  # {'smallest': 'earliest', 'largest': 'latest'}
        )
        logger.info('init kafka consumer done. %s', self.kafka_topic)

    def read(self, **kwargs):
        utf8_decode: bool = kwargs.pop('utf8_decode', True)
        for message in self.consumer:
            message: ConsumerRecord = message
            data = message.value
            if utf8_decode:
                data = data.decode('utf8')
            yield data

    def close(self):
        self.consumer.close()
