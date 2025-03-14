from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
import logging
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode, AirbyteMessage
import confluent_kafka
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import json

StreamData = Union[Mapping[str, Any], AirbyteMessage]

logger = logging.getLogger("airbyte")

class SourceApacheKafka(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking connection to kafka bootstrap servers...")
        kafka_consumer_config = {
            "auto.offset.reset": "earliest",
            "bootstrap.servers": config['bootstrap_servers'],
            "group.id": 'test_group',
            "enable.auto.commit": False,
            "logger": logger
        }

        try:
            consumer = confluent_kafka.Consumer(kafka_consumer_config)
        except Exception as e:
            logger.info(f"Kafka connection failed: {e}")
            return False, e

        logger.info(f"Kafka connection success")
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Topic('test.topic', config)]


class Topic(Stream):
    # Set this as a noop.
    primary_key = None

    def __init__(self, topic_name: str, config: dict, **kwargs):
        super().__init__(**kwargs)
        self.topic_name = topic_name
        self.config = config

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: Optional[List[str]] = None,
            stream_slice: Optional[Mapping[str, Any]] = None,
            stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        consumer = self.get_consumer(sync_mode)
        consumer.subscribe([self.topic_name])
        try:
            messages = consumer.consume()
            for message in messages:
                print('Message:')
                print(message.value())
                value =message.value().decode("utf-8")
                key = message.key().decode("utf-8")
                yield {'key': key, 'value': value}
        finally:
            consumer.close()

    def get_consumer(self, sync_mode):
        if sync_mode == SyncMode.full_refresh:
            offset = 'earliest'
        else:
            offset = 'latest'
        kafka_consumer_config = {
            "auto.offset.reset": offset,
            "bootstrap.servers": self.config['bootstrap_servers'],
            "group.id": 'test_group',
            "enable.auto.commit": False,
            "logger": logger
        }
        return confluent_kafka.Consumer(kafka_consumer_config)

