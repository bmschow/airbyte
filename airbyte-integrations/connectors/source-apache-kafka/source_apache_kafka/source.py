from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union

import logging
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import package_name_from_class
from airbyte_cdk.models import SyncMode, AirbyteMessage, AirbyteCatalog
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

from time import sleep

import confluent_kafka
from mockafka import FakeConsumer

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

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        """Implements the Discover operation from the Airbyte Specification.
        See https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/#discover.
        """
        streams = [stream.as_airbyte_stream() for stream in self.streams(config=config)]
        return AirbyteCatalog(streams=streams)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Topic(topic_name, config) for topic_name in config['topics'].split(',')]


class Topic(Stream):
    # Set this as a noop.
    primary_key = None

    def __init__(self, topic_name: str, config: dict, **kwargs):
        super().__init__(**kwargs)
        self.name = topic_name
        self.config = config

    def get_json_schema(self) -> Mapping[str, Any]:
        # overrides super function to just get `topic.json` since all topics will have the same schema
        return ResourceSchemaLoader(package_name_from_class(self.__class__)).get_schema('topic')

    def get_consumer(self, sync_mode):
        if self.config.get('is_test'):
            logger.warning("`is_test` is true, using FakeConsumer for testing")
            return FakeConsumer()
        kafka_consumer_config = {
            "auto.offset.reset": 'latest',
            "bootstrap.servers": self.config['bootstrap_servers'],
            "group.id": 'test_group',
            "enable.auto.commit": False,
            "logger": logger
        }
        logger.info("CONSUMER CONFIG")
        logger.info(kafka_consumer_config)
        return confluent_kafka.Consumer(kafka_consumer_config)

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: Optional[List[str]] = None,
            stream_slice: Optional[Mapping[str, Any]] = None,
            stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        logger.info("Getting consumer")
        consumer = self.get_consumer(sync_mode)
        offset = confluent_kafka.OFFSET_BEGINNING
        consumer.subscribe([self.name], on_assign=_seek_to_offset_callback(offset))
        logger.info("Sleeping while subscription process")
        sleep(1)
        try:
            message = consumer.poll(timeout=3)
            while message:
                value =message.value().decode("utf-8")
                key = message.key().decode("utf-8")
                yield {'key': key, 'value': value}
                message = consumer.poll(timeout=3)
        finally:
            consumer.close()

def get_offset(sync_mode: SyncMode, stream_state):
    if sync_mode == SyncMode.incremental:
        raise NotImplemented("Need to implement")
    if sync_mode == SyncMode.full_refresh:
        return confluent_kafka.OFFSET_BEGINNING
    raise AssertionError("Configuration not supported for offset seeking")


def _seek_to_offset_callback(offset):
    """ Used by the consumer to seek to a specific offset in the topic """

    def on_assign(consumer, partitions):
        for p in partitions:
            logger.info(f"Setting partition {p} to offset {offset}")
            p.offset = offset
        consumer.assign(partitions)

    return on_assign

