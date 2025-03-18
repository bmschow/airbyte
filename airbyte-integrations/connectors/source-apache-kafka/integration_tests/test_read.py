# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timezone
from unittest import TestCase

import freezegun
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import read
from airbyte_cdk.models import ConfiguredAirbyteCatalog, SyncMode
from source_apache_kafka import SourceApacheKafka

from mockafka import FakeProducer, FakeAdminClientImpl
from mockafka.admin_client import NewTopic
from random import randint

_A_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "is_test": True,
    "topics": "test.topic"
}
_NOW = datetime.now(timezone.utc)

@freezegun.freeze_time(_NOW.isoformat())
class FullRefreshTest(TestCase):

    def test_read_4_message_full_queue(self) -> None:
        admin = FakeAdminClientImpl()
        admin.create_topics([
            NewTopic(topic='test.topic', num_partitions=5)
        ])

        # Produce messages
        producer = FakeProducer()
        for i in range(0, 10):
            producer.produce(
                topic='test.topic',
                key=f'test_key{i}'.encode('utf-8'),
                value=f'test_value{i}'.encode('utf-8'),
                partition=randint(0, 4)
            )

        # Subscribe consumer
        output = read(SourceApacheKafka(), _A_CONFIG, _configured_catalog("test.topic", SyncMode.full_refresh))
        print('output')
        print(output.records)
        assert len(output.records) == 10

def _configured_catalog(stream_name: str, sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(stream_name, sync_mode).build()
