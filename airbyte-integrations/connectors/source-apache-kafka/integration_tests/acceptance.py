#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import subprocess
import pytest
from airbyte_cdk.test.entrypoint_wrapper import read
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.models import ConfiguredAirbyteCatalog, SyncMode
from source_apache_kafka import SourceApacheKafka
from source_apache_kafka.source import wait_for_kafka_ready
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time
from typing import Optional

import sys
print(sys.executable)

# pytest_plugins = ("connector_acceptance_test.plugin",)

TEST_TOPIC = 'acceptance.test.topic'
BOOTSTRAP_SERVERS = 'localhost:9092'


@pytest.fixture(scope="session", autouse=True)
def connector_setup():
    subprocess.call(["docker", "compose", "-f", "integration_tests/docker-compose.yml", "up", "-d"])
    
    # Wait for Kafka to be ready
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    if not wait_for_kafka_ready(admin_client):
        raise Exception("Kafka failed to become ready within timeout period")

    yield

    subprocess.call(["docker", "compose", "-f", "integration_tests/docker-compose.yml", "down"])


def test_read_two_messages_full_refresh():
    print('setting up topics...')
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    
    # Delete topic if it exists
    try:
        admin_client.delete_topics([TEST_TOPIC])
        wait_for_kafka_ready(admin_client)
    except Exception:
        pass
    
    # Create topic and wait for it to be ready
    admin_client.create_topics([NewTopic(TEST_TOPIC, num_partitions=2)])
    if not wait_for_kafka_ready(admin_client, topic=TEST_TOPIC):
        raise Exception(f"Topic {TEST_TOPIC} failed to become ready within timeout period")
    
    print('producing messages...')
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "security.protocol": "PLAINTEXT"})
    producer.produce(TEST_TOPIC, key="test_key", value="test_value")
    producer.produce(TEST_TOPIC, key="test_key_2", value="test_value_2")
    producer.flush()

    config = {
        "bootstrap_servers": "localhost:9092",
        "topics": TEST_TOPIC
    }

    output = read(SourceApacheKafka(), config, _configured_catalog(TEST_TOPIC, SyncMode.full_refresh))
    print('output')
    print(output.records)
    assert len(output.records) == 2


def _configured_catalog(stream_name: str, sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(stream_name, sync_mode).build()


def test_read_6_messages_updates_state():
    print('setting up topics...')
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    
    # Delete topic if it exists
    try:
        admin_client.delete_topics([TEST_TOPIC])
        wait_for_kafka_ready(admin_client)
    except Exception:
        pass
    
    # Create topic and wait for it to be ready
    admin_client.create_topics([NewTopic(TEST_TOPIC, num_partitions=2)])
    if not wait_for_kafka_ready(admin_client, topic=TEST_TOPIC):
        raise Exception(f"Topic {TEST_TOPIC} failed to become ready within timeout period")
    
    print('producing messages...')
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "security.protocol": "PLAINTEXT"})
    for i in range(6):
        producer.produce(TEST_TOPIC, key=f"test_key_{i}", value=f"test_value_{i}")
    producer.flush()

    config = {
        "bootstrap_servers": "localhost:9092",
        "topics": TEST_TOPIC
    }

    output = read(SourceApacheKafka(), config, _configured_catalog(TEST_TOPIC, SyncMode.full_refresh))
    print('output')
    print(output.records)
    assert len(output.records) == 6


def _configured_catalog(stream_name: str, sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(stream_name, sync_mode).build()
