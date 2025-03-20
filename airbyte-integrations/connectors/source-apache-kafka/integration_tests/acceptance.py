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
import logging
import sys
print(sys.executable)

# pytest_plugins = ("connector_acceptance_test.plugin",)

TEST_TOPIC = 'acceptance.test.topic'
BOOTSTRAP_SERVERS = 'localhost:9092'

logger = logging.getLogger("airbyte")


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


def test_read_sequential_slices():
    print('setting up topics...')
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    
    # Delete topic if it exists
    try:
        admin_client.delete_topics([TEST_TOPIC])
        wait_for_kafka_ready(admin_client)
    except Exception:
        pass
    
    # Create topic with 2 partitions and wait for it to be ready
    admin_client.create_topics([NewTopic(TEST_TOPIC, num_partitions=2)])
    if not wait_for_kafka_ready(admin_client, topic=TEST_TOPIC):
        raise Exception(f"Topic {TEST_TOPIC} failed to become ready within timeout period")
    
    # First slice: Write 4 messages (2 to each partition)
    print('producing first slice of messages...')
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "security.protocol": "PLAINTEXT"})
    for i in range(4):
        # Use partition key to ensure messages go to specific partitions
        producer.produce(TEST_TOPIC, partition=i%2, key=f"partition_{i%2}", value=f"value_slice1_{i}")
    producer.flush()

    config = {
        "bootstrap_servers": "localhost:9092",
        "topics": TEST_TOPIC
    }

    # First read - should get 4 messages
    output1 = read(SourceApacheKafka(), config, _configured_catalog(TEST_TOPIC, SyncMode.incremental))
    assert len(output1.records) == 4
    print(f'First read got {len(output1.records)} records')
    
    # Verify state was emitted after first read
    assert len(output1.state_messages) > 0, "State should be emitted after first read"
    first_state = output1.state_messages[-1].state.stream.stream_state
    assert first_state, "State should contain partition information"
    assert len(first_state.partitions) == 2, "State should contain information for both partitions"

    # Second slice: Write 4 more messages
    print('producing second slice of messages...')
    for i in range(4):
        producer.produce(TEST_TOPIC, partition=i%2, key=f"partition_{i%2}", value=f"value_slice2_{i}")
    producer.flush()

    # Second read - should get 4 new messages, using state from first read
    output2 = read(SourceApacheKafka(), config, _configured_catalog(TEST_TOPIC, SyncMode.incremental), state=[output1.state_messages[-1].state])
    assert len(output2.records) == 4
    print(f'Second read got {len(output2.records)} records')
    
    # Verify state was emitted after second read
    assert len(output2.state_messages) > 0, "State should be emitted after second read"
    second_state = output2.state_messages[-1].state.stream.stream_state
    assert second_state, "State should contain partition information"
    assert len(second_state.partitions) == 2, "State should contain information for both partitions"
    
    # Verify state was updated with new offsets
    assert second_state.partitions['0'] > first_state.partitions['0'], "State should be updated with new offsets"
    assert second_state.partitions['1'] > first_state.partitions['1'], "State should be updated with new offsets"

    # Verify we got different messages in each read
    first_read_values = set([str(record.record.data) for record in output1.records])
    second_read_values = set([str(record.record.data) for record in output2.records])
    assert not first_read_values.intersection(second_read_values), "Should not have overlapping messages between reads"

    # Verify messages are properly partitioned
    partition0_messages = [r for r in output1.records + output2.records if r.record.data['key'] == "partition_0"]
    partition1_messages = [r for r in output1.records + output2.records if r.record.data['key'] == "partition_1"]
    assert len(partition0_messages) == 4, "Should have 4 messages in partition 0"
    assert len(partition1_messages) == 4, "Should have 4 messages in partition 1"


def test_discover_method():
    print('setting up topics for discover test...')
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    
    test_topics = ['discover.test.topic1', 'discover.test.topic2', 'discover.test.topic3']
    internal_topic = '_internal.topic'  # This should be filtered out
    default_topic = 'default_ksql_processing_log'  # Auto-created by Kafka
    
    # Delete topics if they exist
    try:
        admin_client.delete_topics(test_topics + [internal_topic, default_topic])
        wait_for_kafka_ready(admin_client)
    except Exception:
        pass
    
    # Create topics and wait for them to be ready
    admin_client.create_topics([NewTopic(topic, num_partitions=1) for topic in test_topics + [internal_topic]])
    for topic in test_topics + [internal_topic]:
        if not wait_for_kafka_ready(admin_client, topic=topic):
            raise Exception(f"Topic {topic} failed to become ready within timeout period")
    
    config = {
        "bootstrap_servers": "localhost:9092"
        # Note: No topics specified in config - discover should find them automatically
    }
    
    source = SourceApacheKafka()
    catalog = source.discover(logger, config)
    
    # Verify we got a stream for each non-internal topic
    assert len(catalog.streams) == 3, f"Expected 3 streams, got {len(catalog.streams)}"
    
    # Verify each stream has the correct name and schema
    discovered_topics = set()
    for stream in catalog.streams:
        assert stream.name in test_topics, f"Unexpected stream name: {stream.name}"
        discovered_topics.add(stream.name)
        
        # Verify internal topic and default topic were not included
        assert stream.name != internal_topic, "Internal topic should not be discovered"
        assert stream.name != default_topic, "Default KSQL topic should not be discovered"
        
        # Verify schema matches topic.json
        schema = stream.json_schema
        assert "properties" in schema, "Schema should have properties"
        assert "key" in schema["properties"], "Schema should have key field"
        assert "value" in schema["properties"], "Schema should have value field"
        assert "offset" in schema["properties"], "Schema should have offset field"
        assert "partition" in schema["properties"], "Schema should have partition field"
    
    # Verify we discovered all non-internal topics
    assert discovered_topics == set(test_topics), "Not all topics were discovered"
