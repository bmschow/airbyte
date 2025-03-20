from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union

import logging
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import package_name_from_class
from airbyte_cdk.models import SyncMode, AirbyteMessage, AirbyteCatalog, AirbyteStateMessage, AirbyteStateType, AirbyteStreamState, StreamDescriptor, AirbyteStateBlob, Type
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

import time
import confluent_kafka
from mockafka import FakeConsumer

StreamData = Union[Mapping[str, Any], AirbyteMessage]

logger = logging.getLogger("airbyte")

class SourceApacheKafka(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking connection to kafka bootstrap servers...")
        kafka_consumer_config = get_consumer_config(config)

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
        kafka_consumer_config = get_consumer_config(config)
        consumer = confluent_kafka.Consumer(kafka_consumer_config)
        
        try:
            # Get cluster metadata which includes topic information
            cluster_metadata = consumer.list_topics(timeout=10)
            all_topics = list(cluster_metadata.topics.keys())
            
            # Filter out internal topics (those starting with _ or __)
            available_topics = [topic for topic in all_topics if not topic.startswith('_')]
            
            if not available_topics:
                logger.warning("No topics found in Kafka cluster")
                return AirbyteCatalog(streams=[])
            
            logger.info(f"Discovered {len(available_topics)} topics: {available_topics}")
            streams = [stream.as_airbyte_stream() for stream in self.streams(config, available_topics)]
            return AirbyteCatalog(streams=streams)
        finally:
            # Make sure to close the consumer
            consumer.close()

    def streams(self, config: Mapping[str, Any], available_topics: List[str] = None) -> List[Stream]:
        """Returns a list of stream instances.
        
        Args:
            config: The user-supplied configuration
            available_topics: Optional list of topics from discover. If not provided, uses topics from config.
        """
        if available_topics is None:
            # Fall back to config-specified topics if available_topics not provided
            available_topics = config['topics'].split(',') if 'topics' in config else []
        
        return [Topic(topic_name, config) for topic_name in available_topics]

def get_consumer_config(config: Mapping[str, Any]) -> Mapping[str, Any]:
    consumer_config = {
        "auto.offset.reset": "earliest",
        "bootstrap.servers": config['bootstrap_servers'],
        "group.id": config.get('group_id') or 'airbyte_group',
        "enable.auto.commit": False,
        "logger": logger
    }

    if config.get('security_protocol'):
        if config['security_protocol'] == 'MSK_IAM':
            def msk_iam_oauth_callback(oauth_config):
                from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
                aws_region = config['iam_aws_region']
                auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(aws_region)
                return auth_token, expiry_ms / 1000
            consumer_config['security.protocol'] = 'SASL_SSL'
            consumer_config['sasl.mechanisms'] = 'OAUTHBEARER'
            consumer_config['oauth_cb'] = msk_iam_oauth_callback
        else:
            consumer_config['security.protocol'] = config['security_protocol']
    if config.get('sasl_mechanism'):
        consumer_config['sasl.mechanisms'] = config['sasl_mechanism']
        
    return consumer_config


class Topic(Stream):
    # Set this as a noop.
    primary_key = None
    cursor_field = 'offset'
    state_checkpoint_interval = 5

    def __init__(self, topic_name: str, config: dict, **kwargs):
        super().__init__(**kwargs)
        self.name = topic_name
        self.config = config
        self._partition_states = {}

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Creates slices for each partition in the topic.
        Each slice contains partition information and offset range.
        """
        # Get admin client to fetch partition information
        admin_client = confluent_kafka.admin.AdminClient({"bootstrap.servers": self.config['bootstrap_servers']})
        wait_for_kafka_ready(admin_client, topic=self.name)
        
        # Get topic metadata to determine partitions
        topic_metadata = admin_client.list_topics().topics[self.name]
        partitions = topic_metadata.partitions
        
        if stream_state and isinstance(stream_state, dict):
            if 'partitions' in stream_state:
                self._partition_states = stream_state['partitions']
            else:
                logger.warning(f"Stream state missing 'partitions' key: {stream_state}")
        
        # Get the number of partitions
        num_partitions = len(partitions)
        
        for partition_id in range(num_partitions):
            # Get last offset for this partition from state
            last_offset = int(self._partition_states.get(str(partition_id), confluent_kafka.OFFSET_BEGINNING))
            
            if sync_mode == SyncMode.incremental and last_offset != confluent_kafka.OFFSET_BEGINNING:
                # For incremental sync, start from last offset + 1 to avoid rereading the last message
                current_offset = last_offset + 1
            else:
                current_offset = confluent_kafka.OFFSET_BEGINNING
            
            # Create a slice for this partition
            yield {
                "partition": partition_id,
                "start_offset": current_offset,
                "end_offset": current_offset + 1000  # Process 1000 messages per slice
            }

    def get_json_schema(self) -> Mapping[str, Any]:
        return ResourceSchemaLoader(package_name_from_class(self.__class__)).get_schema('topic')

    def get_consumer(self):
        kafka_consumer_config = get_consumer_config(self.config)
        logger.info("CONSUMER CONFIG")
        logger.info(kafka_consumer_config)
        return confluent_kafka.Consumer(kafka_consumer_config)

    @property
    def state(self) -> Mapping[str, Any]:
        return {
            'partitions': self._partition_states
        }

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._partition_states = value.get('partitions', {})

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: Optional[List[str]] = None,
            stream_slice: Optional[Mapping[str, Any]] = None,
            stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        logger.info("Getting consumer")
        consumer = self.get_consumer()
        
        partition = stream_slice.get("partition")
        start_offset = stream_slice.get("start_offset", confluent_kafka.OFFSET_BEGINNING)
        end_offset = stream_slice.get("end_offset", float('inf'))
        
        # Get admin client for waiting for Kafka to be ready
        admin_client = confluent_kafka.admin.AdminClient({"bootstrap.servers": self.config['bootstrap_servers']})
        wait_for_kafka_ready(admin_client, topic=self.name)
        
        # Subscribe to specific partition
        topic_partition = confluent_kafka.TopicPartition(self.name, partition, start_offset)
        consumer.assign([topic_partition])
        
        logger.info(f"Processing partition {partition} from offset {start_offset} to {end_offset}")
        
        try:
            message = consumer.poll(timeout=3)
            messages_read = 0
            last_state_emit = 0
            
            while message:
                current_offset = message.offset()
                if current_offset >= end_offset:
                    break
                    
                value = message.value().decode("utf-8")
                key = message.key().decode("utf-8")
                yield {
                    'key': key, 
                    'value': value, 
                    'offset': current_offset,
                    'partition': partition
                }
                # Update state for this partition
                self._partition_states[str(partition)] = current_offset
                messages_read += 1
                
                # Emit state message every state_checkpoint_interval messages
                if messages_read - last_state_emit >= self.state_checkpoint_interval:
                    yield AirbyteMessage(
                        type=Type.STATE,
                        state=AirbyteStateMessage(
                            type=AirbyteStateType.STREAM,
                            stream=AirbyteStreamState(
                                stream_descriptor=StreamDescriptor(name=self.name),
                                stream_state=AirbyteStateBlob(self.state)
                            ),
                        )
                    )
                    last_state_emit = messages_read
                
                message = consumer.poll(timeout=3)
            
            # Always emit final state at the end of the read
            if messages_read > 0:  # Only emit if we read any messages
                yield AirbyteMessage(
                    type=Type.STATE,
                    state=AirbyteStateMessage(
                        type=AirbyteStateType.STREAM,
                        stream=AirbyteStreamState(
                            stream_descriptor=StreamDescriptor(name=self.name),
                            stream_state=AirbyteStateBlob(self.state)
                        ),
                    )
                )
        finally:
            consumer.close()

def get_offset(sync_mode: SyncMode, stream_state):
    if sync_mode == SyncMode.incremental:
        if stream_state and 'partitions' in stream_state:
            # For incremental sync, use the last offset from state
            return max(int(offset) for offset in stream_state['partitions'].values())
        return confluent_kafka.OFFSET_BEGINNING
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

def wait_for_kafka_ready(admin_client, topic: Optional[str] = None, max_retries: int = 40, retry_delay: float = 0.5) -> bool:
    """
    Wait for Kafka to be ready and optionally for a specific topic to exist.
    Returns True if successful, False if timeout.
    """
    for _ in range(max_retries):
        try:
            # Check if we can connect to Kafka
            admin_client.list_topics()
            
            # If a topic is specified, check if it exists
            if topic:
                topics = admin_client.list_topics().topics
                if topic in topics:
                    return True
                else:
                    raise Exception(f"Topic {topic} does not exist")
            else:
                return True
        except Exception:
            time.sleep(retry_delay)
    return False