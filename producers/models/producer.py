"""Producer base-class providing common utilites and functionality"""
import logging
import time
import sys

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
GROUP_ID = "UDACITY_STREAMING_NANODEGREE"



class Producer:
    """Defines and provides common functionality amongst Producers"""

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "group.id": GROUP_ID,
            "partition.assignment.strategy": "roundrobin"
        }

        self.client = AdminClient(self.broker_properties)

        self.schema_registry = CachedSchemaRegistryClient(
            {
                "url": SCHEMA_REGISTRY_URL        
            }
        )

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            config = self.broker_properties,
            schema_registry = self.schema_registry
        )

    def topic_exists(self):
        """Checks if the given topic exists"""
        return self.client.list_topics(topic=self.topic_name).topics.get(self.topic_name) is not None


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        if Producer.topic_exists(self):
            logger.info(f"topic {self.topic_name} already exists. Exiting without creation.")
        else:
            futures = self.client.create_topics(
                [
                    NewTopic(
                        topic = self.topic_name,
                        num_partitions = 3,
                        replication_factor = 1,
                        config = {
                            "cleanup.policy":"delete",
                            "compression.type":"lz4"
                        }
                    )
                ]
            )

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"topic {topic_name} has been successfully created")
                except Exception as e:
                    logger.error(f"failed to create topic {topic_name}: {e}")
                    raise
 

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        if self.producer.flush() == 0:
            logger.info("producer closed successfully")
        else:
            logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

def main():
    """Checks for topic and creates the topic if it does not exist"""
    producer = Producer (
        topic_name = "test-auto-topic-creation-1",
        key_schema = None,
        value_schema = None,
        num_partitions=1,
        num_replicas=1
    )

    producer.create_topic()
    producer.close()

if __name__ == "__main__":
    main()