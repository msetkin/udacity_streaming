"""Defines core consumer functionality"""
import sys
#sys.path.append('/home/mikhail/udacity-streaming/udacity-streaming')

import logging
from logging import config

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

from producers.models.producer import BROKER_URL



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro = True,
        offset_earliest = False,
        sleep_secs = 1.0,
        consume_timeout = 0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "group.id": "group_1",             
            "auto.offset.reset": "earliest" if self.offset_earliest else "latest",
            "max.poll.interval.ms": 5 * 60 * 1000,
            "session.timeout.ms": int (self.consume_timeout * 60 * 1000)
        }

        

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer (self.broker_properties)
        else:
            self.consumer = Consumer (self.broker_properties)

        self.consumer.subscribe (
            topics = [self.topic_name_pattern],
            on_assign = self.on_assign
        )
 
    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

        consumer.assign(partitions)

        logger.debug("partitions assigned for %s", self.topic_name_pattern)
        

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        
        

        #while True:
        #    message = self.consumer.poll(1.0)
        #    if message is None:
        #        logger.info("no message received by consumer")
        #        return 0
        #    elif message.error() is not None:
        #        logger.info(f"error from consumer {message.error()}")
        #        return -1
        #    else:
        #        logger.info(f"consumed message {message.key()}: {message.value()}")
        #        return 1
        try:
            message = self.consumer.poll(self.consume_timeout)
        except Exception as e:
            logger.error(f"Error while polling: {e}")
        
        if message is None:
            logger.debug(f'No message found for {self.topic_name_pattern}')
            return 0
        elif message.error():
            logger.error(f'Error while consuming message: {message.error()}')
            return 0
        else:
            self.message_handler(message)
            logger.debug(f"consumed message = {message.value()}")
            return 1
        

    def close(self):
        """Cleans up any open kafka consumers"""
        if self.consumer is not None:
            self.consumer.close();


if __name__ == "__main__":
    kafkaconsumer = KafkaConsumer(
        topic_name_pattern = "com.streaming.produce.station",
        offset_earliest = True,
        is_avro = True
    )
    
    kafkaconsumer.consume()
