"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        self.station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        #self.station_id = station.station_id
        #self.line = station.color
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #

        # Complete the code in `producers/models/turnstile.py` so that:
	    #  * A topic is created for each turnstile for each station in Kafka to track the turnstile events
        
        
        super().__init__(
            topic_name = f"com.streaming.produce.turnstile", # TODO: Come up with a better topic name
            key_schema = Turnstile.key_schema,
            value_schema = Turnstile.value_schema,
            num_partitions = 3,
            num_replicas = 1,
        )
        

    def run(self, timestamp, time_step):
        #logger.info(f"com.streaming.turnstile.{self.station.color}.{self.station.station_id}")
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #

        message_key = {
            "timestamp": self.time_millis()
        }
        
        message_value = {
            "line":self.station.color.name,
            "station_id":self.station.station_id,
            "station_name":self.station_name,
            "num_entries":num_entries
        }
        
        self.producer.produce(
            topic=self.topic_name,
            key = message_key,
            value = message_value,
            key_schema = self.key_schema,
            value_schema = self.value_schema
        )
        
        logger.debug(f"object produced to topic \"{self.topic_name}\": {message_key}:{message_value}")

