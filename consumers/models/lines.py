"""Contains functionality related to Lines"""
import json
import logging

from models import Line
from ksql import TURNSTILE_SUMMARY_TABLE

logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""

        if "com.streaming.produce.station" in message.topic() or "org.chicago.cta.stations.table.v1" in message.topic():
            value = message.value()
            
            if message.topic() == "org.chicago.cta.stations.table.v1":
                value = json.loads(value)
            
            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        
        elif message.topic() == TURNSTILE_SUMMARY_TABLE:
            logger.debug(f"message.topic {message.topic()}, message.value {message.value()}")
            try:
                json_data = json.loads(message.value())
            except ValueError:  # includes simplejson.decoder.JSONDecodeError
                logger.error(f"Decoding JSON has failed")
            
            line_color = json_data.get("LINE_COLOR")[0]
            logger.debug(f"line_color: {line_color}")

            if line_color == "red":
                self.red_line.process_message(message)
            elif line_color == "green":
                self.green_line.process_message(message)
            elif line_color == "blue":
                self.blue_line.process_message(message)
            else:
                logger.error(f"unknown color: {line_color}")
        
        else:
            logger.info("ignoring non-lines message %s", message.topic())

