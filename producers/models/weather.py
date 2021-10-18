"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random

from confluent_kafka import avro

import urllib.parse

import requests

from producers.models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation",
        start=0
    )    

    REST_PROXY_URL = "http://localhost:8082"

    with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
        key_schema = json.dumps(json.load(f))

    with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
        value_schema = json.dumps(json.load(f))

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        super().__init__(
            topic_name = "com.streaming.restproxy.weather",
            key_schema = Weather.key_schema,
            value_schema = Weather.value_schema,
            num_partitions = 3,
            num_replicas = 1,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += round (min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0),2)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}                                    
        data = {
            'key_schema': Weather.key_schema,
            'value_schema': Weather.value_schema,
            'records': [
                {
                    'key': {
                        'timestamp': self.time_millis()
                    },
                    'value': {
                        'temperature': {
                            'float': self.temp
                        },
                        'status': {
                            'string': self.status.name
                        }
                    }
                }
            ]
        }
        
        resp = requests.post(
            f"{Weather.REST_PROXY_URL}/topics/{self.topic_name}",  
            data = json.dumps(data),
            headers = headers
        )

        try:
            resp.raise_for_status()
        except:
            logger.error(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

        logger.debug(
            "sent weather data to kafka, records: %s",
            str(data)

        )