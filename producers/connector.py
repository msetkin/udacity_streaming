"""Configures a Kafka Connector for Postgres Station data"""
import sys, os
import json
import logging

import requests


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "com.streaming.pg.cta.stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.info("connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps( 
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "500",
                    "connection.url": "jdbc:postgresql://postgres:5432/cta",
                    "connection.user": os.environ['PG_USER'],
                    "connection.password": os.environ['PG_PASSWORD'],
                    "table.whitelist": "stations",
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "topic.prefix": "com.streaming.connect.",
                    "poll.interval.ms": 10000,
                }
            }
        ),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        logger.error(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")
        return
    
    logging.info("connector created successfully")

def main():
    configure_connector()


if __name__ == "__main__":
    main()
