"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



KSQL_URL = "http://localhost:8088"
TURNSTILE_SUMMARY_TABLE = "com_streaming_ksql_turnstile_summary_table"

#
# Next, we will use KSQL to aggregate turnstile data for each of our stations.
# Recall that when we produced turnstile data, we simply emitted an event, not a count. 
# What would make this data more useful would be to summarize it by station so that 
# downstream applications always have an up-to-date count
#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statement, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = f"""
CREATE STREAM com_streaming_ksql_turnstile_stream (
    line VARCHAR,
    station_id INT,
    station_name VARCHAR,
    num_entries INT
) WITH (
    KAFKA_TOPIC = 'com.streaming.produce.turnstile',
    VALUE_FORMAT = 'Avro'
);

CREATE TABLE com_streaming_ksql_turnstile_table (
    line VARCHAR,
    station_id INT,
    station_name VARCHAR,
    num_entries INT
) WITH (
    KAFKA_TOPIC = 'com.streaming.produce.turnstile',
    VALUE_FORMAT = 'Avro',
    KEY = 'station_id'
);

CREATE TABLE {TURNSTILE_SUMMARY_TABLE}
WITH (
    VALUE_FORMAT = 'JSON'
)
AS
    SELECT station_id,
           sum (num_entries) as sum_num_entries,
           count (1) as cnt_entries
      FROM com_streaming_ksql_turnstile_stream
    WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTE)
     GROUP BY station_id;
"""

def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists(TURNSTILE_SUMMARY_TABLE) is True:
        logging.info(f"topic {TURNSTILE_SUMMARY_TABLE} already exists. Terminating KSQL statement...")
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except:
        logger.error(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")
        return
    
    logging.info("KSQL statement executed successfully")


if __name__ == "__main__":
    execute_statement()
