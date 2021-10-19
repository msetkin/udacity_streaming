import psycopg2
import logging
import os
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sleep_seconds = 180

try:
    conn = psycopg2.connect(f"dbname='cta' user='{os.environ['PG_USER']}' host='localhost' password='{os.environ['PG_PASSWORD']}'")
except:
    logger.error ("I am unable to connect to the database")

cur = conn.cursor()

query = f"""
insert
	into stations 
    (
    	stop_id,
		direction_id,
		stop_name,
		station_name,
		station_descriptive_name,
		station_id,
		\"order\",
		red,
		blue,
		green
	)
select
    (select
            max (stop_id) + 1
        from 
            stations) as stop_id,
    direction_id,
    stop_name,
    station_name,
    station_descriptive_name,
    station_id,
    \"order\",
    red,
    blue,
    green
from
    stations
order by
    random()
limit
    1;"""


try:
    while True:
        logger.debug("PG simulation running")
        
        cur.execute(query)
        conn.commit()

        logger.info("PG station record inserted successfully")

        time.sleep(sleep_seconds)

except KeyboardInterrupt as e:
    cur.close()
    conn.close()
    logger.info("PG simulation shutting down")

