"""Defines trends calculations for stations"""
from dataclasses import asdict, dataclass
import faust
import logging
from webcolors import rgb_to_name


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.

app = faust.App(
    "stations-stream",
    broker="kafka://localhost:9092",
    store="memory://"
    )

topic = app.topic(
    "com.streaming.connect.stations", 
    value_type = Station,
    )

changelog_topic = app.topic(
    "com.streaming.faust.stations.changelog",
    partitions = 1
    )

out_topic = app.topic(
    "org.chicago.cta.stations.table.v1",
    partitions = 1,
    key_type = str,
    value_type = TransformedStation,
    )

# TODO: Define a Faust Table
table = app.Table(
    name = "com.streaming.faust.stations.transformed",
    default = TransformedStation,
    partitions = 1,
    changelog_topic = changelog_topic
)

def get_color_name (is_red:bool, is_green:bool, is_blue:bool):
    red_int = 255 * int (is_red)
    green_int = 255 * int (is_green)
    blue_int = 255 * int (is_blue)

    return rgb_to_name ((red_int, green_int, blue_int)).replace("lime","green")


@app.agent(topic)
async def process_station (stations):
    async for station in stations:
        processed_station = TransformedStation (
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = get_color_name (station.red, station.green, station.blue)
        )
        table[processed_station.station_id] = processed_station
        await out_topic.send(key = str(processed_station.station_id), value = processed_station)
        print (processed_station.asdict())



if __name__ == "__main__":
    app.main()
