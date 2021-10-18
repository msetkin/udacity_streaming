"""Contains functionality related to Lines"""
import json
import logging

from models import Station
from ksql import TURNSTILE_SUMMARY_TABLE

import pandas as pd
from webcolors import rgb_to_name


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_color_name (is_red:bool, is_green:bool, is_blue:bool):
    red_int = 255 * int (is_red)
    green_int = 255 * int (is_green)
    blue_int = 255 * int (is_blue)

    return rgb_to_name ((red_int, green_int, blue_int)).replace("lime","green")

def get_line_stations (color):
    df = pd.read_csv("/home/mikhail/udacity-streaming/udacity-streaming/producers/data/cta_stations.csv")
    df["line"] = df.apply(lambda x: get_color_name(x['red'], x['green'], x['blue']), axis=1)
    df1 = df[['station_id','station_name','order','line']].drop_duplicates().copy()
    logger.info (f"loaded_color: {color}")
    return df1[df1['line']==color][['station_id','station_name','order']].to_dict(orient="index")

class Line:
    """Defines the Line Model"""

    def __init__(self, color):
        """Creates a line"""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        
        self.stations = dict([(x['station_id'], Station.from_message(x)) for x in get_line_stations(color).values()])
        
    def _handle_station(self, value):
        """Adds the station to this Line's data model"""
        if value["line"] != self.color:
            logger.error(f"value[line] = {value['line']}, self.color= {self.color}")
            return
        self.stations[value["station_id"]] = Station.from_message(value)

    def _handle_arrival(self, message):
        """Updates train locations"""
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("unable to handle previous station due to missing station")
        else:
            logger.debug(
                "unable to handle previous station due to missing previous info"
            )

        station_id = value.get("station_id")
        station = self.stations.get(station_id)

        if station is None:
            logger.debug("unable to handle message due to missing station")
            return
        station.handle_arrival(
            value.get("direction"), value.get("train_id"), value.get("train_status")
        )

    def process_message(self, message):
        """Given a kafka message, extract data"""
        
        message_topic = message.topic()
        
        if message_topic == "org.chicago.cta.stations.table.v1": # Set the conditional correctly to the stations Faust Table
            try:
                value = json.loads(message.value())
                self._handle_station(value)
            except Exception as e:
                logger.fatal("bad station? %s, %s", value, e)
        
        elif message_topic == "com.streaming.produce.station": # Set the conditional to the arrival topic
            self._handle_arrival(message)
        
        elif message_topic == TURNSTILE_SUMMARY_TABLE: # Set the conditional to the KSQL Turnstile Summary Topic
            
            try:
                json_data = json.loads(message.value())
            except ValueError:  # includes simplejson.decoder.JSONDecodeError
                logger.error(f"Decoding JSON has failed")
                return

            station_id = json_data.get("STATION_ID")
                        
            station = self.stations.get(int(station_id))
            
            if station is None:
                logger.error("unable to handle message due to missing station")
                logger.info(f"{message_topic}, station_id: {station_id}, type_station_id: {type(station_id)}, json_data: {json_data}, station: {station}")
                logger.info("")
                return
            
            station.process_message(json_data)
            return
        
        else:
            logger.debug(
                "unable to find handler for message from topic %s", message.topic
            )


"""2021-10-19 00:07:27,421 models.line  INFO     loaded_color: red
2021-10-19 00:07:27,424 models.line  INFO     self.stations: dict_keys([40900, 41190, 40100, 41300, 40760, 40880, 41380, 40340, 41200, 40770, 40540, 40080, 41420, 41320, 41220, 40650, 40630, 41450, 40330, 41660, 41090, 40560, 41490, 41400, 41000, 40190, 41230, 41170, 40910, 40990, 40240, 41430, 40450]), length: 33

2021-10-19 00:07:27,441 models.line  INFO     loaded_color: green
2021-10-19 00:07:27,444 models.line  INFO     self.stations: dict_keys([40020, 41350, 40610, 41260, 40280, 40700, 40480, 40030, 41670, 41070, 41360, 40170, 41510, 41160, 40380, 40260, 41700, 40680, 41400, 41690, 41120, 40300, 41270, 41080, 40130, 40510, 40940, 40290]), length: 28

2021-10-19 00:07:27,464 models.line  INFO     loaded_color: blue
2021-10-19 00:07:27,467 models.line  INFO     self.stations: dict_keys([40890, 40820, 40230, 40750, 41280, 41330, 40550, 41240, 40060, 41020, 40570, 40670, 40590, 40320, 41410, 40490, 40380, 40370, 40790, 40070, 41340, 40430, 40350, 40470, 40810, 40220, 40250, 40920, 40970, 40010, 40180, 40980, 40390]), length: 33


2021-10-19 00:07:45,436 models.line  INFO     self.stations: dict_keys([40020, 41350, 40610, 41260, 40280, 40700, 40480, 40030, 41670, 41070, 41360, 40170, 
41510, 41160, 40380, 40260, 41700, 40680, 41400, 41690, 41120, 40300, 41270, 41080, 40130, 40510, 40940, 40290, 41351]), 
station_id: 40030, json_data: {'STATION_ID': 40030, 'SUM_NUM_ENTRIES': 40, 'CNT_ENTRIES': 35}
"""