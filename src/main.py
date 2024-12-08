"""Continously write mesaurements from Netatmo to InfluxDB
"""
import os
from time import sleep
import logging
import netatmo
import influx


# interval between Netatmo queries in _minutes_
READ_INTERVAL = int(os.getenv("READ_INTERVAL"))

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )
    try:
        while True:
            netatmo_station = netatmo.read_station_info()
            if netatmo_station is None:
                sleep(120)
                continue
            influx_data = netatmo.read_data_records(netatmo_station)
            influx.write(influx_data)
            sleep(READ_INTERVAL * 60)
    except KeyboardInterrupt:
        logging.warning("Interrupted")
