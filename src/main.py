"""Continously write mesaurements from Netatmo to InfluxDB
"""

import os
from time import sleep
from datetime import datetime
import logging
import pytz
import lnetatmo
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


# interval between Netatmo queries in _minutes_
READ_INTERVAL = int(os.getenv("READ_INTERVAL"))
NETATMO_TYPES = [
    "temperature",  # °C
    "humidity",  # %
    "co2",  # ppm
    "pressure",  # bar
    "noise",  # db
    "rain",  # mm
    "windstrength",  # km/h, °
]
TZ = pytz.timezone("Europe/Berlin")


def connect_to_influxdb():
    influx_url = os.getenv("INFLUX_URL")
    influx_token = os.getenv("INFLUX_TOKEN")
    influx_org = os.getenv("INFLUX_ORG")

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)

    logging.debug("Connected to InfluxDB %s (%s):", influx_url, influx_org)
    return client


def get_latest_timestamp(module_name, mtype):
    influx_bucket = os.getenv("INFLUX_BUCKET")

    client = connect_to_influxdb()
    query_api = client.query_api()

    tables = query_api.query(
        f'from(bucket: "{influx_bucket}")'
        "|> range(start: -1d)"
        f'|> filter(fn: (r) => r["_measurement"] == "{module_name}")'
        f'|> filter(fn: (r) => r["_field"] == "{mtype}")'
        "|> last()"
    )
    client.close()

    return tables[0].records[0].values["_time"]


def get_netatmo_data() -> lnetatmo.WeatherStationData:
    """Read weather data via Netatmo API

    Returns:
        lnetatmo.WeatherStationData: Netatmo Weather Station Data
    """
    auth = lnetatmo.ClientAuth()

    data = lnetatmo.WeatherStationData(auth)

    logging.info("Reading from Netatmo station(s):")
    station_info = [
        f"  {st_name} in {st_data['place']['city']}"
        for st_name, st_data in data.stations.items()
    ]
    logging.info("\n".join(station_info))

    return data


def netatmo2influx_single(weather_data: lnetatmo.WeatherStationData) -> list:
    """Read latest values from Netatmo and parse into InfluxDB format

    Args:
        weather_data (lnetatmo.WeatherStationData): Netatmo Weather Station Data

    Returns:
        list: List of InfluxDB points
    """
    measurement = weather_data.lastData()

    record_list = []
    for room, r_d in measurement.items():
        timestamp = datetime.fromtimestamp(r_d["When"], tz=TZ)
        logging.info("  %s: %s", str.rjust(room, 12), timestamp)
        for measure, value in r_d.items():
            if measure == "When":
                continue
            elif isinstance(value, str):
                continue
            record_list.append(Point(room).field(measure, float(value)).time(timestamp))

    return record_list


def __read_module(
    module: dict, station_id: str, weather_data: lnetatmo.WeatherStationData
) -> list:
    """Read interval for single Netatmo module and parse into InfluxDB format

    Args:
        module (dict): Netatmo module info
        station_id (str): MAC address of Netatmo station
        weather_data (lnetatmo.WeatherStationData): Netatmo Weather Station Data

    Returns:
        list: List of InfluxDB points
    """

    # measurement types of module
    m_types = [
        str.lower(m)
        for m in module["dashboard_data"].keys()
        if str.lower(m) in NETATMO_TYPES
    ]

    start_date = get_latest_timestamp(module["module_name"], m_types[0])

    logging.info(
        "    %s (%s): %s",
        module["module_name"],
        start_date.isoformat(),
        m_types
    )
    # measurements of module
    record_list = []
    for m_type in m_types:
        measure = weather_data.getMeasure(
            device_id=station_id,
            module_id=module["_id"],
            scale="max",
            mtype=m_type,
            date_begin=int((start_date).timestamp()),
            date_end=int(datetime.now().timestamp()),
        )

        if measure is None:
            logging.info("      Measurement is `None` for %s", m_type)
            continue
        if len(measure["body"]) == 0:
            logging.info("      No data for %s", m_type)
            continue

        record_list += [
            Point(module["module_name"])
            .field(m_type, float(val[0]))
            .time(datetime.fromtimestamp(int(ts_epoch), tz=TZ))
            for ts_epoch, val in measure["body"].items()
        ]

    # metadata of module
    for m_type in ["battery_percent", "wifi_status", "rf_status"]:
        if m_type in module:
            ts_epoch = (
                module["last_seen"]
                if "last_seen" in module
                else module["last_status_store"]
            )
            record_list.append(
                Point(module["module_name"])
                .field(m_type, float(module[m_type]))
                .time(datetime.fromtimestamp(int(ts_epoch), tz=TZ))
            )

    return record_list


def netatmo2influx_interval(weather_data: lnetatmo.WeatherStationData) -> list:
    """Read interval of values from Netatmo and parse into InfluxDB format

    Args:
        weather_data (lnetatmo.WeatherStationData): Netatmo Weather Station Data

    Returns:
        list: List of InfluxDB points
    """

    influx_records = []

    for station_id in weather_data.stationIds.keys():
        station = weather_data.stationById(station_id)

        influx_records += __read_module(station, station_id, weather_data)

        for module in station["modules"]:
            influx_records += __read_module(module, station_id, weather_data)

    return influx_records


def write_influxdb(record_list: list):
    """Write to InfluxDB

    Args:
        record_list (list): List of InfluxDB points
    """
    influx_bucket = os.getenv("INFLUX_BUCKET")

    client = connect_to_influxdb()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=influx_bucket, record=record_list)
    client.close()

    logging.info("Writing %s records to InfluxDB", len(record_list))


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )
    try:
        while True:
            netatmo_data = get_netatmo_data()
            influx_data = netatmo2influx_interval(netatmo_data)
            write_influxdb(influx_data)
            sleep(READ_INTERVAL * 60)
    except KeyboardInterrupt:
        logging.warning("Interrupted")
