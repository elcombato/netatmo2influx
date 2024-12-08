import lnetatmo
import logging
from datetime import datetime
from influxdb_client import Point
import pytz
from influx import get_latest_timestamp


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


def read_station_info() -> lnetatmo.WeatherStationData:
    """Read weather data via Netatmo API

    Returns:
        lnetatmo.WeatherStationData: Netatmo Weather Station Data
    """
    auth = lnetatmo.ClientAuth()

    try:
        data = lnetatmo.WeatherStationData(auth)
    except TypeError:
        logging.exception("Reading data from Netatmo server failed:")
        return None

    logging.info("Reading from Netatmo station(s):")
    station_info = [
        f"  {st_name} in {st_data['place']['city']}"
        for st_name, st_data in data.stations.items()
    ]
    logging.info("\n".join(station_info))

    return data


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
    if "dashboard_data" not in module:
        logging.error("No data for %s:", station_id)
        logging.error(module)
        return []
    # measurement types of module
    m_types = [
        str.lower(m)
        for m in module["dashboard_data"].keys()
        if str.lower(m) in NETATMO_TYPES
    ]

    start_date = get_latest_timestamp(module["module_name"], m_types[0])
    if start_date is None:
        return []

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


def read_data_records(weather_data: lnetatmo.WeatherStationData) -> list:
    """Read interval of values from Netatmo and parse into InfluxDB format

    Args:
        weather_data (lnetatmo.WeatherStationData): Netatmo Weather Station Data

    Returns:
        list: List of InfluxDB points
    """

    influx_records = []

    for station_id in weather_data.stationIds.keys():
        station = weather_data.getStation(station_id)

        station_data = __read_module(station, station_id, weather_data)
        if len(station_data):
            influx_records += station_data

        for module in station["modules"]:
            module_data = __read_module(module, station_id, weather_data)
            if len(module_data):
                influx_records += module_data

    return influx_records