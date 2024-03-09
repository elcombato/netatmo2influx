import os
from datetime import datetime
import logging
import pytz
import lnetatmo
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )

    auth = lnetatmo.ClientAuth()
    weather_data = lnetatmo.WeatherStationData(auth)
    latest_data = weather_data.lastData()

    logging.info("Reading from Netatmo station(s):")
    station_info = [
        f" - {st_name} in {st_data['place']['city']} {st_data['place']['location']}"
        for st_name, st_data in weather_data.stations.items()
    ]
    logging.info("\n".join(station_info))

    influx_url = os.getenv("INFLUX_URL")
    influx_token = os.getenv("INFLUX_TOKEN")
    influx_org = os.getenv("INFLUX_ORG")
    influx_bucket = os.getenv("INFLUX_BUCKET")

    logging.info(
        "Writing to InfluxDB bucket %s in %s (%s):",
        influx_bucket,
        influx_org,
        influx_url,
    )

    tz = pytz.timezone("Europe/Berlin")
    record_list = []
    for room, r_d in latest_data.items():
        timestamp = datetime.fromtimestamp(r_d["When"], tz=tz)
        logging.info(" - %s: %s", str.rjust(room, 12), timestamp)
        for measure, value in r_d.items():
            if measure == "When":
                continue
            elif isinstance(value, str):
                continue
            record_list.append(Point(room).field(measure, float(value)).time(timestamp))

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=influx_bucket, record=record_list)
    client.close()
