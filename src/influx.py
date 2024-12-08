import os
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import logging


def connect():
    influx_url = os.getenv("INFLUX_URL")
    influx_token = os.getenv("INFLUX_TOKEN")
    influx_org = os.getenv("INFLUX_ORG")

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)

    logging.debug("Connected to InfluxDB %s (%s):", influx_url, influx_org)
    return client


def get_latest_timestamp(module_name, mtype):
    influx_bucket = os.getenv("INFLUX_BUCKET")

    client = connect()
    query_api = client.query_api()

    tables = query_api.query(
        f'from(bucket: "{influx_bucket}")'
        "|> range(start: -30d)"
        f'|> filter(fn: (r) => r["_measurement"] == "{module_name}")'
        f'|> filter(fn: (r) => r["_field"] == "{mtype}")'
        "|> last()"
    )
    client.close()

    if len(tables) == 0:
        return
    return tables[0].records[0].values["_time"]


def write(record_list: list):
    """Write to InfluxDB

    Args:
        record_list (list): List of InfluxDB points
    """
    influx_bucket = os.getenv("INFLUX_BUCKET")

    client = connect()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=influx_bucket, record=record_list)
    client.close()

    logging.info("Writing %s records to InfluxDB", len(record_list))