from datetime import datetime
from influxdb import InfluxDBClient
from influx_file_uploader.settings.service_settings import INFLUXDB_HOST


def format_time(entry):
    dt_obj = datetime.strptime(f'{entry["Date"]} {entry["Time"]}',
                               '%Y-%m-%d %H:%M:%S.%f')
    millisec = dt_obj.timestamp() * 1000

    return int(millisec)


def compose_data(measurement, tags, value, time):
    data = "{0!s},{1!s} {2!s} {3!s}".format(measurement, tags, value, time)
    return data


def format_tags(msg):
    tag = ["client=firstbeach"]

    if len(msg['device_name']) != 0:
        tag.append(f"device_name=\"{msg['device_name']}\"")

    if len(msg['road_name']) != 0:
        tag.append(f"road_name=\"{msg['road_name']}\"")

    return ','.join(tag)


def format_value(entry):
    field_pairs = []
    for key, value in entry.items():
        if key not in ['Time', 'Date', 'device_name', 'road_name']:
            field_pairs.append("{0!s}={1!s}".format(key, value))
    return ','.join(field_pairs)


def format_measurement_name():
    # name = []
    # for arg in args:
    #     if arg in entry:
    #         if entry[arg] != '':
    #             name.append(entry[arg])
    # return '_'.join(name)
    return 'statistics'


def encode(msg):
    measurements = []

    for entry in msg:
        try:
            measurement = format_measurement_name()
            tags = format_tags(entry)
            value = format_value(entry)
            time = format_time(entry)
            measurements.append(compose_data(measurement, tags, value, time))
        except Exception as e:
            print("Error in input data: %s. Skipping.", e)
            continue
    return measurements


def write(msg):
    client = InfluxDBClient(host=INFLUXDB_HOST, port=8086, username='admin', password='password', ssl=False, verify_ssl=False)
    client.write_points(points=msg, database='firstbeach', protocol='line', time_precision='ms')


def converter(full_data):
    write(encode(full_data))
