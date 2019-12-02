# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import aerospike
from aerospike import exception as e
try:
    from aerospike_helpers.operations import list_operations as lh
except:
    pass  # Needs Aerospike client >= 3.4.0
from aerospike import predexp as pxp
import datetime
from datetime import timedelta
import pprint
import sys

argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    "--help", dest="help", action="store_true", help="Displays this message."
)
argparser.add_argument(
    "-U",
    "--username",
    dest="username",
    metavar="<USERNAME>",
    help="Username to connect to database.",
)
argparser.add_argument(
    "-P",
    "--password",
    dest="password",
    metavar="<PASSWORD>",
    help="Password to connect to database.",
)
argparser.add_argument(
    "-h",
    "--host",
    dest="host",
    default="127.0.0.1",
    metavar="<ADDRESS>",
    help="Address of Aerospike server.",
)
argparser.add_argument(
    "-p",
    "--port",
    dest="port",
    type=int,
    default=3000,
    metavar="<PORT>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-n",
    "--namespace",
    dest="namespace",
    default="test",
    metavar="<NS>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "-s",
    "--set",
    dest="set",
    default="sensor_data",
    metavar="<SET>",
    help="Port of the Aerospike server.",
)
argparser.add_argument(
    "--sensor",
    dest="sensor_id",
    type=int,
    default=1,
    metavar="<SENSOR-ID>",
    help="Sensor ID",
)
argparser.add_argument(
    "-i",
    "--interactive",
    dest="interactive",
    action="store_true",
    help="Interactive Mode",
)
options = argparser.parse_args()
if options.help:
    argparser.print_help()
    print()
    sys.exit(1)


def version_tuple(version):
    return tuple(int(i) for i in version.split("."))


def pause():
    input("Hit return to continue")


def print_sensor_data(rec, pp):
    k, _, b = rec
    print(k[2])
    print(b['t'])
    print("=" * 30)


if options.namespace and options.namespace != "None":
    namespace = options.namespace
else:
    namespace = None
set = options.set if options.set and options.set != "None" else None

config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
    policy = {"key": aerospike.POLICY_KEY_SEND}
except e.ClientError as e:
    if not options.quiet:
        print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

version = client.info_all("version")
release = list(version.values())[0][1].split(" ")[-1]
if version_tuple(aerospike.__version__) < version_tuple("3.4.0") or version_tuple(
    release
) < version_tuple("4.0"):
    print(
        "\nPlease use Python client >= 3.4.0, ",
        "Aerospike database >= 4.0 for this example.",
    )
    sys.exit(3)

pp = pprint.PrettyPrinter(indent=2)
sensor_id = options.sensor_id
spacer = "=" * 30
minute = 0
key = (namespace, set, "sensor{}-12-31".format(sensor_id))
print("\nRetrieve sensor{} data for 8-11am, December 31st".format(sensor_id))
if options.interactive:
    pause()
starts = 8 * 60
ends = 11 * 60
try:
    ops = [
        lh.list_get_by_value_range(
            "t",
            aerospike.LIST_RETURN_VALUE,
            [starts, aerospike.null()],
            [ends, aerospike.null()],
        )
    ]
    _, _, b = client.operate(key, ops)
    pp.pprint(b["t"])
    print(spacer)

    print("\nGet sensor{} data for April 2nd".format(sensor_id))
    key = (namespace, set, "sensor{}-04-02".format(sensor_id))
    if options.interactive:
        pause()
    _, _, b = client.get(key)
    pp.pprint(b["t"])
    print(spacer)

    print("\nGet a year's data for sensor{}".format(sensor_id))
    if options.interactive:
        pause()
    dt = datetime.datetime(2018, 1, 1, 0, 0, 0)
    keys = []
    for i in range(1, 366):
        keys.append((namespace, set,"sensor{}-{:02d}-{:02d}".format(sensor_id, dt.month, dt.day)))
        dt = dt + timedelta(days=1)
    sensor_year = client.get_many(keys)
    for rec in sensor_year:
        k, _, b =  rec
        print(k[2])
        pp.pprint(b["t"])
        print(spacer)

    print("\nGet the data from all sensors for June 19")
    if options.interactive:
        pause()
    dt = datetime.datetime(2018, 1, 1, 0, 0, 0)
    keys = []
    for i in range(1, 1001):
        keys.append((namespace, set,"sensor{}-06-19".format(i)))
    one_day_all_sensors = client.get_many(keys)
    for rec in one_day_all_sensors:
        k, _, b =  rec
        print(k[2])
        pp.pprint(b["t"])
        print(spacer)

    print("\nScan for a random sampling (about 0.25%) of all the sensor data")
    if options.interactive:
        pause()
    predexp =  [
        pxp.rec_digest_modulo(365),
        pxp.integer_value(1),
        pxp.integer_equal()
    ]
    query = client.query(namespace, set)
    query.predexp(predexp)
    query.foreach(print_sensor_data)

except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
client.close()
