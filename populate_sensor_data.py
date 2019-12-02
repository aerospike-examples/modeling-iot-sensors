# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import aerospike
from aerospike import exception as e

try:
    from aerospike_helpers.operations import list_operations as lh
except:
    pass  # Needs Aerospike client >= 3.4.0
import sys

argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument("data_file", nargs="?", default=None, help="Path to data file")
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
    "-i",
    "--sensor",
    dest="sensor_id",
    type=int,
    default=1,
    metavar="<SENSOR-ID>",
    help="Sensor ID",
)
argparser.add_argument(
    "-q", "--quiet", dest="quiet", action="store_true", help="Quiet Mode"
)
options = argparser.parse_args()
if options.help or not options.data_file:
    argparser.print_help()
    print()
    sys.exit(1)


def version_tuple(version):
    return tuple(int(i) for i in version.split("."))


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
if (version_tuple(aerospike.__version__) < version_tuple("3.4.0")
    or version_tuple(release) < version_tuple("4.0")):
    if not options.quiet:
        print(
            "\nPlease use Python client >= 3.4.0, ",
            "Aerospike database >= 4.0 for this example.",
        )
    sys.exit(3)

sensor_id = options.sensor_id
spacer = "=" * 30
minute = 0
f = open(options.data_file, "r")
for line in f:
    try:
        try:
            _, h, t = line.split(",")
            try:
                prev_hour
            except:
                prev_temp = int(float(t.strip()[1:-1]) * 10)
                prev_day = h[1:6]
                prev_hour = int(h[7:9])
                continue
            temp = int(float(t.strip()[1:-1]) * 10)
            day = h[1:6]
            hour = int(h[7:9])
            readings = []
            step = (temp - prev_temp) / 60.0
            for i in range(0, 60):
                prev_temp = prev_temp + step
                readings.append([minute, int(prev_temp)])
                minute = minute + 1
            key = (namespace, set, "sensor{}-{}".format(sensor_id, prev_day))
            if not options.quiet:
                print(spacer)
                print("Day {0} hour {1}".format(prev_day, prev_hour))
                print(readings)
            client.operate(key, [lh.list_append_items("t", readings)], policy=policy)
            if day != prev_day:
                minute = 0
            prev_temp = temp
            prev_day = day
            prev_hour = hour
        except ValueError as e:
            if not options.quiet:
                print(e)
            pass
    except IndexError:
        pass
f.close()
if not options.quiet:
    print(spacer)
    print("Sensor {} data for December 31".format(sensor_id))
    k, m, b = client.get((namespace, set, "sensor{}-12-31".format(sensor_id)))
    print(b)
    print("Above is Sensor {} data for December 31".format(sensor_id))
    print(spacer)
client.close()
