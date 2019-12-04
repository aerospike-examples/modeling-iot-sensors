# Modeling IoT Sensors in Aerospike

This is companion code for my Medium article
[Aerospike Modeling: IoT Sensors](https://medium.com/aerospike-developer-blog/aerospike-modeling-iot-sensors-c74e1411d493).

## Populate Test Data

```
chmod +x run_sensors.sh
./run_sensors.sh
```

## Query the Data

```
python query_iot_data.py --help
usage: query_iot_data.py [--help] [-U <USERNAME>] [-P <PASSWORD>]
                         [-h <ADDRESS>] [-p <PORT>] [-n <NS>] [-s <SET>]
                         [--sensor <SENSOR-ID>] [-i]

optional arguments:
  --help                Displays this message.
  -U <USERNAME>, --username <USERNAME>
                        Username to connect to database.
  -P <PASSWORD>, --password <PASSWORD>
                        Password to connect to database.
  -h <ADDRESS>, --host <ADDRESS>
                        Address of Aerospike server.
  -p <PORT>, --port <PORT>
                        Port of the Aerospike server.
  -n <NS>, --namespace <NS>
                        Port of the Aerospike server.
  -s <SET>, --set <SET>
                        Port of the Aerospike server.
  --sensor <SENSOR-ID>  Sensor ID
  -i, --interactive     Interactive Mode
```
