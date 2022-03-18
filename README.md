# Forecast Parser

A container that parses forecast-files and provides them to a kafka-broker in smaller chunks.

## Description

This repo contains a python-script that will read/parse forecast files provided by DMI (Danmarks Meteorologiske Institut) (including relayed data from ECMWF) and ConWx. The data will be split into smaller chunks and passed to a Kafka-broker. Furthermore there is a kSQL configuration that will split the kafka-stream into several smaller kSQL streams and tables that will be applied if kSQL is found. The script is intended to be run as part of a container/kubernetes, so a Dockerfile is provided as well, as is a set of helm charts with a default configuration.

### Exposed environment variables:

| Name | Default value | Description |
|--|--|--|
|DEBUG|(not set)|Set to 'TRUE' to enable very verbose debugging log|
|KAFKA_TOPIC|weather-forecast-raw|The topic the script will post messages to on the kafka-broker|
|KAFKA_HOST|my-cluster-kafka-brokers|Host-name or IP of the kafka-broker|
|KAFKA_PORT|9092|Port of the kafka-broker (will be ignored if port is included in KAFKA_HOST)|
|KSQL_HOST|kafka-cp-ksql-server|Host-name or IP of the kSQL server|
|KSQL_PORT|8088|Port of the ksql-server (will be ignored if port is included in KSQL_HOST)|

### File handling / Input

Every 5 seconds the '/forecast-files/' folder is scanned for new files. Files that fit the name-filter will be parsed one by one and then deleted (other files will be ignored). The files must fit the agreed structure (examples can be found in the '/tests/valid-testdata/' subfolder) and naming, otherwise it will most likely break execution and not be able to recover (an issue has been rasied for this).

### Kafka messages / Output

Messages are sent to the kafka broker in json format with the following structure:

| Name | type | description |
|--|--|--|
|estimation_time|VARCHAR|Time the estimation was generated|
|estimation_source|VARCHAR|Filename of the source|
|lon_lat_key|VARCHAR|Unique key for each location in style x.xx_y.yy (not used)|
|position_lon|DOUBLE|Longitude position (nearest match in csv gridpoint lookup)|
|position_lat|DOUBLE|Lattitude position (nearest match in csv gridpoint lookup)|
|forecast_type|VARCHAR|Type of the source (usually first part of the filename)|
|forecast_time|ARRAY\<VARCHAR\>|The hour the forecast is for (YYYYMMDDHH)|
|temperature_2m|ARRAY\<DOUBLE\>|Temperature (K) at 2m|
|temperature_100m|ARRAY\<DOUBLE\>|Temperature (K) at 100m|
|wind_speed_10m|ARRAY\<DOUBLE\>|Wind speed (m/s) at 10m|
|wind_direction_10m|ARRAY\<DOUBLE\>|Wind direction (deg) at 10m|
|wind_speed_100m|ARRAY\<DOUBLE\>|Wind speed (m/s) at 100m|
|wind_direction_100m|ARRAY\<DOUBLE\>|Wind direction (deg) at 100m|
|direct_radiation|ARRAY\<DOUBLE\>|Short wave radiation (solar W/m2)|
|global_radiation|ARRAY\<DOUBLE\>|Short wave radiation per hour|
|accumulated_global_radiation|ARRAY\<DOUBLE\>|Accumulated daliy radiation (W/m2)|

The data-arrays are structured in such a way that each row fits a timestamp in the forecast_time array. In kSQL the data is split into three seperate streams and tables where the arrays have been "exploded" so each message in these represents a single hour in time. In the table, the newest estimate will always replace older ones (this has not been documented further here). Depending on what the forecast-file contains, some of the data-arrays may contain zeros or not be included at all in the output.

## Getting Started

The quickest way to have something running is through docker (see the section [Running container](#running-container)).

Feel free to either import the python-file as a lib or run it directly - or use HELM to spin it up as a pod in kubernetes. These methods are not documented and you will need the know-how yourself (the files have been prep'ed to our best ability though).

### Dependencies

To run the script a kafka broker must be available (use the 'KAFKA_HOST' environment variable). Furthermore a kSQL server should be available (use the 'KSQL_HOST' environment variable) - if unavaliable, the application will still run but error-messages will be logged periodically.

#### Python (if not run as part of the container)

The python script can probably run on any python 3.9+ version, but your best option will be to check the Dockerfile and use the same version as the container. Further requirements (python packages) can be found in the app/requirements.txt file.

#### Docker

Built and tested on version 20.10.7.

#### HELM (only relevant if using HELM for deployment)

Built and tested on version 3.7.0.

### Running container

1. Clone the repo to a suitable place
````bash
git clone https://github.com/energinet-singularity/forecast-parser.git
````

2. Build the container and create a volume
````bash
docker build forecast-parser/ -t forecast-parser:latest
docker volume create forecast-files
````

3. Start the container in docker (change hosts to fit your environment - KSQL_HOST is not required as stated above)
````bash
docker run -v forecast-files:/forecast-files -e KAFKA_HOST=127.0.0.1:9092 -e KSQL_HOST=127.0.0.1:8088 -it --rm forecast-parser:latest
````
The container will now be running interactively and you will be able to see the log output. To parse a forecast, it will have to be delivered to the volume somehow. This can be done by another container mapped to the same volume, or manually from another bash-client

Manual file-move to the volume (please verify volume-path is correct before trying this):
````bash
sudo cp forecast-parser/tests/valid-testdata/EnetEcm_2010010100.txt /var/lib/docker/volumes/forecast-files/_data/
````

## Help

* Be aware: There are at least two kafka-python-brokers available - make sure to use the correct one (see app/requirements.txt file).

For anything else, please submit an issue or ask the authors.

## Version History

* 1.1.3:
    * First production-ready version
    <!---* See [commit change]() or See [release history]()--->

Older versions are not included in the README version history. For detauls on them, see the main-branch commit history, but beware: it was the early start and it was part of the learning curve, so it is not pretty. They are kept as to not disturb the integrity of the history.

## License

This project is licensed under the Apache-2.0 License - see the LICENSE.md file for details
