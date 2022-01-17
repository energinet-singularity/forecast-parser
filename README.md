# Forecast Parser

A container that parses forecast-files and provides them to a kafka-broker in smaller chunks.

## Description

This repo contains a python-script that will read/parse forecast files provided by DMI (Danmarks Meteorologiske Institut) (including relayed data from ECMWF) and ConWx. The data will be split into smaller chunks and passed to a Kafka-broker. Furthermore there is a kSQL configuration that will split the kafka-stream into several smaller kSQL streams and tables that will be applied if kSQL is found. The script is intended to be run as part of a container/kubernetes, so a Dockerfile is provided as well, as is a set of helm charts with a default configuration.

Exposed environment variables:
| Name | Default value | Description |
|--|--|--|
|KAFKA_TOPIC|weather-forecast-raw|The topic the script will post messages to on the kafka-broker|
|KAFKA_HOST|my-cluster-kafka-brokers|Host-name or IP of the kafka-broker|
|KAFKA_PORT|9092|Port of the kafka-broker (will be ignored if port is included in KAFKA_HOST)|
|KSQL_HOST|kafka-cp-ksql-server|Host-name or IP of the kSQL server|
|KSQL_PORT|8088|Port of the ksql-server (will be ignored if port is included in KSQL_HOST)|


## Getting Started

### Dependencies

To run the script a kafka broker must be available (use the 'KAFKA_HOST' environment variable). Furthermore a kSQL server should be available (use the 'KSQL_HOST' environment variable) - if unavaliable, the application will still run but error-messages will be logged periodically.

#### Python (if not run as part of the container)

The python script can probably run on any python 3.9+ version, but your best option will be to check the Dockerfile and use the same version as the container. Further requirements (python packages) can be found in the app/requirements.txt file.

#### Docker

Built and tested on version 20.10.7.

#### HELM (only relevant if using HELM for deployment)

Built and tested on version 3.7.0.

### Executing program

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
