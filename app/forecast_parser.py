import logging
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime as dt, timedelta as td
import json
import re
import os
import time
import requests
from sched import scheduler
from typing import List, Tuple

# Initialize log
log = logging.getLogger(__name__)


def extract_forecast(filepath: str, field_dict: dict) -> pd.DataFrame:
    """Read the forecast file from 'filepath' and, using field_dict, create a pandas DataFrame.

    Parameters
    ----------
    filepath : str
        Full path of the forecast file.
    field_dict : dict
        Dictionary with field-setup.

    Returns
    -------
        pd.DataFrame
            A dataframe containing the forecast
    """
    # First read content of file (unzip if zipped)
    with open(filepath, "rt") as fc:
        content = fc.readlines()

    # Get calculation time and if conwx, the data_index
    filename = os.path.basename(filepath)
    if 'ConWx' in filename:
        calculation_time = content[0][len("#date="):].rstrip()
        data_index = ["ID-A", "ID-B", "POS-A", "POS-B"] + \
                     [(dt.strptime(calculation_time, "%Y%m%d%H") + td(hours=h)).strftime("%Y%m%d%H")
                      for h in range(int(content[1][len("#minlen="):]), int(content[2][len("#maxlen="):])+1)]
    else:
        calculation_time = re.search(r"Iteration = (\d+)", content[0]).group(1)
        data_index = []

    # Go through file and load data
    data_array = []
    df_dict = {}
    parameter = ''
    for line in content[1:]:
        if line[0] == "#":
            if len(data_array) > 0:
                df = pd.DataFrame(data_array, columns=data_index)
                df.index = df["POS-A"].round(2).astype(str) + "_" + df["POS-B"].round(2).astype(str)
                df.drop(columns=(["POS-A", "POS-B"]), inplace=True)
                if "ConWx" in filename:
                    df.drop(columns=["ID-A", "ID-B"], inplace=True)
                    if "temperature" in parameter:
                        df = df + 273.15
                df_dict[parameter] = df
                data_array = []
            if "# Valid times:" in line:
                # load data_index
                data_index = ["POS-A", "POS-B"] + line[len("# Valid times: "):].split()
            elif re.search(r" +(.+[a-z].+)", line):
                parameter = field_dict[re.search(r" +(.+[a-z].+)", line).group(1)]
        else:
            if float(line.split()[4]) != -99:
                data_array.insert(len(data_array), [float(h) for h in line.split()])

    # Concat all measurements into a multi-index dataframe and return it
    df_all = pd.concat(df_dict.values(), keys=df_dict.keys())
    df_all.index.names = ("parameter", "location")
    df_all.reset_index(level='location', inplace=True)
    df_all['estim_time'] = calculation_time
    df_all['estim_file'] = filename
    return df_all.fillna(0)


def publish_forecast(producer: KafkaProducer, topic: str, df: pd.DataFrame, location_lookup: dict):
    """Transform the dataframe (DF) and send it to the Kafka-broker(producer) on the named topic.

    Parameters
    ----------
    producer : KafkaProducer
        The KafkaProducer where the data should be sent.
    topic : str
        Topic where the data should be delivered.
    df : pd.DataFrame
        Dataframe containing the data that needs to be sent to Kafka.
    location_lookup : dict
        Dictionary where locations can be looked up.
    """
    # Load estimation time and filename
    estim_time = df.iloc[0]["estim_time"]
    estim_file = df.iloc[0]["estim_file"]
    estim_type = estim_file.split("_")[0]
    for location in df["location"].unique():
        # Load lon and lat positions
        location_lookup["dist"] = location_lookup['lon'].sub(float(location.split("_")[0])).abs() + \
            location_lookup['lat'].sub(float(location.split("_")[1])).abs()
        pos_lon = location_lookup.loc[location_lookup["dist"].idxmin()]["lon"].item()
        pos_lat = location_lookup.loc[location_lookup["dist"].idxmin()]["lat"].item()
        lon_lat = f"{pos_lon:0.2f}_{pos_lat:0.2f}"

        hide_col = ["location", "estim_time", "estim_file"]
        # Create json-object with forecast times as array
        json_item = {
            "estimation_time":      estim_time,
            "estimation_source":    estim_file,
            "lon_lat_key":          lon_lat,
            "position_lon":         pos_lon,
            "position_lat":         pos_lat,
            "forecast_type":        estim_type,
            "forecast_time":        df[df['location'] == location].drop(columns=hide_col).columns.tolist()
        }

        # Add measurements as new arrays
        for meas_name in df[df['location'] == location].index:
            json_item[meas_name] = df[df['location'] == location].drop(columns=hide_col).loc[meas_name].round(2).tolist()

        # Send data to kafka
        producer.send(topic, json.dumps(json_item))


def setup_ksql(host: str, ksql_config: dict, timer: scheduler = None) -> bool:
    """This function is being deprecated ASAP."""
    if timer is not None:
        timer.enter(3600, 1, setup_ksql, (ksql_config, timer))

    # Verifying connection
    log.info(f"(Re)creating kSQLdb setup on host '{host}'..")

    try:
        response = requests.get(f"http://{host}/info")
        if response.status_code != 200:
            raise Exception('Host responded with error.')
    except Exception as e:
        log.exception(e)
        log.exception(f"Rest API on 'http://{host}/info' did not respond as expected." +
                      " Make sure environment variable 'KSQL_HOST' is correct.")
        return False

    # Drop all used tables and streams (to prepare for rebuild)
    log.info('Host responded - trying to DROP tables and streams to re-create them.')

    try:
        table_drop_string = ' '.join([f"DROP TABLE IF EXISTS {item['NAME']};" for item in ksql_config["config"][::-1]
                                      if item['TYPE'] == 'TABLE'])
        stream_drop_string = ' '.join([f"DROP STREAM IF EXISTS {item['NAME']};" for item in ksql_config["config"][::-1]
                                       if item['TYPE'] == 'STREAM'])

        if len(table_drop_string) > 1:
            response = requests.post(f"http://{host}/ksql", json={"ksql": table_drop_string, "streamsProperties": {}})
            if response.status_code != 200:
                log.debug(response.json())
                raise Exception('Host responded with error when DROPing tables.')
        if len(stream_drop_string) > 1:
            response = requests.post(f"http://{host}/ksql", json={"ksql": stream_drop_string, "streamsProperties": {}})
            if response.status_code != 200:
                log.debug(response.json())
                raise Exception('Host responded with error when DROPing streams.')
    except Exception as e:
        log.exception(e)
        log.exception("Error when trying to drop tables and/or streams.")
        return False

    # Create streams and tables based on config
    log.info("kSQL host accepted config DROP - trying to rebuild config.")

    try:
        for ksql_item in ksql_config['config']:
            response = requests.post(f"http://{host}/ksql", json={"ksql": f"CREATE {ksql_item['TYPE']} " +
                                     f"{ksql_item['NAME']} {ksql_item['CONFIG']};", "streamsProperties": {}})
            if response.status_code != 200:
                log.debug(response.json())
                raise Exception(f"Error when trying to create {ksql_item['TYPE']} '{ksql_item['NAME']}'' - " +
                                f"got status code '{response.status_code}'")
            if response.json().pop()['commandStatus']['status'] != 'SUCCESS':
                log.debug(response.json())
                raise Exception(f"Error when trying to create {ksql_item['TYPE']} '{ksql_item['NAME']}'' - " +
                                f"got reponse '{response.json().pop()['commandStatus']['status']}'")
            log.info(f"kSQL host accepted CREATE {ksql_item['TYPE']} {ksql_item['NAME']} - continuing..")
    except Exception as e:
        log.exception(e)
        log.exception("Error when rebuilding streams and tables.")
        return False

    # All went well!
    log.info(f"kSQLdb setup on host '{host}' was (re)created.")
    return True


def load_config(coords_file: str, ksql_file: str, topic: str) -> Tuple[dict, dict, pd.DataFrame]:
    """Read config from 'coords_file' and 'ksql_file' and then return as tuple 'output_path'.

    Parameters
    ----------
    coords_file : str
        Path of the config-file with coordinates.
    ksql_file : str
        Path of the kSQL config-file.
    topic : str
        Used for eval part. Will be deprecated soon.

    Returns
    -------
        dict
            kSQL mapping.
        dict
            Fields in the kSQL setup.
        pd.DataFrame
            Frame with GPS coordinates.
    """
    # Initialize coordinate and field-name lookup dictionaries
    coords = pd.read_csv(coords_file, index_col=0)
    mapping = json.loads(open(ksql_file).read())

    # Compute/evaluate the configuration
    for config in mapping['config']:
        config['CONFIG'] = eval(config['CONFIG'])

    fields = {text: field['ID'] for field in mapping['fields'] for text in field['Text']}

    # Return values
    return (mapping, fields, coords)


def main_loop(producer: KafkaProducer, topic: str, input_folder: str, file_filter: str, field_dict: dict,
              coords_dict: dict, scan_interval_s: int = 5, timer: scheduler = None):
    # Save input arguments to pass into timer at the end of function
    local_args = tuple(locals().values())

    log.debug(f"Scanning '{input_folder}' folder..")
    for file in [fs_item for fs_item in os.scandir(input_folder) if fs_item.is_file()]:
        if re.search(file_filter, file.name) is not None:
            log.info(f"Parsing file '{file.path}'.")
            publish_forecast(producer, topic, extract_forecast(file.path, field_dict), coords_dict)
            os.remove(file.path)
        else:
            log.warning(f'Unknown file found: {file.path}')

    if timer is not None:
        timer.enter(scan_interval_s, 1, main_loop, local_args)


if __name__ == "__main__":
    # Set up logging
    if os.environ.get('DEBUG', 'FALSE').upper() == 'FALSE':
        # __main__ will output INFO-level, everything else stays at WARNING
        logging.basicConfig(format="%(levelname)s:%(asctime)s:%(name)s - %(message)s")
        logging.getLogger(__name__).setLevel(logging.INFO)
    elif os.environ['DEBUG'].upper() == 'TRUE':
        # Set EVERYTHING to DEBUG level
        logging.basicConfig(format="%(levelname)s:%(asctime)s:%(name)s - %(message)s", level=logging.DEBUG)
        log.debug('Setting all logs to debug-level')
    else:
        raise ValueError(f"'DEBUG' env. variable is '{os.environ['DEBUG']}', but must be either 'TRUE', 'FALSE' or unset.")

    log.info("Initializing forecast-parser..")

    # Set up constants, load data from files and initialize timer
    FILE_FILTER = r"(E[Nn]et(NEA|Ecm)_|ConWx_prog_)\d+(_\d{3})?\.(txt|dat)"
    FOLDER_CHECK_WAIT = 5
    FORECAST_FOLDER = "/forecast-files/"
    TEMPLATE_FOLDER = "/app/"
    kafka_topic = os.environ.get('KAFKA_TOPIC', "weather-forecast-raw")
    mapping, fields, coords = load_config("app/gridpoints.csv", "app/ksql-config.json", kafka_topic)
    timer = scheduler(time.time, time.sleep)

    # Set up Kafka
    if os.environ.get('KAFKA_HOST') is None:
        raise ValueError("KAFKA_HOST is a required variable but it has not been set.")
    else:
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'], value_serializer=lambda x: x.encode('utf-8'))
        except Exception:
            raise ConnectionError(f"Connection to '{os.environ['KAFKA_HOST']}' failed. Check evironment variable "
                                  "'KAFKA_HOST' and reload the script to try again.")
        else:
            log.debug("Kafka connection established.")
            timer.enter(15, 1, main_loop, (producer, kafka_topic, FORECAST_FOLDER, FILE_FILTER,
                        fields, coords, FOLDER_CHECK_WAIT, timer))

    # Set up KSQL
    if os.environ.get('KSQL_HOST') is None:
        log.warning("'KSQL_HOST' parameter not set. Skipping KSQL setup.")
    else:
        log.info(f"Expecting KSQL at: {os.environ['KSQL_HOST']}")
        timer.enter(0, 1, setup_ksql, (os.environ['KSQL_HOST'], mapping, timer))

    # Start the scheduler
    log.info("Initialization done - Starting scheduler..")
    timer.run()
