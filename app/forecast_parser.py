from kafka import KafkaProducer
import pandas as pd
import datetime as dt
import json
import sys
import re
import os
import time
import requests

# Variables initialized (some are exposed as system variables)
kafka_topic = os.environ.get('KAFKA_TOPIC', "weather-forecast-raw")
kafka_host = os.environ.get('KAFKA_HOST', "my-cluster-kafka-brokers")
kafka_port = os.environ.get('KAFKA_PORT', "9092")
ksql_host = os.environ.get('KSQL_HOST', "kafka-cp-ksql-server")
ksql_port = os.environ.get('KSQL_PORT', "8088")

# File- and folder variables
forecast_folder = "/forecast-files/"
coords_csv_file = "app/gridpoints.csv"
ksql_config_file = "app/ksql-config.json"
forecast_file_filter = r"(E[Nn]et(NEA|Ecm)_|ConWx_prog_)\d+(_\d{3})?\.(txt|dat)"

# Hardcoded variables
folder_check_wait = 5
ksql_setup_valid = False

# Make sure port-number is part of the hostname
if ':' not in kafka_host and kafka_port != "":
    kafka_host += f":{kafka_port}"
if ':' not in ksql_host and ksql_port != "":
    ksql_host += f":{ksql_port}"

print('Starting filemover script with following settings:')
print(f'- KAFKA_TOPIC: {kafka_topic}')
print(f'- KAFKA_HOST: {kafka_host}')
print(f'- KSQL_HOST: {ksql_host}')
print('')


def extract_forecast(filepath: str, field_dict: dict):
    # First read content of file (unzip if zipped)
    content = open(filepath, "rt").readlines()

    # Get calculation time and if conwx, the data_index
    filename = filepath.split("/")[-1]
    if 'ConWx' in filename:
        calculation_time = content[0][len("#date="):].rstrip()
        data_index = ["ID-A", "ID-B", "POS-A", "POS-B"] + \
                     [(dt.datetime.strptime(calculation_time, "%Y%m%d%H") + dt.timedelta(hours=h)).strftime("%Y%m%d%H")
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
    return df_all


def publish_forecast(producer: KafkaProducer, df: dict, kafka_topic: str, location_lookup: dict):
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
        producer.send(kafka_topic, json.dumps(json_item))


def setup_ksql(ksql_host: str, ksql_config: dict) -> bool:
    # Verifying connection
    print(f"(Re)creating kSQLdb setup on host '{ksql_host}'..")

    try:
        response = requests.get(f"http://{ksql_host}/info")
        if response.status_code != 200:
            raise Exception('Host responded with error.')
    except Exception as e:
        print(e)
        print(f"Rest API on 'http://{ksql_host}/info' did not respond as expected." +
              " Make sure environment variable 'KSQL_HOST' is correct.")
        return False

    # Drop all used tables and streams (to prepare for rebuild)
    print('Host responded - trying to DROP tables and streams to re-create them.')

    try:
        table_drop_string = ' '.join([f"DROP TABLE IF EXISTS {item['NAME']};" for item in ksql_config["config"][::-1]
                                      if item['TYPE'] == 'TABLE'])
        stream_drop_string = ' '.join([f"DROP STREAM IF EXISTS {item['NAME']};" for item in ksql_config["config"][::-1]
                                       if item['TYPE'] == 'STREAM'])

        if len(table_drop_string) > 1:
            response = requests.post(f"http://{ksql_host}/ksql", json={"ksql": table_drop_string, "streamsProperties": {}})
            if response.status_code != 200:
                print(response.json())
                raise Exception('Host responded with error when DROPing tables.')
        if len(stream_drop_string) > 1:
            response = requests.post(f"http://{ksql_host}/ksql", json={"ksql": stream_drop_string, "streamsProperties": {}})
            if response.status_code != 200:
                print(response.json())
                raise Exception('Host responded with error when DROPing streams.')
    except Exception as e:
        print(e)
        print("Error when trying to drop tables and/or streams.")
        return False

    # Create streams and tables based on config
    print("kSQL host accepted config DROP - trying to rebuild config.")

    try:
        for ksql_item in ksql_config['config']:
            response = requests.post(f"http://{ksql_host}/ksql", json={"ksql": f"CREATE {ksql_item['TYPE']} " +
                                     f"{ksql_item['NAME']} {ksql_item['CONFIG']};", "streamsProperties": {}})
            if response.status_code != 200:
                print(response.json())
                raise Exception(f"Error when trying to create {ksql_item['TYPE']} '{ksql_item['NAME']}'' - " +
                                f"got status code '{response.status_code}'")
            if response.json().pop()['commandStatus']['status'] != 'SUCCESS':
                print(response.json())
                raise Exception(f"Error when trying to create {ksql_item['TYPE']} '{ksql_item['NAME']}'' - " +
                                f"got reponse '{response.json().pop()['commandStatus']['status']}'")
            print(f"kSQL host accepted CREATE {ksql_item['TYPE']} {ksql_item['NAME']} - continuing..")
    except Exception as e:
        print(e)
        print("Error when rebuilding streams and tables.")
        return False

    # All went well!
    print(f"kSQLdb setup on host '{ksql_host}' was (re)created.")
    return True


def load_config(coords_csv_file, ksql_config_file):
    # Initialize coordinate and field-name lookup dictionaries
    coords_dict = pd.read_csv(coords_csv_file, index_col=0)
    field_mapping = json.loads(open(ksql_config_file).read())

    # Compute/evaluate the configuration
    for config in field_mapping['config']:
        config['CONFIG'] = eval(config['CONFIG'])

    field_dict = {text: field['ID'] for field in field_mapping['fields'] for text in field['Text']}

    # Return values
    return field_mapping, field_dict, coords_dict


if __name__ == "__main__":
    print("Starting 'main' routine..")

    # Connect producer to Kafka (By keeping it here there is no need to reconnect all the time)
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_host, value_serializer=lambda x: x.encode('utf-8'))
    except Exception:
        print("Connection to kafka failed. Set evironment variable 'KAFKA_HOST' and reload the script to try again.")
        sys.exit(1)

    print("Kafka connection established.")

    # Load the config-files
    field_mapping, field_dict, coords_dict = load_config(coords_csv_file, ksql_config_file)

    print("Primary initialization done - going into loop..")
    print("")

    # Iterate through folder for files
    while True:
        # Check kSQLdb has been set up, otherwise reconfigure it.
        if not ksql_setup_valid:
            if setup_ksql(ksql_host, field_mapping):
                ksql_setup_valid = True

        # Do the main loop / check for files
        print("Checking folder for new files..")
        for root, _, files in os.walk(forecast_folder):
            for filename in files:
                forecast_file = os.path.join(root, filename)
                print(f"Parsing file '{forecast_file}'.")
                if re.search(forecast_file_filter, forecast_file) is not None:
                    publish_forecast(producer, extract_forecast(forecast_file, field_dict), kafka_topic, coords_dict)
                    os.remove(forecast_file)
        time.sleep(folder_check_wait)
