from kafka import KafkaProducer
import pandas as pd
import datetime as dt
import json
import re                           #Used to identify file-types
import os                           #Used for file-management and environt variables
import time                         #Used for sleep-function only
import requests                     #Used to manage ksql setup

#Variables initialized (some are exposed as system variables)
kafka_topic = os.environ.get('KAFKA_TOPIC', "weather-forecast-raw")
kafka_host = os.environ.get('KAFKA_HOST', "my-cluster-kafka-brokers")
ksql_host = os.environ.get('KSQL_HOST', "kafka-cp-ksql-server")
ksql_setup_valid = False
forecast_folder = "forecast-files/"
coords_csv_file = "gridpoints.csv"
forecast_file_filter = r"(E[Nn]et(NEA|Ecm)_|ConWx_prog_)\d+(_\d{3})?\.(txt|dat)"
folder_check_wait = 5

def extract_forecast(filepath: str, field_dict: dict):
    #First read content of file (unzip if zipped)
    content = open(filepath, "rt").readlines()

    #Get calculation time and if conwx, the data_index
    filename = filepath.split("/")[-1]
    if 'ConWx' in filename:
        calculation_time = content[0][len("#date="):].rstrip()
        data_index = ["ID-A","ID-B","POS-A","POS-B"]+[(dt.datetime.strptime(calculation_time, "%Y%m%d%H") + dt.timedelta(hours=h)).strftime("%Y%m%d%H") for h in range(int(content[1][len("#minlen=") :]), int(content[2][len("#maxlen=") :])+1)]
    else:
        calculation_time = re.search(r"Iteration = (\d+)", content[0]).group(1)
        data_index = []

    #Go through file and load data
    data_array = []
    df_dict = {}
    for line in content[1:]:
        if line[0] == "#":
            if len(data_array) > 0:
                df = pd.DataFrame(data_array, columns=data_index)
                df.index = df["POS-A"].round(2).astype(str) + "_" + df["POS-B"].round(2).astype(str)
                df.drop(columns=(["POS-A","POS-B"]), inplace=True)
                if "ConWx" in filename:
                    df.drop(columns=["ID-A", "ID-B"], inplace=True)
                    if "temperature" in parameter: df = df + 273.15
                df_dict[parameter] = df
                data_array = []
            if "# Valid times:" in line:
                #load data_index
                data_index = ["POS-A","POS-B"]+line[len("# Valid times: ") :].split()
            elif re.search(r" +(.+[a-z].+)", line):
                parameter = field_dict[re.search(r" +(.+[a-z].+)", line).group(1)]
        else:
            if float(line.split()[4]) != -99:
                data_array.insert(len(data_array), [float(h) for h in line.split()])

    #Concat all measurements into a multi-index dataframe and return it
    df_all = pd.concat(df_dict.values(), keys=df_dict.keys())
    df_all.index.names = ("parameter", "location")
    df_all.reset_index(level='location', inplace=True)
    df_all['estim_time'] = calculation_time
    df_all['estim_file'] = filename
    return df_all

def publish_forecast(producer: KafkaProducer, df: dict, kafka_topic: str, location_lookup: dict):
    #Load estimation time and filename
    estim_time = df.iloc[0]["estim_time"]
    estim_file = df.iloc[0]["estim_file"]
    estim_type = estim_file.split("_")[0]
    for location in df["location"].unique():
        #Load lon and lat positions
        location_lookup["dist"] = location_lookup['lon'].sub(float(location.split("_")[0])).abs() + location_lookup['lat'].sub(float(location.split("_")[1])).abs()
        pos_lon = location_lookup.loc[location_lookup["dist"].idxmin()]["lon"].item()
        pos_lat = location_lookup.loc[location_lookup["dist"].idxmin()]["lat"].item()
        lon_lat = f"{pos_lon:0.2f}_{pos_lat:0.2f}"

        #Create json-object with forecast times as array
        json_item = {
            "estimation_time":      estim_time,
            "estimation_source":    estim_file,
            "lon_lat_key":          lon_lat,
            "position_lon":         pos_lon,
            "position_lat":         pos_lat,
            "forecast_type":        estim_type,
            "forecast_time":        df[df['location'] == location].drop(columns=["location","estim_time","estim_file"]).columns.tolist()
        }

        #Add measurements as new arrays
        for meas_name in df[df['location'] == location].index:
            json_item[meas_name] = df[df['location'] == location].drop(columns=["location","estim_time","estim_file"]).loc[meas_name].round(2).tolist()

        #Send data to kafka
        producer.send(kafka_topic, json.dumps(json_item))

def setup_ksql(kafka_topic: str, ksql_host: str):
    #Setup of ksql streams and tables
    ksql_setup=[
        {'TYPE': 'STREAM', 'NAME': 'STREAM_FORECAST_RAW', 'CONFIG': f"CREATE stream STREAM_FORECAST_RAW (forecast_type VARCHAR,estimation_time VARCHAR,estimation_source VARCHAR,position_lon DOUBLE,position_lat DOUBLE,forecast_time ARRAY<VARCHAR>,temperature_2m ARRAY<DOUBLE>,temperature_100m ARRAY<DOUBLE>,wind_speed_10m ARRAY<DOUBLE>,wind_direction_10m ARRAY<DOUBLE>,wind_speed_100m ARRAY<DOUBLE>,wind_direction_100m ARRAY<DOUBLE>,direct_radiation ARRAY<DOUBLE>,global_radiation ARRAY<DOUBLE>,accumulated_global_radiation ARRAY<DOUBLE>) WITH (kafka_topic={kafka_topic},value_format = 'json');"},
        {'TYPE': 'STREAM', 'NAME': 'STREAM_FORECAST_CONWX', 'CONFIG': "CREATE STREAM STREAM_FORECAST_CONWX AS SELECT estimation_time,position_lon,position_lat,EXPLODE(temperature_2m) AS temperature_2m,EXPLODE(temperature_100m) AS temperature_100m,EXPLODE(wind_speed_10m) AS wind_speed_10m,EXPLODE(wind_direction_10m) AS wind_direction_10m,EXPLODE(wind_speed_100m) AS wind_speed_100m,EXPLODE(wind_direction_100m) AS wind_direction_100m,EXPLODE(direct_radiation) AS direct_radiation,EXPLODE(global_radiation) AS global_radiation,EXPLODE(accumulated_global_radiation) AS accumulated_global_radiation,EXPLODE(forecast_time) AS forecast_time FROM STREAM_FORECAST_RAW WHERE forecast_type = 'ConWx';"},
        {'TYPE': 'TABLE', 'NAME': 'FORECAST_CONWX', 'CONFIG': "CREATE TABLE FORECAST_CONWX WITH (KEY_FORMAT='JSON') AS SELECT forecast_time,position_lon,position_lat,LATEST_BY_OFFSET(temperature_2m) AS temperature_2m,LATEST_BY_OFFSET(temperature_100m) AS temperature_100m,LATEST_BY_OFFSET(wind_speed_10m) AS wind_speed_10m,LATEST_BY_OFFSET(wind_speed_100m) AS wind_speed_100m,LATEST_BY_OFFSET(wind_direction_10m) AS wind_direction_10m,LATEST_BY_OFFSET(wind_direction_100m) AS wind_direction_100m,LATEST_BY_OFFSET(direct_radiation) AS direct_radiation,LATEST_BY_OFFSET(global_radiation) AS global_radiation,LATEST_BY_OFFSET(accumulated_global_radiation) AS accumulated_global_radiation FROM STREAM_FORECAST_CONWX GROUP BY forecast_time,position_lon,position_lat EMIT CHANGES;"},
        {'TYPE': 'STREAM', 'NAME': 'STREAM_FORECAST_DMI', 'CONFIG': "CREATE STREAM STREAM_FORECAST_DMI AS SELECT estimation_time,position_lon,position_lat,EXPLODE(temperature_2m) AS temperature_2m,EXPLODE(temperature_100m) AS temperature_100m,EXPLODE(wind_speed_10m) AS wind_speed_10m,EXPLODE(wind_direction_10m) AS wind_direction_10m,EXPLODE(wind_speed_100m) AS wind_speed_100m,EXPLODE(wind_direction_100m) AS wind_direction_100m,EXPLODE(direct_radiation) AS direct_radiation,EXPLODE(global_radiation) AS global_radiation,EXPLODE(accumulated_global_radiation) AS accumulated_global_radiation,EXPLODE(forecast_time) AS forecast_time FROM STREAM_FORECAST_RAW WHERE forecast_type != 'ConWx';"},
        {'TYPE': 'TABLE', 'NAME': 'FORECAST_DMI', 'CONFIG': "CREATE TABLE FORECAST_DMI WITH (KEY_FORMAT='JSON') AS SELECT forecast_time,position_lon,position_lat,LATEST_BY_OFFSET(temperature_2m) AS temperature_2m,LATEST_BY_OFFSET(temperature_100m) AS temperature_100m,LATEST_BY_OFFSET(wind_speed_10m) AS wind_speed_10m,LATEST_BY_OFFSET(wind_speed_100m) AS wind_speed_100m,LATEST_BY_OFFSET(wind_direction_10m) AS wind_direction_10m,LATEST_BY_OFFSET(wind_direction_100m) AS wind_direction_100m,LATEST_BY_OFFSET(direct_radiation) AS direct_radiation,LATEST_BY_OFFSET(global_radiation) AS global_radiation,LATEST_BY_OFFSET(accumulated_global_radiation) AS accumulated_global_radiation FROM STREAM_FORECAST_DMI GROUP BY forecast_time,position_lon,position_lat EMIT CHANGES;"}
    ]

    #Verifying connection
    print(f"Validating kSQLdb setup on host '{ksql_host}'..")
    
    if requests.get(f"http://{ksql_host}:8088/info").status_code == 200:
        print('Host responded in an orderly fashion..')
    else:
        print(f"Rest API on 'http://{ksql_host}:8088/info' did not respond as expected.")
        print('Resuming normal operation - will check again later..')
        return False

    #Verifying streams are set up correctly
    response = requests.post(f"http://{ksql_host}:8088/ksql",json={"ksql": "LIST STREAMS;", "streamsProperties": {}})
    if response.status_code == 200:
        stream_dict = [stream['name'] for stream in response.json().pop()['streams']]
        for ksql_config in ksql_setup:
            if ksql_config['TYPE'] == 'STREAM':
                if ksql_config['NAME'] in stream_dict:
                    print(f'Stream \'{ksql_config["NAME"]}\' found.')
                else:
                    response = requests.post(f"http://{ksql_host}:8088/ksql",json={"ksql": ksql_config['CONFIG'], "streamsProperties": {}})
                    if response.json().pop()['commandStatus']['status'] == 'SUCCESS':
                        print(f'Stream \'{ksql_config["NAME"]}\' created.')
                    else:
                        print(f'Problem while trying to create stream \'{ksql_config["NAME"]}\' - will retry again later..')
                        return False
    else:
        print('Could not list streams - will check again later..')    
        return False
    
    #Verifying tables are set up correctly
    response = requests.post(f"http://{ksql_host}:8088/ksql",json={"ksql": "LIST TABLES;", "streamsProperties": {}})
    if response.status_code == 200:
        table_dict = [table['name'] for table in response.json().pop()['tables']]
        for ksql_config in ksql_setup:
            if ksql_config['TYPE'] == 'TABLE':
                if ksql_config['NAME'] in table_dict:
                    print(f'Table \'{ksql_config["NAME"]}\' found.')
                else:
                    response = requests.post(f"http://{ksql_host}:8088/ksql",json={"ksql": ksql_config['CONFIG'], "streamsProperties": {}})
                    if response.json().pop()['commandStatus']['status'] == 'SUCCESS':
                        print(f'Table \'{ksql_config["NAME"]}\' created.')
                    else:
                        print(f'Problem while trying to create table \'{ksql_config["NAME"]}\' - will retry again later..')
                        return False
    else:
        print('Could not list tables - will check again later..')    
        return False
    
    print('kSQL setup has been validated.')
    return True

if __name__ == "__main__":
    print("Starting 'main' routine..")

    #Connect producer to Kafka (By keeping it here there is no need to reconnect all the time)
    producer = KafkaProducer(bootstrap_servers=kafka_host, value_serializer=lambda x: x.encode('utf-8'))
    print("Kafka connection established.")

    #Initialize coordinate lookup dictionary
    coords_dict = pd.read_csv(coords_csv_file, index_col=0)
    print("Coordinate dictionary created.")

    #Initialize field-name lookup dictionary
    field_dict = {
        "2m temperatur(K)":                         "temperature_2m",
        "100m temperatur(K)":                       "temperature_100m",
        "10m wind speed(m/s)":                      "wind_speed_10m",
        "10m wind direction(deg)":                  "wind_direction_10m",
        "100m wind speed(m/s)":                     "wind_speed_100m",
        "100m wind direction(deg)":                 "wind_direction_100m",
        "Short wave radiation aka ny (W/m2)":       "direct_radiation",
        "Short wave radiation per hour (W/m2)":     "global_radiation",
        "Global radiation (W/m2)":                  "global_radiation",
        "Accumulated short wave radiation (J/m2)":  "accumulated_global_radiation",
        "2 m temperature":                          "temperature_2m",
        "100 m temperature":                        "temperature_100m",
        "10 m wind speed":                          "wind_speed_10m",
        "10 m wind direction":                      "wind_direction_10m",
        "100 m wind speed":                         "wind_speed_100m",
        "100 m wind direction":                     "wind_direction_100m",
        "global radiation":                         "global_radiation"
    }
    print("Field dictionary loaded.")

    #Iterate through folder for files
    while True:
        #Check kSQLdb has been set up, otherwise reconfigure it.
        if not ksql_setup_valid:
            if setup_ksql(kafka_topic, ksql_host): ksql_setup_valid = True
        
        #Do the main loop / check for files
        print("Checking folder for new files..")
        for root, directories, files in os.walk(forecast_folder):
            for filename in files:
                forecast_file = os.path.join(root, filename)
                print(f"Parsing file '{forecast_file}'.")
                if re.search(forecast_file_filter, forecast_file) is not None:
                    publish_forecast(producer, extract_forecast(forecast_file, field_dict), kafka_topic, coords_dict)
                    os.remove(forecast_file)
        time.sleep(folder_check_wait)