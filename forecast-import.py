#TO DO:
# - Add error handling - there is NONE!
# - Manage gridpoints in a more clever way
# - Perhaps improve file-handling

from kafka import KafkaProducer
import pandas as pd
import datetime as dt
import json
import re                           #Used to identify file-types
import os                           #Used for file-management and environt variables
import time                         #Used for sleep-function only

#Variables exposed as system variables
kafka_topic = os.environ['KAFKA_TOPIC']
if kafka_topic == "": kafka_topic = "weather-forecast-raw"
kafka_host=os.environ['KAFKA_HOST']

#Global variables
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
        print("Checking folder for new files..")
        for root, directories, files in os.walk(forecast_folder):
            for filename in files:
                forecast_file = os.path.join(root, filename)
                print(f"Parsing file '{forecast_file}'.")
                if re.search(forecast_file_filter, forecast_file) is not None:
                    publish_forecast(producer, extract_forecast(forecast_file, field_dict), kafka_topic, coords_dict)
                    os.remove(forecast_file)
        time.sleep(folder_check_wait)