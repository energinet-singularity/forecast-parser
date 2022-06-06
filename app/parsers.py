import pandas as pd
import re
import logging

log = logging.getLogger()


def get_dmi_helpers():
    fieldmap = {"2m temperatur(K)": "temperature_2m",
                "10m wind speed(m/s)": "wind_speed_10m",
                "10m wind direction(deg)": "wind_direction_10m",
                "100m temperatur(K)": "temperature_100m",
                "100m wind speed(m/s)": "wind_speed_100m",
                "100m wind direction(deg)": "wind_direction_100m",
                "Accumulated short wave radiation (J/m2)": "accumulated_global_radiation",
                "Global radiation (W/m2)": "global_radiation",
                "Short wave radiation aka ny (W/m2)": "global_radiation",
                "Short wave radiation per hour (W/m2)": "global_radiation"}
    return {"columnmap": fieldmap}


def get_conwx_helpers():
    pass


def dmi_parser(file_contents: list[str], helpers: dict) -> pd.DataFrame:
    try:
        DF_INDEX = ['lon', 'lat', 'time']
        DT_FORMAT = "%Y%m%d%H"
        data_array = []
        df_combined: pd.DataFrame = None
        
        for line in file_contents[1:]:
            # First line contains forecast metadata
            # Lines starting with hash contain meta-data about following rows
            if line[0] == "#":
                if len(data_array) > 0:
                    df = pd.DataFrame(data_array).explode(['time', parameter]).set_index(DF_INDEX)
                    if df_combined is None:
                        df_combined = df
                    else:
                        df_combined = df_combined.join(df)
                    data_array.clear()
                    
                if "Valid times:" in line:
                    time_index = line[len("# Valid times: ") :].split()
                else:
                    try:
                        parameter = helpers["columnmap"][re.search(r" +(.+[a-z].+)", line).group(1)]
                    except Exception as e:
                        log.error("Could not translate column name - using 'INVALID_COL_NAME' in stead.")
                        log.exception(e)
                        parameter = "INVALID_COL_NAME"
            else:
                data_array.append({"lon": line.split()[0],
                                   "lat": line.split()[1],
                                   "time": pd.to_datetime(time_index, format=DT_FORMAT, utc=True),
                                   parameter: line.split()[2:]})

        df_combined = df_combined.join(pd.DataFrame(data_array).explode(['time', parameter]).set_index(DF_INDEX))
        timestr = re.search(r"Iteration = (\d+)", file_contents[0]).group(1)
        df_combined['calculation_time'] = pd.to_datetime(timestr, format=DT_FORMAT, utc=True)
        df_combined.fillna(0, inplace=True)

    except Exception as e:
        log.exception(e)
        raise e

    return df_combined

def conwx_parser(file_contents: list[str], helpers: dict) -> pd.DataFrame:
    pass
"""
    # Get calculation time and setup data_index depending on parse_type
    try:
        if parse_type == "DMI":
            calculation_time = re.search(r"Iteration = (\d+)", file_contents[0]).group(
                1
            )
            data_index = []
        if parse_type == "ConWx":
            calculation_time = file_contents[0][len("#date=") :].rstrip()
            data_index = ["ID-A", "ID-B", "POS-A", "POS-B"] + [
                (dt.strptime(calculation_time, "%Y%m%d%H") + td(hours=h)).strftime(
                    "%Y%m%d%H"
                )
                for h in range(
                    int(file_contents[1][len("#minlen=") :]),
                    int(file_contents[2][len("#maxlen=") :]) + 1,
                )
            ]
        else:
            raise ValueError(f"Unknown parse_type specified")

        # Go through file and load data
        data_array = []
        df_dict = {}
        parameter = ""
        for line in file_contents[1:]:
            if line[0] == "#":
                if len(data_array) > 0:
                    df = pd.DataFrame(data_array, columns=data_index)
                    df.index = (
                        df["POS-A"].round(2).astype(str)
                        + "_"
                        + df["POS-B"].round(2).astype(str)
                    )
                    df.drop(columns=(["POS-A", "POS-B"]), inplace=True)
                    if "ConWx" in filename:
                        df.drop(columns=["ID-A", "ID-B"], inplace=True)
                        if "temperature" in parameter:
                            df = df + 273.15
                    df_dict[parameter] = df
                    data_array = []
                if "# Valid times:" in line:
                    # load data_index
                    data_index = ["POS-A", "POS-B"] + line[
                        len("# Valid times: ") :
                    ].split()
                elif re.search(r" +(.+[a-z].+)", line):
                    parameter = field_dict[re.search(r" +(.+[a-z].+)", line).group(1)]
            else:
                if float(line.split()[4]) != -99:
                    data_array.insert(len(data_array), [float(h) for h in line.split()])

        # Concat all measurements into a multi-index dataframe and return it
        df_all = pd.concat(df_dict.values(), keys=df_dict.keys())
        df_all.index.names = ("parameter", "location")
        df_all.reset_index(level="location", inplace=True)
        df_all["estim_time"] = calculation_time
        df_all["estim_file"] = filename
        df_all.fillna(0, inplace=True)

        # Load estimation time and filename
        estim_time = df_all.iloc[0]["estim_time"]
        estim_file = df_all.iloc[0]["estim_file"]
        estim_type = estim_file.split("_")[0]
        for location in df_all["location"].unique():
            # Load lon and lat positions
            location_lookup["dist"] = (
                location_lookup["lon"].sub(float(location.split("_")[0])).abs()
                + location_lookup["lat"].sub(float(location.split("_")[1])).abs()
            )
            pos_lon = location_lookup.loc[location_lookup["dist"].idxmin()][
                "lon"
            ].item()
            pos_lat = location_lookup.loc[location_lookup["dist"].idxmin()][
                "lat"
            ].item()
            lon_lat = f"{pos_lon:0.2f}_{pos_lat:0.2f}"

            hide_col = ["location", "estim_time", "estim_file"]
            # Create json-object with forecast times as array
            json_item = {
                "estimation_time": estim_time,
                "estimation_source": estim_file,
                "lon_lat_key": lon_lat,
                "position_lon": pos_lon,
                "position_lat": pos_lat,
                "forecast_type": estim_type,
                "forecast_time": df_all[df_all["location"] == location]
                .drop(columns=hide_col)
                .columns.tolist(),
            }

            # Add measurements as new arrays
            for meas_name in df_all[df_all["location"] == location].index:
                json_item[meas_name] = (
                    df_all[df_all["location"] == location]
                    .drop(columns=hide_col)
                    .loc[meas_name]
                    .round(2)
                    .tolist()
                )

    except Exception as e:
        log.exception(f"Forecast extraction failed with error '{e}'.")

    return df_all
"""

def load_geographical_coordinates(coordinate_file: str) -> pd.DataFrame:
    """Load geographical coordinates from a .csv file and return it as a pandas DataFrame

    Note: Data in first column must match CONVEX geographical ID.

    Parameters
    ----------
    coordinate_file : str
        Full path of .csv file containing geographical field mapping

    Returns
    -------
    pd.DataFrame
        DataFrame with geographical coordinate sets
    """
    try:
        grid_points = pd.read_csv(coordinate_file, index_col=0)
    except Exception:
        raise IOError(
            f"Could not load file {coordinate_file} into pandas DataFrame."
        ) from None

    return grid_points
