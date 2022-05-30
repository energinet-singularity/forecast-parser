import configuration
import pandas as pd
from datetime import datetime as dt, timedelta as td
import json
import re
import os
import time
from sched import scheduler
from singupy.api import DataFrameAPI as singuapi


def clean_stale_data(df: pd.DataFrame) -> pd.DataFrame:
    return df


def update_forecast(base_df: pd.DataFrame, new_forecast: pd.DataFrame) -> pd.DataFrame:
    return base_df


def extract_forecast(
    filepath: str, field_dict: dict, location_lookup: dict
) -> pd.DataFrame:
    """Read the forecast file from 'filepath' and, using field_dict, create a pandas DataFrame.

    Parameters
    ----------
    filepath : str
        Full path of the forecast file.
    field_dict : dict
        Dictionary with field-setup.
    location_lookup: dict
        Dictionary with geo-coords

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the forecast
    """
    # First read content of file (unzip if zipped)
    with open(filepath, "rt") as fc:
        content = fc.readlines()

    # Get calculation time and if conwx, the data_index
    filename = os.path.basename(filepath)
    if "ConWx" in filename:
        calculation_time = content[0][len("#date=") :].rstrip()
        data_index = ["ID-A", "ID-B", "POS-A", "POS-B"] + [
            (dt.strptime(calculation_time, "%Y%m%d%H") + td(hours=h)).strftime(
                "%Y%m%d%H"
            )
            for h in range(
                int(content[1][len("#minlen=") :]),
                int(content[2][len("#maxlen=") :]) + 1,
            )
        ]
    else:
        calculation_time = re.search(r"Iteration = (\d+)", content[0]).group(1)
        data_index = []

    # Go through file and load data
    data_array = []
    df_dict = {}
    parameter = ""
    for line in content[1:]:
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
                data_index = ["POS-A", "POS-B"] + line[len("# Valid times: ") :].split()
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
        pos_lon = location_lookup.loc[location_lookup["dist"].idxmin()]["lon"].item()
        pos_lat = location_lookup.loc[location_lookup["dist"].idxmin()]["lat"].item()
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

    return df_all


def load_field_mapping(field_mapping_file: str) -> dict[str, list[str]]:
    """Load field mapping from a .json file and return it as a dictionary

    Parameters
    ----------
    field_mapping_file : str
        Full path of .json file containing field mapping

    Returns
    -------
    dict[str, list[str]]
        A dict where key is field id and value is forecast file names

    Raises
    ------
    FileNotFoundError
        If specified file was not found or could not be parsed into json
    """
    try:
        with open(field_mapping_file) as file:
            field_mapping = json.loads(file.read())
    except Exception:
        raise IOError(f"Could not load file {field_mapping_file} as json.") from None

    return field_mapping


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


def generate_dummy_input(template_path: str, output_path: str, timer: scheduler = None):
    """Take templates from 'template_path', update timestamps and output them to 'output_path'.

    Parameters
    ----------
    template_path : str
        The path where the templates can be found.
    output_path : str
        The path where the mock data should be written to.
    timer : scheduler
        (Optional) If specified, a new run will be scheduled for now + ~1h
    """

    log.info("Running dummy-input generation")
    if timer is not None:
        next_run = (dt.now()).replace(microsecond=0, second=0, minute=30) + td(hours=1)
        timer.enterabs(
            next_run.timestamp(),
            1,
            generate_dummy_input,
            (template_path, output_path, timer),
        )

    # Make template-file config
    CONFIG_DATA = {
        r"^ENetNEA_\d{10}\.txt$": {
            "filename": "ENetNEA_<timestamp>.txt",
            "delay_h": 3,
            "timelist": [1, 4, 7, 10, 13, 16, 19, 22],
        },
        r"^EnetEcm_\d{10}\.txt$": {
            "filename": "EnetEcm_<timestamp>.txt",
            "delay_h": 7,
            "timelist": [8, 20],
        },
        r"^ConWx_prog_\d{10}_048\.dat$": {
            "filename": "ConWx_prog_<timestamp>_048.dat",
            "delay_h": 5,
            "timelist": [0, 6, 12, 18],
        },
        r"^ConWx_prog_\d{10}_180\.dat$": {
            "filename": "ConWx_prog_<timestamp>_180.dat",
            "delay_h": 7,
            "timelist": [2, 8, 14, 20],
        },
    }

    print("Files and Directories in '% s':" % template_path)
    obj = os.scandir(template_path)
    for entry in obj:
        if entry.is_dir() or entry.is_file():
            print(entry.name)
    obj.close()
    # Go through all files in template path and check if they fit any of the config-regex
    for template_file in [
        fs_item for fs_item in os.scandir(template_path) if fs_item.is_file()
    ]:
        for config in [
            CONFIG_DATA[key]
            for key in CONFIG_DATA.keys()
            if re.search(key, template_file.name)
        ]:
            print("I was here")
            # Determine if the file is expected to arrive 'now'
            if dt.now().hour in config["timelist"]:
                with open(template_file.path) as f:
                    template = f.read().split("\n")
                new_timestamp = time.strftime(
                    r"%Y%m%d%H", (dt.now() - td(hours=config["delay_h"])).timetuple()
                )
                new_filename = config["filename"].replace("<timestamp>", new_timestamp)
                with open(os.path.join(output_path, new_filename), "w") as f:
                    f.write(
                        "\n".join(
                            change_dummy_timestamp(
                                template, dt.now() - td(hours=config["delay_h"])
                            )
                        )
                    )
                log.info(f"Created dummy-file '{new_filename}'")
                continue


def change_dummy_timestamp(
    contents: list[str], new_t0: dt = dt.now(), max_forecast_h: int = 1000
) -> list[str]:
    """Takes file contents (contents) as a list of lines and then changes the timestamp base on new_t0.

    Parameters
    ----------
    contents : list[str]
        Contents of the forecast-template.
    new_t0 : datetime.datetime
        (Optional) The new t0-timestamp.
        Default = now().
    max_forecast_h : int
        (Optional) Maximum hours - only used to somewhat verify regex value is a timestamp.
        Default = 1000 (leave alone if unsure)
    """
    # All templates have the t0 time at the end of first line after a '='-sign
    timestring = contents[0].replace(" ", "").split("=")[-1]
    template_t0 = dt.fromtimestamp(time.mktime(time.strptime(timestring, r"%Y%m%d%H")))

    new_contents = []
    for row in contents:
        new_row = row
        # Regex: Look for exactly 10 digits with no digits right before or after
        for match in re.finditer(r"(?<!\d)(\d{10})(?!\d)", row):
            try:
                template_time = dt.fromtimestamp(
                    time.mktime(time.strptime(match.group(0), r"%Y%m%d%H"))
                )
            except Exception:
                pass
            else:
                if (
                    0
                    <= (template_time - template_t0).total_seconds() / 3600
                    < max_forecast_h
                ):
                    new_time = new_t0 + (template_time - template_t0)
                    new_row = (
                        new_row[: match.start()]
                        + time.strftime(r"%Y%m%d%H", new_time.timetuple())
                        + new_row[match.end() :]
                    )
        new_contents.append(new_row)

    return new_contents


def read_and_remove_forecast(forecast_file_path: str) -> str:
    """Read file into memory and remove it from source

    Parameters
    ----------
    forecast_file_path : str
        Full path of forecast file

    Returns
    -------
    str
        File as string
    """
    try:
        with open(forecast_file_path) as file:
            file_contents = file.read()
    except Exception:
        log.error(f"Could not read file '{forecast_file_path}'.")

    try:
        os.remove(forecast_file_path)
    except Exception:
        log.error(f"Could not remove file '{forecast_file_path}'")

    return file_contents


def main(
    forecast_api: singuapi,
    settings: configuration.Settings,
    field_dict: dict,
    coords_dict: dict,
    scan_interval_s: int = 5,
    timer: scheduler = None,
):
    # Save input arguments to pass into timer at the end of function
    local_args = tuple(locals().values())

    log.debug(f"Scanning '{settings.FORECAST_FOLDER}' folder..")
    # TODO - Improve ListComprehension if possible
    for file in [
        fs_item for fs_item in os.scandir(settings.FORECAST_FOLDER) if fs_item.is_file()
    ]:
        # TODO - Read file here and send to extractor
        if re.search(settings.FILE_FILTER, file.name) is not None:
            log.info(f"Parsing file '{file.path}'.")
            forecast_data = extract_forecast(file.path, field_dict, coords_dict)
            forecast_api["weather_forecast"] = update_forecast(
                forecast_api["weather_forecast"], forecast_data
            )
        else:
            log.warning(f"Unknown file found: {file.path}")

    if timer is not None:
        timer.enter(scan_interval_s, 1, main, local_args)


if __name__ == "__main__":
    LOG_LEVEL = configuration.get_log_settings()
    log = configuration.get_logger(__name__, LOG_LEVEL)
    log.info("Initializing forecast-parser..")

    forecast_api = singuapi()
    settings = configuration.get_settings()
    grid_points = load_geographical_coordinates(settings.GRID_POINT_PATH)
    timer = scheduler(time.time, time.sleep)
    scan_interval_s = 5
    timer.enter(15, 1, main, (settings, grid_points, timer))

    if settings.USE_MOCK_DATA:
        # MOVE MOVE MOVE MOVE MOVE MOVE MOVE MOVE MOVE
        # Run creation of mock-data at first coming x:30 absolute time
        next_run = (dt.now()).replace(microsecond=0, second=15, minute=0)
        if dt.now().minute >= 30:
            next_run += td(hours=1)

        timer.enterabs(
            next_run.timestamp(),
            1,
            generate_dummy_input,
            (settings.APP_FOLDER, settings.FORECAST_FOLDER, timer),
        )
    # /MOVE
    log.info("Initialization done - Starting scheduler..")
    timer.run()
