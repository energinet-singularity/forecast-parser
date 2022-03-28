import logging
import pandas as pd
from datetime import datetime as dt, timedelta as td
import json
import re
import os
import time
from sched import scheduler
from typing import List
from singupy import api as singu_api

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
    log.debug("Running dummy-input generation")
    if timer is not None:
        next_run = (dt.now()).replace(microsecond=0, second=0, minute=30) + td(hours=1)
        timer.enterabs(next_run.timestamp(), 1, generate_dummy_input, (template_path, output_path, timer))

    # Make template-file config
    CONFIG_DATA = {
        r"^ENetNEA_\d{10}\.txt$": {
            "filename": "ENetNEA_<timestamp>.txt",
            "delay_h": 3,
            "timelist": [1, 4, 7, 10, 13, 16, 19, 22]},
        r"^EnetEcm_\d{10}\.txt$": {
            "filename": "EnetEcm_<timestamp>.txt",
            "delay_h": 7,
            "timelist": [8, 20]},
        r"^ConWx_prog_\d{10}_048\.dat$": {
            "filename": "ConWx_prog_<timestamp>_048.dat",
            "delay_h": 5,
            "timelist": [0, 6, 12, 18]},
        r"^ConWx_prog_\d{10}_180\.dat$": {
            "filename": "ConWx_prog_<timestamp>_180.dat",
            "delay_h": 7,
            "timelist": [2, 8, 14, 20]}}

    # Go through all files in template path and check if they fit any of the config-regex
    for template_file in [fs_item for fs_item in os.scandir(template_path) if fs_item.is_file()]:
        for config in [CONFIG_DATA[key] for key in CONFIG_DATA.keys() if re.search(key, template_file.name)]:
            # Determine if the file is expected to arrive 'now'
            if dt.now().hour in config['timelist']:
                with open(template_file.path) as f:
                    template = f.read().split('\n')
                new_timestamp = time.strftime(r"%Y%m%d%H", (dt.now()-td(hours=config['delay_h'])).timetuple())
                new_filename = config['filename'].replace('<timestamp>', new_timestamp)
                with open(os.path.join(output_path, new_filename), "w") as f:
                    f.write('\n'.join(change_dummy_timestamp(template, dt.now()-td(hours=config['delay_h']))))
                log.info(f"Created dummy-file '{new_filename}'")
                continue


def change_dummy_timestamp(contents: List[str], new_t0: dt = dt.now(), max_forecast_h: int = 1000) -> List[str]:
    """Takes file contents (contents) as a list of lines and then changes the timestamp base on new_t0.

    Parameters
    ----------
    contents : List[str]
        Contents of the forecast-template.
    new_t0 : datetime.datetime
        (Optional) The new t0-timestamp.
        Default = now().
    max_forecast_h : int
        (Optional) Maximum hours - only used to somewhat verify regex value is a timestamp.
        Default = 1000 (leave alone if unsure)
    """
    # All templates have the t0 time at the end of first line after a '='-sign
    timestring = contents[0].replace(' ', '').split('=')[-1]
    template_t0 = dt.fromtimestamp(time.mktime(time.strptime(timestring, r"%Y%m%d%H")))

    new_contents = []
    for row in contents:
        new_row = row
        # Regex: Look for exactly 10 digits with no digits right before or after
        for match in re.finditer(r'(?<!\d)(\d{10})(?!\d)', row):
            try:
                template_time = dt.fromtimestamp(time.mktime(time.strptime(match.group(0), r"%Y%m%d%H")))
            except Exception:
                pass
            else:
                if 0 <= (template_time - template_t0).total_seconds()/3600 < max_forecast_h:
                    new_time = new_t0 + (template_time - template_t0)
                    new_row = new_row[:match.start()] + time.strftime(r"%Y%m%d%H", new_time.timetuple()) + \
                        new_row[match.end():]
        new_contents.append(new_row)

    return new_contents


def main_loop(rest_api: singu_api.DataFrameAPI, input_folder: str, file_filter: str, field_dict: dict, 
              scan_interval_s: int = 5, timer: scheduler = None):
    # Save input arguments to pass into timer at the end of function
    local_args = tuple(locals().values())

    log.debug(f"Scanning '{input_folder}' folder..")
    for file in [fs_item for fs_item in os.scandir(input_folder) if fs_item.is_file()]:
        if re.search(file_filter, file.name) is not None:
            log.info(f"Parsing file '{file.path}'.")
            rest_api.update_data(extract_forecast(file.path, field_dict))
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
    TEMPLATE_FOLDER = "/templates/"
    with open("app/namedict.json") as f:
        fields = json.loads(f.read())
    rest_api = singu_api.DataFrameAPI(pd.DataFrame())
    timer = scheduler(time.time, time.sleep)
    
    # Set up mocking of data
    if os.environ.get('USE_MOCK_DATA', 'FALSE').upper() == 'FALSE':
        pass
    elif os.environ['USE_MOCK_DATA'] == 'TRUE':
        # Run creation of mock-data at firstcoming x:30 absolute time
        next_run = (dt.now()).replace(microsecond=0, second=0, minute=30)
        if dt.now().minute >= 30:
            next_run += td(hours=1)
        timer.enterabs(next_run.timestamp(), 1, generate_dummy_input, (TEMPLATE_FOLDER, FORECAST_FOLDER, timer))
    else:
        raise ValueError(f"'USE_MOCK_DATA' env. variable is '{os.environ['USE_MOCK_DATA']}',"
                         " but must be either 'TRUE', 'FALSE' or unset.")

    timer.enter(15, 1, main_loop, (rest_api, FORECAST_FOLDER, FILE_FILTER, fields, FOLDER_CHECK_WAIT, timer))

    # Start the scheduler
    log.info("Initialization done - Starting scheduler..")
    timer.run()
