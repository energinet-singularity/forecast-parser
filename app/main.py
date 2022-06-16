import configuration
import parsers
import mock_data

import logging
import pandas as pd
import re
import os
import schedule
from singupy.api import DataFrameAPI as singuapi
from typing import Iterator, Callable
from time import sleep

log = logging.getLogger()


class WeatherForecast:
    """Class for storing weather forecasts and their settings

    Attributes
    ----------
    file_filter : str
        Regex-based string to identify a file to ID as this forecast type
    helpers : dict
        Dictionary of required helpers for parsing the file (the dict is
        passed to the parsing function)
    df : pd.DataFrame (Read only)
        Pandas DataFrame containing the forecast data
    """

    file_filter: str
    helpers: dict
    __df: pd.DataFrame = None
    __parser: Callable[[list[str], dict], pd.DataFrame]

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    def __init__(
        self,
        file_filter: str,
        parser: Callable[[list[str], dict], pd.DataFrame],
        helpers: dict = {},
    ) -> None:
        """Create a new WeatherForecast object

        Parameters
        ----------
        file_filter : str
            Regex-based string to identify a file to ID as this forecast type
        parser : Callable[[list[str], dict], pd.DataFrame]
            Function which can parse the file - will receive the forecast
            as a list of lines and the 'helpers' dict and must return a pandas
            DataFrame
        helpers : dict, optional
            Dictionary with information used by the parser, by default {}
        """
        self.file_filter = file_filter
        self.__parser = parser
        self.helpers = helpers

    def load(self, forecast_data: list[str]) -> None:
        """Update forecast DataFrame with new data

        Parameters
        ----------
        forecast_data : list[str]
            Forecast data as a list of strings (usually from file)
        """
        try:
            self.__df = self.__parser(forecast_data, self.helpers)
        except Exception as e:
            log.error(f"Could not parse forecast.")
            raise e


class ForecastManager:
    """Class for managing multiple forecasts and the API

    Attributes
    ----------
    DataFrame : pd.DataFrame (Read only)
        Pandas DataFrame containing all forecast data
    """

    __forecasts: dict[WeatherForecast]
    __api: singuapi
    __dbname: str

    def __init__(self, api: singuapi, dbname: str):
        """Create a new ForecastManager object

        Parameters
        ----------
        api : singuapi
            API where the DataFrame is served
        dbname : str
            Name of the database in the API object
        """
        self.__forecasts = {}
        self.__api = api
        self.__dbname = dbname

    def __len__(self) -> int:
        return len(self.__forecasts)

    def __iter__(self) -> Iterator[WeatherForecast]:
        return iter(self.__forecasts.values())

    def __getitem__(self, name) -> WeatherForecast:
        try:
            return self.__forecasts[name]
        except Exception:
            log.error(
                f"Could not access WeatherForecast with name '{name}'"
                + " in ForecastManager object."
            )
            raise KeyError(f"Object contains no WeatherForecast with name '{name}'.")

    def __setitem__(self, name, forecast) -> None:
        if isinstance(forecast, WeatherForecast):
            self.__forecasts[name] = forecast
        elif forecast is None:
            if name in self.__forecasts:
                del self.__forecasts[name]
        else:
            log.error(
                f"Tried to set value of '{name}' WeatherForecast to an"
                + f" object of '{type(forecast)} type in ForecastManager object.'"
            )
            raise ValueError(
                "Input must be either a valid WeatherForecast object or 'None'"
            )

    def __delitem__(self, name) -> None:
        if name in self.__forecasts:
            del self.__forecasts[name]
        else:
            log.error(
                "Could not find (and thereby delete) WeatherForecast"
                + f" with name '{name}' in ForecastManager object."
            )
            raise KeyError(f"Object contains no WeatherForecast with name '{name}'.")

    @property
    def DataFrame(self) -> pd.DataFrame:
        try:
            return self.__api[self.__dbname]
        except Exception:
            return pd.DataFrame()

    def UpdateForecast(self):
        """Create new combined forecast and assign it to the API"""
        forecast_dict = [
            self.__forecasts[src].df.assign(forecast_type=src)
            for src in self.__forecasts
            if self.__forecasts[src].df is not None
        ]
        # Recreate index with the 'ignore_index' flag
        self.__api[self.__dbname] = pd.concat(forecast_dict, ignore_index=True)

    def ScanPath(self, path: str):
        """Check for new files in path and update manager if relevant

        Parameters
        ----------
        path : str
            Path to scan for new forecast files
        """
        parse_list = {}

        for forecast in self.__forecasts.values():
            for file in list_files_by_age(path=path, filter=forecast.file_filter):
                parse_list[file] = forecast

        for file in list(parse_list):
            log.info(f"Parsing file '{file}'.")
            try:
                parse_list[file].load(load_file(file))
            except Exception as e:
                log.error(f"Could not load forecast in file '{file}'.")
                log.exception(e)
                del(parse_list[file])

        if len(parse_list) > 0:
            self.UpdateForecast()


def list_files_by_age(path: str, filter: str = ".*") -> list[str]:
    """List files in 'path' where file name fits 'filter' and sort with oldest first"""
    file_list = []

    try:
        file_list = [
            file.path
            for file in os.scandir(path)
            if file.is_file() and re.match(filter, file.name)
        ]
        file_list.sort(key=os.path.getmtime)

    except Exception:
        log.exception(f"An exception occurred while listing files in path '{path}'")
        # TODO : raise exception when running as sidecar or file handling inside container

    return file_list


def load_file(file_path: str, remove_file_after_load: bool = True) -> list[str]:
    """Load file 'file_path' into list and remove it if 'remove_file_after_load'"""
    try:
        with open(file_path) as file:
            file_contents = file.readlines()
        if remove_file_after_load:
            os.remove(file_path)
    except Exception as e:
        log.exception(e)
        raise e

    return file_contents


def get_manager(settings: configuration.Settings) -> ForecastManager:
    """Create and return a ForecastManager given a settings-input

    Parameters
    ----------
    settings : configuration.Settings
        Set of configurations/settings used for initializing

    Returns
    -------
    ForecastManager
        Initialized ForecastManager instance
    """
    forecast_api = singuapi(dbname=settings.API_DBNAME, port=settings.API_PORT)
    forecast_manager = ForecastManager(api=forecast_api, dbname=settings.API_DBNAME)

    forecast_manager[settings.ECM_TYPE_NAME] = WeatherForecast(
        file_filter=settings.ECM_FILE_FILTER,
        parser=parsers.dmi_parser,
        helpers=parsers.get_dmi_helpers(),
    )

    forecast_manager[settings.NEA_TYPE_NAME] = WeatherForecast(
        file_filter=settings.NEA_FILE_FILTER,
        parser=parsers.dmi_parser,
        helpers=parsers.get_dmi_helpers(),
    )

    forecast_manager[settings.CONWX_TYPE_NAME] = WeatherForecast(
        file_filter=settings.CONWX_FILE_FILTER,
        parser=parsers.conwx_parser,
        helpers=parsers.get_conwx_helpers(settings.GRID_POINT_PATH),
    )

    return forecast_manager


if __name__ == "__main__":
    configuration.set_logging_format()
    settings = configuration.get_settings()
    configuration.setup_log(log, settings.LOGLEVEL)
    log.info("Initializing forecast-parser..")

    if settings.USE_MOCK_DATA:
        log.info("Mock data enabled. Setting scheduler to run each hour at x:30.")
        mock_data.dummy_input_planner(
            template_path=settings.TEMPLATE_PATH,
            output_path=settings.FORECAST_PATH,
            force=True,
        )
        schedule.every().hour.at(":30").do(
            mock_data.dummy_input_planner,
            template_path=settings.TEMPLATE_PATH,
            output_path=settings.FORECAST_PATH,
        )

    forecast_manager = get_manager(settings=settings)
    schedule.every(settings.FOLDER_SCAN_INTERVAL_S).seconds.do(forecast_manager.ScanPath, path=settings.FORECAST_PATH)

    log.info("Initialization done - Starting scheduler..")
    while True:
        schedule.run_pending()
        sleep(settings.MAIN_SLEEP_TIME_S)
