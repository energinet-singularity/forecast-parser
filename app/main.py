import app.configuration.configuration as configuration
import app.parsers as parsers
import app.mock_data as mock_data
import logging
import pandas as pd
import re
import os
import time
from sched import scheduler
from singupy.api import DataFrameAPI as singuapi

log = logging.getLogger()


class WeatherForecast:
    type: str
    file_filter: str
    helpers: dict
    __df: pd.DataFrame = None
    __parser: callable[list(str), dict]

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    def __init__(self, type: str, file_filter: str, parser: callable[list(str), dict], helpers: dict = {}) -> None:
        self.type = type
        self.file_filter = file_filter
        self.__parser = parser
        self.helpers = helpers

    def load(self, file_contents: list(str)) -> None:
        try:
            self.__df = self.__parser(file_contents, self.helpers)
        except Exception as e:
            log.error(f"Could not parse forecast - failed with error: {e}")

class ForecastJoiner:
    __forecasts: dict[WeatherForecast]

    def __init__(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self.__forecasts)

    def __iter__(self):
        return iter(self.__forecasts.keys())

    def __getitem__(self, name) -> WeatherForecast:
        try:
            return self.__forecasts[name]
        except Exception:
            log.error(f"Could not access WeatherForecase with name '{name}' in ForecastJoiner object.")
            raise KeyError(f"Object contains no WeatherForecast with name '{name}'.")

    def __setitem__(self, name, df) -> None:
        if isinstance(df, WeatherForecast):
            self.__forecasts[name] = df
        elif df is None:
            if name in self.__forecasts:
                del self.__forecasts[name]
        else:
            log.error(f"Tried to set value of '{name}' WeatherForecast to an object of '{type(df)} type in ForecastJoiner object.'")
            raise ValueError("Input must be either a valid WeatherForecast object or 'None'")

    def __delitem__(self, name) -> None:
        if name in self.__forecasts:
            del self.__forecasts[name]
        else:
            log.error(f"Could not find (and thereby delete) WeatherForecast with name '{name}' in ForecastJoiner object.")
            raise KeyError(f"Object contains no WeatherForecast with name '{name}'.")

    def Forecast(self) -> pd.DataFrame:
        forecast_dict = {src.type: src.df for src in self.__forecasts if src.df is not None}
        return pd.concat(forecast_dict)
        

def list_files_by_age(path: str, filter: str = ".*") -> list[str]:
    pass


def read_and_remove_file(file_path: str) -> list[str]:
    """Read file into memory and remove it from source

    Parameters
    ----------
    file_path : str
        Full path of file

    Returns
    -------
    list
        File as list of strings
    """
    try:
        file_contents = []
        with open(file_path) as file:
            file_contents = file.readlines()
    except Exception:
        log.error(f"Could not read file: '{file_path}'.")

    try:
        os.remove(file_path)
    except Exception:
        log.error(f"Could not remove file: '{file_path}'")

    return file_contents


def main(
    forecast_api: singuapi,
    settings: configuration.Settings,
    all_forecasts: ForecastJoiner,
    timer: scheduler = None,
):
    # Save input arguments to pass into timer at the end of function
    local_args = tuple(locals().values())
    data_updated = False

    for file in list_files_by_age(path=settings.FORECAST_FOLDER):
        for forecast in all_forecasts:
            if re.match(forecast.file_filter, file):
                try:
                    log.info(f"Parsing file '{file}'.")
                    with open(file) as f:
                        forecast.load(f.readlines)
                    data_updated = True
                except Exception as e:
                    log.error("Could not load forecast in file '{file}'.")
                    log.exception(e)                
                finally:
                    # TODO : How do you want to handle file could not be removed?
                    os.remove(file)

        if data_updated:
            forecast_api[settings.API_DBNAME] = all_forecasts.Forecast()

    if timer is not None:
        timer.enter(settings.scan_interval_s, 1, main, local_args)


if __name__ == "__main__":
    # Initialization
    # TODO: FIX LOGGING and MOCK data
    LOG_LEVEL = configuration.get_log_settings()
    log.setLevel(LOG_LEVEL)
    log.info("Initializing forecast-parser..")

    settings = configuration.get_settings()
    if settings.USE_MOCK_DATA:
        mock_data.setup_mock_data(settings.APP_FOLDER, settings.FORECAST_FOLDER)

    timer = scheduler(time.time, time.sleep)

    # Initialize forecasts and API
    all_forecasts = ForecastJoiner()

    all_forecasts[settings.ECM_TYPE_NAME] = WeatherForecast(type=settings.ECM_TYPE_NAME,
                                                                file_filter=settings.ECM_FILE_FILTER,
                                                                parser=parsers.dmi_parser,
                                                                helpers=parsers.get_dmi_helpers())

    all_forecasts[settings.NEA_TYPE_NAME] = WeatherForecast(type=settings.NEA_TYPE_NAME,
                                                                file_filter=settings.NEA_FILE_FILTER,
                                                                parser=parsers.dmi_parser,
                                                                helpers=parsers.get_dmi_helpers())

    all_forecasts[settings.CONWX_TYPE_NAME] = WeatherForecast(type=settings.CONWX_TYPE_NAME,
                                                                  file_filter=settings.CONWX_FILE_FILTER,
                                                                  parser=parsers.conwx_parser,
                                                                  helpers=parsers.get_conwx_helpers())

    forecast_api = singuapi(dbname=settings.API_DBNAME, port=settings.API_PORT)

    timer.enter(15, 1, main, (forecast_api, settings, all_forecasts, timer))
    log.info("Initialization done - Starting scheduler..")
    timer.run()
