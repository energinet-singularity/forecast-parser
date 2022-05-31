import logging
import os
from dataclasses import dataclass


def get_log_settings() -> str:
    log_level_value = os.environ.get("LOG_LEVEL", "INFO")
    if not log_level_value in ("INFO", "DEBUG"):
        raise ValueError(
            f"'LOG_LEVEL' env. variable is '{log_level_value}', but must be either 'INFO', 'DEBUG' or unset."
        )

    return log_level_value


@dataclass(frozen=True, eq=True)
class Settings:
    FILE_FILTER = r"(E[Nn]et(NEA|Ecm)_|ConWx_prog_)\d+(_\d{3})?\.(txt|dat)"
    FOLDER_CHECK_WAIT = 5
    FORECAST_FOLDER = "/forecast-files/"
    APP_FOLDER = "/app/"
    GRID_POINT_PATH = "app/gridpoints.csv"
    FIELD_MAP_PATH = "app/field-map.json"
    USE_MOCK_DATA: bool = False
    scan_interval_s = 5


def get_settings() -> Settings:

    use_mock_data_value = os.environ.get("USE_MOCK_DATA", "TRUE").upper()

    if not use_mock_data_value in ("TRUE", "FALSE"):
        raise ValueError(
            f"'USE_MOCK_DATA' env. variable is '{use_mock_data_value}',"
            " but must be either 'TRUE', 'FALSE' or unset."
        )

    use_mock_data = use_mock_data_value == "TRUE"

    settings: Settings = Settings(USE_MOCK_DATA=use_mock_data)
    return settings


def get_logger(application_name: str, LOG_LEVEL: str):
    if LOG_LEVEL.upper() == "INFO":
        debug_level = logging.INFO
    elif LOG_LEVEL.upper() == "DEBUG":
        debug_level = logging.DEBUG

    logging.basicConfig(format="%(levelname)s:%(asctime)s:%(name)s - %(message)s")
    logger = logging.getLogger(application_name)
    logger.setLevel(debug_level)
    return logger
