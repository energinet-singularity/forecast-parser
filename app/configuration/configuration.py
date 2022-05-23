import logging
import os

def get_log_settings() -> str:
    # Logging
    LOG_LEVEL = os.environ.get('DEBUG', 'FALSE')
    return LOG_LEVEL


def get_settings() -> tuple:
    # Data files
    FILE_FILTER = r"(E[Nn]et(NEA|Ecm)_|ConWx_prog_)\d+(_\d{3})?\.(txt|dat)"
    FOLDER_CHECK_WAIT = 5
    FORECAST_FOLDER = "/forecast-files/"
    TEMPLATE_FOLDER = "/app/"
    GRID_POINT_PATH = TEMPLATE_FOLDER + 'gridpoints.csv'
    return (FILE_FILTER, FOLDER_CHECK_WAIT, FORECAST_FOLDER, TEMPLATE_FOLDER, GRID_POINT_PATH)


def get_logger(application_name: str, LOG_LEVEL:str):
    if LOG_LEVEL.upper() == 'FALSE':
        debug_level = logging.INFO
    elif LOG_LEVEL.upper() == 'TRUE':
        debug_level = logging.DEBUG
    else:
        raise ValueError(f"'DEBUG' env. variable is '{os.environ['DEBUG']}', but must be either 'TRUE', 'FALSE' or unset.")

    logging.basicConfig(format="%(levelname)s:%(asctime)s:%(name)s - %(message)s")
    logger = logging.getLogger(application_name)
    logger.setLevel(debug_level)
    return logger
