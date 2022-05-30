import pytest
import os
import app.forecast_parser
import app.configuration.configuration as config
from singupy import api as singuapi

LOG_LEVEL = config.get_log_settings()
# Initialize log
log = config.get_logger(__name__, LOG_LEVEL)


def test_query_weather_api_should_return_weather_forecast():
    # Arrange
    # parser = app.forecast_parser
    
    # api = singuapi.DataFrameAPI(dbname=API_DB_NAME, port=API_PORT)

    # Act
    # parser.main()

    # Assert

    pass

def test_get_logger_should_set_debug_level():
    LOG_LEVEL = config.get_log_settings()
    logger =  config.get_logger(__name__, LOG_LEVEL)

    assert logger is not None

def test_main():
    # Get settings
    LOG_LEVEL = config.get_log_settings()
    # Initialize log
    log = config.get_logger(__name__, LOG_LEVEL)
    app.forecast_parser.main(scan_interval_s=10)
