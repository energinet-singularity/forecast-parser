import pytest
import os
import app.forecast_parser
import app.configuration.configuration
from singupy import api as singuapi

def test_query_weather_api_should_return_weather_forecast():
    # Arrange
    # parser = app.forecast_parser
    
    # api = singuapi.DataFrameAPI(dbname=API_DB_NAME, port=API_PORT)

    # Act
    # parser.main()

    # Assert

    pass

def test_get_logger_should_set_debug_level():
    logger =  app.configuration.configuration.get_logger(__name__)

    assert logger is not None

