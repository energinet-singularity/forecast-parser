#!/usr/bin/env python3
import datetime
import time
import pytest
import os
import app.forecast_parser
import pandas as pd


# Load valid files as fixture
@pytest.fixture
def valid_files():
    return [os.path.abspath(os.path.join(dirpath, f)) for dirpath, _, filenames in
            os.walk(f"{os.path.dirname(os.path.realpath(__file__))}/valid-testdata/") for f in filenames]


# @pytest.fixture
def invalid_files():
    return [os.path.abspath(os.path.join(dirpath, f)) for dirpath, _, filenames in
            os.walk(f"{os.path.dirname(os.path.realpath(__file__))}/invalid-testdata/") for f in filenames]


# Check all files files can be parsed
def test_valid_files_can_be_parsed(valid_files):
    geo_coordinates = app.forecast_parser.load_geographical_coordinates("app/gridpoints.csv")

    for file in valid_files:
        print(f"Validating file: {file}")
        # Load the forecast from each file
        pd = app.forecast_parser.extract_forecast(file, geo_coordinates)

        # Verify function returned something
        assert pd is not None

        # Check dimensions are as expected - theoretically a smaller file/object could be okay, but currently they are this big
        assert pd.shape[0] > 2000   # Will be 2000+ for a normal file
        assert pd.shape[1] > 50     # Will be 50+ for a normal file

        # Check all index names are in the field_dict
        for index in pd.index.tolist():
            assert index in geo_coordinates.values()

def test_remove_old_data_from_df(): 
    #arrange
    expected_row_cout_after_remove = 5
    now = datetime.datetime.now()
    delta = datetime.timedelta(hours=1)
    time_stamps =[]
    for i in range(10):
        time_stamps.append(now - (delta * i))

    dataframe = pd.DataFrame(time_stamps, columns = ['timestamp'])

    #act
    dataframe = app.forecast_parser.remove_old_data_from_df(dataframe=dataframe,time_col_name= "timestamp", max_age_in_hours=5)


    #assert
    assert len(dataframe.index) == expected_row_cout_after_remove



               
