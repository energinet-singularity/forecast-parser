#!/usr/bin/env python3
import pytest
import os
import app.forecast_parser


# Load valid files as fixture
@pytest.fixture
def valid_files():
    return [os.path.abspath(os.path.join(dirpath, f)) for dirpath, _, filenames in
            os.walk(f"{os.path.dirname(os.path.realpath(__file__))}/valid-testdata/") for f in filenames]


@pytest.fixture
def invalid_files():
    return [os.path.abspath(os.path.join(dirpath, f)) for dirpath, _, filenames in
            os.walk(f"{os.path.dirname(os.path.realpath(__file__))}/invalid-testdata/") for f in filenames]


# Check all files files can be parsed
def test_valid_files_can_be_parsed(valid_files):
    _, field_dict, _ = app.forecast_parser.load_grid_points("app/gridpoints.csv", "app/ksql-config.json", "test-topic")

    for file in valid_files:
        print(f"Validating file: {file}")
        # Load the forecast from each file
        pd = app.forecast_parser.extract_forecast(file, field_dict)

        # Verify function returned something
        assert pd is not None

        # Check dimensions are as expected - theoretically a smaller file/object could be okay, but currently they are this big
        assert pd.shape[0] > 2000   # Will be 2000+ for a normal file
        assert pd.shape[1] > 50     # Will be 50+ for a normal file

        # Check all index names are in the field_dict
        for index in pd.index.tolist():
            assert index in field_dict.values()
