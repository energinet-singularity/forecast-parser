#!/usr/bin/env python3
import sys, os, pytest, json

#Adding parent dir to path to find and load module/file 'forecast_parser' in parent dir
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from forecast_parser import *

#Load field_dict used in the extract_forecast as fixture
@pytest.fixture
def load_field_dict():
    return {text:field['ID'] for field in json.loads(open("./ksql-config.json").read())['fields'] for text in field['Text']}

#Load valid files as fixture
@pytest.fixture
def valid_files():
    return [os.path.abspath(os.path.join(dirpath, f)) for dirpath,_,filenames in os.walk(f"{os.path.dirname(os.path.realpath(__file__))}/valid-testdata/") for f in filenames]

@pytest.fixture
def invalid_files():
    return [os.path.abspath(os.path.join(dirpath, f)) for dirpath,_,filenames in os.walk(f"{os.path.dirname(os.path.realpath(__file__))}/invalid-testdata/") for f in filenames]

#Check all files files can be parsed
def test_valid_files_can_be_parsed(load_field_dict, valid_files):
    for file in valid_files:
        print(f"Validating file: {file}")
        #Load the forecast from each file
        pd = extract_forecast(file, load_field_dict)

        #Verify function returned something
        assert pd is not None

        #Check dimensions are as expected - theoretically a smaller file/object could be okay, but currently they are this big
        assert pd.shape[0] > 2000   #Will be 2000+ for a normal file
        assert pd.shape[1] > 50     #Will be 

        #Check all index names are in the field_dict
        for index in pd.index.tolist():
            assert index in load_field_dict.values()