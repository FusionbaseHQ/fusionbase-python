import binascii
import os
import numpy as np
import pandas as pd
import pytest
from fusionbase.Fusionbase import DataStream, Fusionbase
import io
import sys

def test_print(data_stream: DataStream):
  capturedOutput = io.StringIO()                  
  sys.stdout = capturedOutput
  print(data_stream)
  sys.stdout = sys.__stdout__
  assert len(capturedOutput.getvalue()) > 5
  
def test_key_property(data_stream: DataStream):
  assert data_stream.key == pytest.generic_stream_key
  
def test_pretty_meta_data(data_stream: DataStream):
  capturedOutput = io.StringIO()                 
  sys.stdout = capturedOutput
  data_stream.pretty_meta_data()
  sys.stdout = sys.__stdout__
  assert len(capturedOutput.getvalue()) > 5
  
def test_get_meta_data(data_stream: DataStream):
  meta_data = data_stream.get_meta_data()
  meta_data_keys = ['_key', 'unique_label', 'name', 'description', 'meta', 'created_by', 'update_policy', 'source', 'data_item_collections', 'store_version', 'data_version', 'data_updated_at', 'data', 'created_at', 'updated_at']
  for k in meta_data_keys:
    assert k in meta_data.keys() , f'{k} IS MISSING IN METADATA'
  
@pytest.mark.skip(reason="no way of currently testing this")
def test_update_metadata(data_stream: DataStream):
  raise NotImplementedError

@pytest.mark.skip(reason="no way of currently testing this")
def test_set_source(data_stream: DataStream):
  raise NotImplementedError

def test_update(data_stream_editable: DataStream):
  df = pd.DataFrame(np.random.randint(0,10000,size=(100, 4)), columns=list('ABCD'))
  data = df.to_dict('records')
  result = data_stream_editable.update(data=data)
  assert result['success'] == True

def test_replace(data_stream_editable: DataStream):
  df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
  data = df.to_dict('records')
  result = data_stream_editable.replace(data=data, sanity_check=False)
  assert result['success'] == True

def test_get_data(data_stream: DataStream):
  data = data_stream.get_data()
  assert len(data) >= 5

def test_get_dataframe(data_stream: DataStream):
  df = data_stream.get_dataframe()
  meta_data = data_stream.get_meta_data()
  assert len(df) == meta_data["meta"]["entry_count"]

def test_get_dataframe_skip_limit(data_stream: DataStream):
  df = data_stream.get_dataframe(skip=3000, limit=10, live=True)
  print(df)
  assert len(df) == 10

def test_get_delta_data(data_stream: DataStream):
  delta_version = '87bd0300-08d6-4aee-9a95-348d2356fa06'
  delta_data = data_stream.get_delta_data(delta_version)
  assert len(delta_data) >= 5

def test_get_delta_dataframe(data_stream: DataStream):
  delta_version = '87bd0300-08d6-4aee-9a95-348d2356fa06'
  df = data_stream.get_delta_dataframe(delta_version)
  assert len(df) >= 5
  
def test_create_stream(fusionbase: Fusionbase):
  unique_label = f'python_package_test_stream__{str(binascii.b2a_hex(os.urandom(10)))}'
  df = pd.read_csv('./tests/data/oktoberfest_beer.csv')
  result_stream = fusionbase.create_stream(unique_label=unique_label, name={"en": f"OKTOBER_FEST_{unique_label.upper()}"}, description={"en": "A TEST STREAM TO TEST THE CREATE STREAM FUNCTION OF THE PYTHON PACKAGE"}, scope="PUBLIC", source={"_id": "data_sources/17255624", "stream_specific": {"uri": "https://fusionbase.com"}}, data=df)
  print(result_stream)
  assert isinstance(result_stream, DataStream), 'RESULT OF UPDATE CREATE MUST BE A DATASTREAM'
  