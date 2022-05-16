import pytest
from fusionbase.Fusionbase import DataStream
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

@pytest.mark.skip(reason="not implemented yet")
def test_update(data_stream_editable: DataStream):
  raise NotImplementedError

@pytest.mark.skip(reason="not implemented yet")
def test_replace(data_stream_editable: DataStream):
  raise NotImplementedError

def test_get_data(data_stream: DataStream):
  data = data_stream.get_data()
  assert len(data) >= 5

def test_get_dataframe(data_stream: DataStream):
  df = data_stream.get_dataframe()
  assert len(df) >= 5

@pytest.mark.skip(reason="not implemented yet")
def test_get_delta_data(data_stream: DataStream):
  delta_version = ''
  delta_data = data_stream.get_delta_data(delta_version)
  assert len(delta_data) >= 5


@pytest.mark.skip(reason="not implemented yet")
def test_get_delta_dataframe(data_stream: DataStream):
  delta_version = ''
  df = data_stream.get_delta_dataframe(delta_version)
  assert len(df) >= 5