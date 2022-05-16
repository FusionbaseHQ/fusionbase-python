import pytest
from fusionbase.Fusionbase import DataService
import io
import sys

@pytest.mark.skip(reason="not implemented yet")
def test_print(data_service: DataService):
  capturedOutput = io.StringIO()                  
  sys.stdout = capturedOutput
  print(data_service)
  sys.stdout = sys.__stdout__
  assert len(capturedOutput.getvalue()) > 5
  
@pytest.mark.skip(reason="not implemented yet")
def test_key_property(data_service: DataService):
  assert data_service.key == pytest.generic_service_key
  
@pytest.mark.skip(reason="not implemented yet")
def test_pretty_meta_data(data_service: DataService):
  capturedOutput = io.StringIO()                 
  sys.stdout = capturedOutput
  data_service.pretty_meta_data()
  sys.stdout = sys.__stdout__
  assert len(capturedOutput.getvalue()) > 5
  
@pytest.mark.skip(reason="not implemented yet") 
def test_get_meta_data(data_service: DataService):
  meta_data = data_service.get_meta_data()
  meta_data_keys = ['_key', 'unique_label', 'name', 'description', 'meta', 'created_by', 'update_policy', 'source', 'data_item_collections', 'store_version', 'data_version', 'data_updated_at', 'data', 'created_at', 'updated_at']
  for k in meta_data_keys:
    assert k in meta_data.keys() , f'{k} IS MISSING IN METADATA'
