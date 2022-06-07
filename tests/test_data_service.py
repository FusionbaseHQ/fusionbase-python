import pytest
from fusionbase.DataService import DataService
import io
import sys

# @pytest.mark.skip(reason="not implemented yet")


def test_print(data_service: DataService):
  captured_output = io.StringIO()
  sys.stdout = captured_output
  print(data_service)
  sys.stdout = sys.__stdout__
  assert len(captured_output.getvalue()) > 5


def test_pretty_meta_data(data_service: DataService):
  captured_output = io.StringIO()
  sys.stdout = captured_output
  data_service.pretty_meta_data(key=pytest.generic_service_key)
  sys.stdout = sys.__stdout__
  assert len(captured_output.getvalue()) > 5


def test_get_meta_data(data_service: DataService):
  meta_data = data_service.get_meta_data(key=pytest.generic_service_key)
  meta_data_keys = ['_id', '_key', 'name', 'description', 'meta', 'provision', 'scope',
                    'request_definition', 'source', 'deleted', 'deleted_at', 'created_by', 'created_at', 'updated_at']
  for k in meta_data_keys:
    assert k in meta_data.keys(), f'{k} IS MISSING IN METADATA'


def test_pretty_request_definition(data_service: DataService):
  captured_output = io.StringIO()
  sys.stdout = captured_output
  data_service.pretty_request_definition(key=pytest.generic_service_key)
  sys.stdout = sys.__stdout__
  assert len(captured_output.getvalue()) > 5


def test_request_definition(data_service: DataService):
  request_definition = data_service.request_definition(key=pytest.generic_service_key)
  request_definition_keys = ['parameters']
  for k in request_definition_keys:
    assert k in request_definition.keys(
    ), f'{k} IS MISSING IN REQUEST DEFINITION'

def test_invoke_missing(data_service: DataService):
  with pytest.raises(Exception):
      data_service.invoke(key=pytest.generic_service_key)
    
def test_invoke_valid(data_service: DataService):
  parameters = [
      {'name': 'q',
       'value': 'Fusionbase GmbH'}
  ]
  result = data_service.invoke(key=pytest.generic_service_key, parameters=parameters)
  result_keys = ['@context', '@type', 'fb_entity_id', 'lei_code', 'legal_name', 'legal_form', 'address', 'registration_data', 'founding_date', 'members', 'make_offers', 'alternate_names']
  
  for key in result_keys:
    assert key in result[0].keys(), f'DIFFERENT RESPONSE EXPECTED FOR VALID INPUT {parameters}'

    
def test_invoke_invalid(data_service: DataService):
  parameters = [
      {'name': 'NotAParameter',
       'value': 'Fusionbase GmbH'}
  ]
  with pytest.raises(Exception):
      data_service.invoke(key=pytest.generic_service_key, parameters=parameters)
    