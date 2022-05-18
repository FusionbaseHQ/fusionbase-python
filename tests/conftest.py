import os
import pytest

from fusionbase.Fusionbase import Fusionbase
from dotenv import load_dotenv

load_dotenv()


def pytest_configure():
    #Streams
    # Key of a generic Stream used for all pulling related functions get_data(), get_meta_data() etc.
    pytest.generic_stream_key = os.getenv("GENERIC_STREAM_KEY")
    # Key of a Stream to try updating data, replacing data etc.
    pytest.editable_stream_label = os.getenv("EDITABLE_STREAM_LABEL")
    
    # Services
    pytest.generic_service_key = os.getenv("GENERIC_SERVICE_KEY")
    

@pytest.fixture()
def fusionbase():
  auth = {"api_key": os.getenv('FUSIONBASE_API_KEY')}
  connection = {"base_uri": os.getenv('FUSIONBASE_API_URI')}
  instance = Fusionbase(auth=auth, connection=connection)
  return instance


@pytest.fixture()
def data_stream(fusionbase: Fusionbase):
  stream = fusionbase.get_datastream(pytest.generic_stream_key)
  return stream

@pytest.fixture()
def data_stream_editable(fusionbase: Fusionbase):
  stream = fusionbase.get_datastream(label = pytest.editable_stream_label)
  return stream


@pytest.fixture()
def data_service(fusionbase: Fusionbase):
  service = fusionbase.get_dataservice(pytest.generic_service_key)
  return service
