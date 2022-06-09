import os
import pytest

from fusionbase.DataStream import DataStream
from fusionbase.DataService import DataService
from dotenv import load_dotenv

load_dotenv()


def pytest_configure():
    #Streams
    # Key of a generic stream which used for all pulling related functions get_data(), get_meta_data() etc.
    pytest.generic_stream_key = os.getenv("GENERIC_STREAM_KEY")
    # Key of a stream updating data, replacing data etc.
    pytest.editable_stream_label = os.getenv("EDITABLE_STREAM_LABEL")
    
    # Services
    pytest.generic_service_key = os.getenv("GENERIC_SERVICE_KEY")


@pytest.fixture()
def data_stream():
  auth = {"api_key": os.getenv('FUSIONBASE_API_KEY')}
  connection = {"base_uri": os.getenv('FUSIONBASE_API_URI')}
  stream = DataStream(auth=auth, connection=connection, log=True)
  return stream

@pytest.fixture()
def data_stream_editable():
  auth = {"api_key": os.getenv('FUSIONBASE_API_KEY')}
  connection = {"base_uri": os.getenv('FUSIONBASE_API_URI')}
  stream = DataStream(auth=auth, connection=connection)
  return stream


@pytest.fixture()
def data_service():
  auth = {"api_key": os.getenv('FUSIONBASE_API_KEY')}
  connection = {"base_uri": os.getenv('FUSIONBASE_API_URI')}
  service = DataService(auth=auth, connection=connection)
  return service
