import binascii
import os
import numpy as np
import pandas as pd
import pytest
from fusionbase import DataStream, Fusionbase
from fusionbase.constants.ResultType import ResultType
from pathlib import Path, PurePath
import io
import sys
import glob
import shutil
import tempfile
import platform

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

def test_get_data_as_json_files(data_stream: DataStream):
  tmp_dir = PurePath(
      Path(
          "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()
      ),
      "fusionbase-test-xxx",
  )
  # Ensure that tmp/cache directory exists
  Path(tmp_dir).mkdir(parents=True, exist_ok=True)

  storage_path = Path(tmp_dir)
  data_stream.get_data(result_type=ResultType.JSON_FILES, storage_path=storage_path)

  data_stream_path = storage_path.joinpath(data_stream.key).joinpath("data")
  data_stream_path = data_stream_path.resolve()

  data = []
  for _file in glob.glob(str(data_stream_path) + os.sep + "*.json"):
    data.extend(pd.read_json(_file, orient="records").to_dict(orient="records"))
  df = pd.DataFrame(data)
  df.sort_values(by="fb_id", inplace=True)

  df_2 = data_stream.as_dataframe()
  df_2.sort_values(by="fb_id", inplace=True)

  schema_df = df.columns.tolist()
  schema_df.sort()

  schema_df_2 = df_2.columns.tolist()
  schema_df_2.sort()

  assert schema_df == schema_df_2
  assert len(df) == len(df_2)

  shutil.rmtree(tmp_dir)
  assert Path(tmp_dir).exists() == False

def test_get_data_as_csv_files(data_stream: DataStream):
  tmp_dir = PurePath(
      Path(
          "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()
      ),
      "fusionbase-test-xxx",
  )
  # Ensure that tmp/cache directory exists
  Path(tmp_dir).mkdir(parents=True, exist_ok=True)

  storage_path = Path(tmp_dir)
  data_stream.get_data(result_type=ResultType.CSV_FILES, storage_path=storage_path)

  data_stream_path = storage_path.joinpath(data_stream.key).joinpath("data")
  data_stream_path = data_stream_path.resolve()

  data = []
  for _file in glob.glob(str(data_stream_path) + os.sep + "*.csv"):
    data.extend(pd.read_csv(_file).to_dict(orient="records"))
  df = pd.DataFrame(data)
  df.sort_values(by="fb_id", inplace=True)

  df_2 = data_stream.as_dataframe()
  df_2.sort_values(by="fb_id", inplace=True)

  schema_df = df.columns.tolist()
  schema_df.sort()

  schema_df_2 = df_2.columns.tolist()
  schema_df_2.sort()

  assert schema_df == schema_df_2
  assert len(df) == len(df_2)

  shutil.rmtree(tmp_dir)
  assert Path(tmp_dir).exists() == False

def test_get_data_as_pickle_files(data_stream: DataStream):
  tmp_dir = PurePath(
      Path(
          "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()
      ),
      "fusionbase-test-xxx",
  )
  # Ensure that tmp/cache directory exists
  Path(tmp_dir).mkdir(parents=True, exist_ok=True)

  storage_path = Path(tmp_dir)
  data_stream.get_data(result_type=ResultType.PICKLE_FILES, storage_path=storage_path)

  data_stream_path = storage_path.joinpath(data_stream.key).joinpath("data")
  data_stream_path = data_stream_path.resolve()

  data = []
  for _file in glob.glob(str(data_stream_path) + os.sep + "*.pkl"):
    # pd.read_pickle just loads to pickled object, which is a Python list in our case
    # Therefore a DataFrame needs to be created explicitly
    data.extend(pd.DataFrame(pd.read_pickle(_file)).to_dict(orient="records"))
  df = pd.DataFrame(data)
  df.sort_values(by="fb_id", inplace=True)

  df_2 = data_stream.as_dataframe()
  df_2.sort_values(by="fb_id", inplace=True)

  schema_df = df.columns.tolist()
  schema_df.sort()

  schema_df_2 = df_2.columns.tolist()
  schema_df_2.sort()

  assert schema_df == schema_df_2
  assert len(df) == len(df_2)

  shutil.rmtree(tmp_dir)
  assert Path(tmp_dir).exists() == False

def test_get_dataframe(data_stream: DataStream):
  df = data_stream.get_dataframe()
  meta_data = data_stream.get_meta_data()
  assert len(df) == meta_data["meta"]["entry_count"]

def test_get_dataframe_as_dataframe(data_stream: DataStream):
  df = data_stream.as_dataframe()
  df_2 = data_stream.get_dataframe()
  meta_data = data_stream.get_meta_data()
  assert len(df) == meta_data["meta"]["entry_count"]
  assert len(df) == len(df_2)
  assert df_2.equals(df)

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
  