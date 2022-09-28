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
import json
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
    meta_data_keys = [
        "_key",
        "unique_label",
        "name",
        "description",
        "meta",
        "created_by",
        "update_policy",
        "source",
        "data_item_collections",
        "store_version",
        "data_version",
        "data_updated_at",
        "data",
        "created_at",
        "updated_at",
    ]
    for k in meta_data_keys:
        assert k in meta_data.keys(), f"{k} IS MISSING IN METADATA"


@pytest.mark.skip(reason="no way of currently testing this")
def test_update_metadata(data_stream: DataStream):
    raise NotImplementedError


@pytest.mark.skip(reason="no way of currently testing this")
def test_set_source(data_stream: DataStream):
    raise NotImplementedError


def test_update(data_stream_editable: DataStream):
    ds_df_current = data_stream_editable.as_dataframe(live=True)

    df = pd.DataFrame(np.random.randint(0, 10000, size=(100, 4)), columns=list("ABCD"))
    data = df.to_dict("records")
    result = data_stream_editable.update(data=data)

    ds_df_local_updated = pd.concat([ds_df_current, df])
    ds_df_local_updated.sort_values(by=list("ABCD"), inplace=True)
    ds_df_local_updated = ds_df_local_updated[list("ABCD")]
    ds_df_local_updated.reset_index(drop=True, inplace=True)

    ds_df_updated = data_stream_editable.as_dataframe(live=True)
    ds_df_updated.sort_values(by=list("ABCD"), inplace=True)
    ds_df_updated = ds_df_updated[list("ABCD")]
    ds_df_updated.reset_index(drop=True, inplace=True)

    pd.testing.assert_frame_equal(
        ds_df_updated,
        ds_df_local_updated,
        check_dtype=False,
        check_index_type=False,
        check_column_type=False,
    )

    assert result["success"] == True


def test_update_in_chunks(data_stream_editable: DataStream):
    chunk_size = 12
    ds_df_current = data_stream_editable.as_dataframe(live=True)
    start_data_version = len(ds_df_current["fb_data_version"].unique().tolist())

    df = pd.DataFrame(np.random.randint(0, 10000, size=(100, 4)), columns=list("ABCD"))
    data = df.to_dict("records")
    result = data_stream_editable.update(data=data, chunk=True, chunk_size=chunk_size)

    ds_df_local_updated = pd.concat([ds_df_current, df])
    ds_df_local_updated.sort_values(by=list("ABCD"), inplace=True)
    ds_df_local_updated = ds_df_local_updated[list("ABCD")]
    ds_df_local_updated.reset_index(drop=True, inplace=True)

    ds_df_updated = data_stream_editable.as_dataframe(live=True)
    data_versions = ds_df_updated["fb_data_version"]
    ds_df_updated.sort_values(by=list("ABCD"), inplace=True)
    ds_df_updated = ds_df_updated[list("ABCD")]
    ds_df_updated.reset_index(drop=True, inplace=True)

    pd.testing.assert_frame_equal(
        ds_df_updated,
        ds_df_local_updated,
        check_dtype=False,
        check_index_type=False,
        check_column_type=False,
    )

    assert (
        len(data_versions.unique()) == chunk_size + start_data_version
    ), "DATA_VERSION_MISMATCH_DATA_STREAM_UPDATE_IN_CHUNKS"

    assert result["success"] == True


def test_replace(data_stream_editable: DataStream):
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    data = df.to_dict("records")
    result = data_stream_editable.replace(data=data, sanity_check=False)
    assert result["success"] == True


def test_replace_with_chunks(data_stream_editable: DataStream):
    chunk_size = 13
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    df.sort_values(by=list("ABCD"), inplace=True)
    df.reset_index(drop=True, inplace=True)

    data = df.to_dict("records")
    result = data_stream_editable.replace(
        data=data, sanity_check=False, chunk=True, chunk_size=chunk_size
    )
    assert result["success"] == True

    # Get DataFrame and check if it is equal to the base
    # Needs to be live since it was just updated
    ds_df = data_stream_editable.as_dataframe(live=True)
    data_versions = ds_df["fb_data_version"]
    ds_df = ds_df[df.columns]

    # Need to be sorted since on push and on pull the order is more or less random
    ds_df.sort_values(by=list("ABCD"), inplace=True)
    ds_df.reset_index(drop=True, inplace=True)

    # Necessary due to pandas type guessing
    # DataStream will return int64 instead of int32
    for c in ds_df.columns.tolist():
        ds_df[c] = ds_df[c].astype("int32")

    # Check if data is correct
    pd.testing.assert_frame_equal(
        ds_df, df, check_dtype=False, check_index_type=False, check_column_type=False
    )

    # Check if number of data version is correct
    assert (
        len(data_versions.unique().tolist()) == chunk_size
    ), "DATA_VERSION_COUNT_DOES_NOT_MATCH"


def test_get_data(data_stream: DataStream):
    data = data_stream.get_data()
    assert len(data) >= 5


def test_get_data_as_json_files(data_stream: DataStream):
    tmp_dir = PurePath(
        Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()),
        "fusionbase-test-xxx",
    )

    if Path(tmp_dir).exists():
        shutil.rmtree(tmp_dir)

    # Ensure that tmp/cache directory exists
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    storage_path = Path(tmp_dir)
    data_stream.get_data(
        result_type=ResultType.JSON_FILES, storage_path=storage_path, live=True
    )        

    data_stream_path = storage_path.joinpath(data_stream.key).joinpath("data")
    data_stream_path = data_stream_path.resolve()

    data = []
    for _file in glob.glob(str(data_stream_path) + os.sep + "*.json"):
        with open(_file, "r") as fp:
            data.extend(json.load(fp))
            fp.close()
        # data.extend(pd.read_json(_file, orient="records").to_dict(orient="records"))
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
        Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()),
        "fusionbase-test-xxx",
    )

    if Path(tmp_dir).exists():
        shutil.rmtree(tmp_dir)

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
        Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()),
        "fusionbase-test-xxx",
    )

    if Path(tmp_dir).exists():
        shutil.rmtree(tmp_dir)

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

    pd.testing.assert_frame_equal(
        df_2, df, check_dtype=False, check_index_type=False, check_column_type=False
    )


def test_get_dataframe_skip_limit(data_stream: DataStream):
    df = data_stream.get_dataframe(skip=3000, limit=10, live=True)
    assert len(df) == 10


def test_get_delta_data(data_stream: DataStream):
    delta_version = "87bd0300-08d6-4aee-9a95-348d2356fa06"
    delta_data = data_stream.get_delta_data(delta_version)
    assert len(delta_data) >= 5


def test_get_delta_dataframe(data_stream: DataStream):
    delta_version = "87bd0300-08d6-4aee-9a95-348d2356fa06"
    df = data_stream.get_delta_dataframe(delta_version)
    assert len(df) >= 5


def test_create_stream(fusionbase: Fusionbase):
    unique_label = (
        f"python_package_test_stream__{str(binascii.b2a_hex(os.urandom(10)))}"
    )
    df = pd.read_csv("./tests/data/oktoberfest_beer.csv")
    df.sort_values(by=df.columns.tolist(), inplace=True)
    df.reset_index(drop=True, inplace=True)

    result_stream = fusionbase.create_stream(
        unique_label=unique_label,
        name={"en": f"OKTOBER_FEST_{unique_label.upper()}"},
        description={
            "en": "A TEST STREAM TO TEST THE CREATE STREAM FUNCTION OF THE PYTHON PACKAGE"
        },
        scope="PUBLIC",
        source={
            "_id": "data_sources/17255624",
            "stream_specific": {"uri": "https://fusionbase.com"},
        },
        data=df,
    )

    assert isinstance(
        result_stream, DataStream
    ), "RESULT OF UPDATE CREATE MUST BE A DATASTREAM"

    ds_df = result_stream.as_dataframe(live=True)
    data_version_length = len(ds_df["fb_data_version"].unique().tolist())
    ds_df = ds_df[df.columns]
    ds_df.sort_values(by=df.columns.tolist(), inplace=True)
    ds_df.reset_index(drop=True, inplace=True)

    pd.testing.assert_frame_equal(
        ds_df, df, check_dtype=False, check_index_type=False, check_column_type=False
    )
    assert (
        data_version_length == 1
    ), "DATA VERSION SIZE MISMATCH IN CREATE WITHOUT CHUNKS"


def test_create_stream_chunk_10(fusionbase: Fusionbase):
    chunk_size = 11
    unique_label = (
        f"python_package_test_stream__{str(binascii.b2a_hex(os.urandom(10)))}"
    )
    df = pd.read_csv("./tests/data/oktoberfest_beer.csv")
    df.sort_values(by=df.columns.tolist(), inplace=True)
    df.reset_index(drop=True, inplace=True)

    result_stream = fusionbase.create_stream(
        unique_label=unique_label,
        name={"en": f"OKTOBER_FEST_{unique_label.upper()}"},
        description={
            "en": "A TEST STREAM TO TEST THE CREATE STREAM FUNCTION OF THE PYTHON PACKAGE"
        },
        scope="PUBLIC",
        source={
            "_id": "data_sources/17255624",
            "stream_specific": {"uri": "https://fusionbase.com"},
        },
        data=df,
        chunk=True,
        chunk_size=chunk_size,
    )
    assert isinstance(
        result_stream, DataStream
    ), "RESULT OF UPDATE CREATE MUST BE A DATASTREAM"

    ds_df = result_stream.as_dataframe(live=True)
    data_version_length = len(ds_df["fb_data_version"].unique().tolist())
    ds_df = ds_df[df.columns]
    ds_df.sort_values(by=df.columns.tolist(), inplace=True)
    ds_df.reset_index(drop=True, inplace=True)

    pd.testing.assert_frame_equal(
        ds_df, df, check_dtype=False, check_index_type=False, check_column_type=False
    )
    assert (
        data_version_length == chunk_size
    ), "DATA VERSION SIZE MISMATCH IN CREATE WITH CHUNKS"
