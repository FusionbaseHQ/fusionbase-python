import binascii
import os
import numpy as np
import pandas as pd
import pytest
from fusionbase import DataStream, Fusionbase
from fusionbase.constants.ResultType import ResultType
from fusionbase.exceptions.NotAuthorizedError import NotAuthorizedError
from pathlib import Path, PurePath
import io
import sys
import json
import glob
import shutil
import tempfile
import platform


def test_print_default_user(data_stream_default_user_default_access: DataStream):
    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    print(data_stream_default_user_default_access)
    sys.stdout = sys.__stdout__
    assert len(capturedOutput.getvalue()) > 5


def test_key_property(data_stream_default_user_default_access: DataStream):
    assert data_stream_default_user_default_access.key == pytest.generic_stream_key


def test_pretty_meta_data(data_stream_default_user_default_access: DataStream):
    capturedOutput = io.StringIO()
    sys.stdout = capturedOutput
    data_stream_default_user_default_access.pretty_meta_data()
    sys.stdout = sys.__stdout__
    assert len(capturedOutput.getvalue()) > 5


def test_get_meta_data(data_stream_default_user_default_access: DataStream):
    meta_data = data_stream_default_user_default_access.get_meta_data()
    meta_data_keys = [
        "_key",
        "name",
        "description",
        "meta",
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


def test_update(data_stream_default_user_default_access: DataStream):
    df = pd.DataFrame(np.random.randint(0, 10000, size=(100, 4)), columns=list("ABCD"))
    data = df.to_dict("records")

    with pytest.raises(NotAuthorizedError):
        data_stream_default_user_default_access.update(data=data)


def test_update_in_chunks(data_stream_default_user_default_access: DataStream):
    chunk_size = 12
    df = pd.DataFrame(np.random.randint(0, 10000, size=(100, 4)), columns=list("ABCD"))
    data = df.to_dict("records")

    with pytest.raises(NotAuthorizedError):
        data_stream_default_user_default_access.update(
            data=data, chunk=True, chunk_size=chunk_size
        )


def test_replace(data_stream_default_user_default_access: DataStream):
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    data = df.to_dict("records")

    with pytest.raises(NotAuthorizedError):
        data_stream_default_user_default_access.replace(data=data, sanity_check=False)


def test_replace_with_chunks(data_stream_default_user_default_access: DataStream):
    chunk_size = 13
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    df.sort_values(by=list("ABCD"), inplace=True)
    df.reset_index(drop=True, inplace=True)
    data = df.to_dict("records")

    with pytest.raises(NotAuthorizedError):
        data_stream_default_user_default_access.replace(
            data=data, sanity_check=False, chunk=True, chunk_size=chunk_size
        )


def test_get_data(data_stream_default_user_default_access: DataStream):
    data = data_stream_default_user_default_access.get_data(live=True)
    df = pd.DataFrame(data)
    as_df = data_stream_default_user_default_access.as_dataframe(live=True)

    assert len(df) == 10, "LENGTH MISMATCH"
    pd.testing.assert_frame_equal(
        df, as_df, check_dtype=False, check_index_type=False, check_column_type=False
    )


def test_get_data_as_json_files(data_stream_default_user_default_access: DataStream):
    tmp_dir = PurePath(
        Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()),
        "fusionbase-test-xxx",
    )

    if Path(tmp_dir).exists():
        shutil.rmtree(tmp_dir)

    # Ensure that tmp/cache directory exists
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    storage_path = Path(tmp_dir)
    data_stream_default_user_default_access.get_data(
        result_type=ResultType.JSON_FILES, storage_path=storage_path, live=True
    )

    data_stream_path = storage_path.joinpath(
        data_stream_default_user_default_access.key
    ).joinpath("data")
    data_stream_path = data_stream_path.resolve()

    data = []
    for _file in glob.glob(str(data_stream_path) + os.sep + "*.json"):
        with open(_file, "r") as fp:
            data.extend(json.load(fp))
            fp.close()
        # data.extend(pd.read_json(_file, orient="records").to_dict(orient="records"))
    df = pd.DataFrame(data)
    df.sort_values(by="fb_id", inplace=True)

    df_2 = data_stream_default_user_default_access.as_dataframe()
    df_2.sort_values(by="fb_id", inplace=True)

    schema_df = df.columns.tolist()
    schema_df.sort()

    schema_df_2 = df_2.columns.tolist()
    schema_df_2.sort()

    assert schema_df == schema_df_2
    assert len(df) == len(df_2)

    shutil.rmtree(tmp_dir)
    assert Path(tmp_dir).exists() == False


def test_get_data_as_csv_files(data_stream_default_user_default_access: DataStream):
    tmp_dir = PurePath(
        Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()),
        "fusionbase-test-xxx",
    )

    if Path(tmp_dir).exists():
        shutil.rmtree(tmp_dir)

    # Ensure that tmp/cache directory exists
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    storage_path = Path(tmp_dir)
    data_stream_default_user_default_access.get_data(
        result_type=ResultType.CSV_FILES, storage_path=storage_path
    )

    data_stream_path = storage_path.joinpath(
        data_stream_default_user_default_access.key
    ).joinpath("data")
    data_stream_path = data_stream_path.resolve()

    data = []
    for _file in glob.glob(str(data_stream_path) + os.sep + "*.csv"):
        data.extend(pd.read_csv(_file).to_dict(orient="records"))
    df = pd.DataFrame(data)
    df.sort_values(by="fb_id", inplace=True)

    df_2 = data_stream_default_user_default_access.as_dataframe()
    df_2.sort_values(by="fb_id", inplace=True)

    schema_df = df.columns.tolist()
    schema_df.sort()

    schema_df_2 = df_2.columns.tolist()
    schema_df_2.sort()

    assert schema_df == schema_df_2
    assert len(df) == len(df_2)

    shutil.rmtree(tmp_dir)
    assert Path(tmp_dir).exists() == False


def test_get_data_as_pickle_files(data_stream_default_user_default_access: DataStream):
    tmp_dir = PurePath(
        Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()),
        "fusionbase-test-xxx",
    )

    if Path(tmp_dir).exists():
        shutil.rmtree(tmp_dir)

    # Ensure that tmp/cache directory exists
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    storage_path = Path(tmp_dir)
    data_stream_default_user_default_access.get_data(
        result_type=ResultType.PICKLE_FILES, storage_path=storage_path
    )

    data_stream_path = storage_path.joinpath(
        data_stream_default_user_default_access.key
    ).joinpath("data")
    data_stream_path = data_stream_path.resolve()

    data = []
    for _file in glob.glob(str(data_stream_path) + os.sep + "*.pkl"):
        # pd.read_pickle just loads to pickled object, which is a Python list in our case
        # Therefore a DataFrame needs to be created explicitly
        data.extend(pd.DataFrame(pd.read_pickle(_file)).to_dict(orient="records"))
    df = pd.DataFrame(data)
    df.sort_values(by="fb_id", inplace=True)

    df_2 = data_stream_default_user_default_access.as_dataframe()
    df_2.sort_values(by="fb_id", inplace=True)

    schema_df = df.columns.tolist()
    schema_df.sort()

    schema_df_2 = df_2.columns.tolist()
    schema_df_2.sort()

    assert schema_df == schema_df_2
    assert len(df) == len(df_2)

    shutil.rmtree(tmp_dir)
    assert Path(tmp_dir).exists() == False


def test_get_dataframe(data_stream_default_user_default_access: DataStream):
    df = data_stream_default_user_default_access.get_dataframe(live=True)
    meta_data = data_stream_default_user_default_access.get_meta_data()
    assert len(df) == 10


def test_get_dataframe_as_dataframe(
    data_stream_default_user_default_access: DataStream,
):
    df = data_stream_default_user_default_access.as_dataframe(live=True)
    df_2 = data_stream_default_user_default_access.get_dataframe(live=True)
    meta_data = data_stream_default_user_default_access.get_meta_data()
    assert len(df) == 10
    assert len(df) == len(df_2)

    pd.testing.assert_frame_equal(
        df_2, df, check_dtype=False, check_index_type=False, check_column_type=False
    )


def test_get_dataframe_skip_limit(data_stream_default_user_default_access: DataStream):
    df = data_stream_default_user_default_access.get_dataframe(
        skip=3000, limit=10, live=True
    )
    assert len(df) == 10


def test_get_delta_data(data_stream_default_user_full_access: DataStream):
    delta_version = "24df1435-3dff-4f33-a820-4d0b0693b843"
    delta_data = data_stream_default_user_full_access.get_delta_data(delta_version)
    assert len(delta_data) >= 5


def test_get_delta_dataframe(data_stream_default_user_full_access: DataStream):
    delta_version = "24df1435-3dff-4f33-a820-4d0b0693b843"
    df = data_stream_default_user_full_access.get_delta_dataframe(delta_version)
    assert len(df) >= 5


def test_create_stream(fusionbase_default_user: Fusionbase):
    unique_label = (
        f"python_package_test_stream__{str(binascii.b2a_hex(os.urandom(10)))}"
    )
    df = pd.read_csv("./tests/data/oktoberfest_beer.csv")
    df.sort_values(by=df.columns.tolist(), inplace=True)
    df.reset_index(drop=True, inplace=True)

    with pytest.raises(NotAuthorizedError):
        fusionbase_default_user.create_stream(
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


def test_create_stream_chunk_10(fusionbase_default_user: Fusionbase):
    chunk_size = 11
    unique_label = (
        f"python_package_test_stream__{str(binascii.b2a_hex(os.urandom(10)))}"
    )
    df = pd.read_csv("./tests/data/oktoberfest_beer.csv")
    df.sort_values(by=df.columns.tolist(), inplace=True)
    df.reset_index(drop=True, inplace=True)

    with pytest.raises(NotAuthorizedError):
        fusionbase_default_user.create_stream(
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
