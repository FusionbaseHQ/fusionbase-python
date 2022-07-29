import os
import pytest

from fusionbase import Fusionbase
from dotenv import load_dotenv

load_dotenv()


def pytest_configure():
    # Streams
    # Key of a generic Stream used for all pulling related functions get_data(), get_meta_data() etc.
    pytest.generic_stream_key = os.getenv("GENERIC_STREAM_KEY")
    # Key of a Stream to try updating data, replacing data etc.
    pytest.editable_stream_label = os.getenv("EDITABLE_STREAM_LABEL")
    # Stream to which a user with default roles has access to
    pytest.generic_stream_default_user_full_access = os.getenv(
        "GENERIC_STREAM_KEY_DEFAULT_USER_FULL_ACCESS"
    )

    # Services
    pytest.generic_service_key = os.getenv("GENERIC_SERVICE_KEY")


@pytest.fixture()
def fusionbase():
    auth = {"api_key": os.getenv("FUSIONBASE_API_KEY")}
    connection = {"base_uri": os.getenv("FUSIONBASE_API_URI")}
    instance = Fusionbase(auth=auth, connection=connection)
    return instance


@pytest.fixture()
def fusionbase_default_user():
    auth = {"api_key": os.getenv("FUSIONBASE_API_KEY_GENERIC_USER")}
    connection = {"base_uri": os.getenv("FUSIONBASE_API_URI")}
    instance = Fusionbase(auth=auth, connection=connection)
    return instance


@pytest.fixture()
def data_stream(fusionbase: Fusionbase):
    stream = fusionbase.get_datastream(pytest.generic_stream_key)
    return stream


@pytest.fixture()
def data_stream_editable(fusionbase: Fusionbase):
    stream = fusionbase.get_datastream(label=pytest.editable_stream_label)
    return stream


@pytest.fixture()
def data_service(fusionbase: Fusionbase):
    service = fusionbase.get_dataservice(pytest.generic_service_key)
    return service


@pytest.fixture()
def data_stream_default_user_default_access(fusionbase_default_user: Fusionbase):
    stream = fusionbase_default_user.get_datastream(pytest.generic_stream_key)
    return stream


@pytest.fixture()
def data_stream_default_user_full_access(fusionbase_default_user: Fusionbase):
    stream = fusionbase_default_user.get_datastream(
        pytest.generic_stream_default_user_full_access
    )
    return stream


@pytest.fixture()
def data_stream_editable_default_user(fusionbase_default_user: Fusionbase):
    stream = fusionbase_default_user.get_datastream(label=pytest.editable_stream_label)
    return stream


@pytest.fixture()
def data_service_default_user(fusionbase_default_user: Fusionbase):
    service = fusionbase_default_user.get_dataservice(pytest.generic_service_key)
    return service
