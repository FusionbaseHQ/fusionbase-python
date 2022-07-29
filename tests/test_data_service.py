import pytest
from fusionbase.Fusionbase import DataService
import io
import sys

# @pytest.mark.skip(reason="not implemented yet")


def test_print(data_service: DataService):
    captured_output = io.StringIO()
    sys.stdout = captured_output
    print(data_service)
    sys.stdout = sys.__stdout__
    assert len(captured_output.getvalue()) > 5


def test_key_property(data_service: DataService):
    assert data_service.key == pytest.generic_service_key


def test_pretty_meta_data(data_service: DataService):
    captured_output = io.StringIO()
    sys.stdout = captured_output
    data_service.pretty_meta_data()
    sys.stdout = sys.__stdout__
    assert len(captured_output.getvalue()) > 5


def test_get_meta_data(data_service: DataService):
    meta_data = data_service.get_meta_data()
    meta_data_keys = [
        "_id",
        "_key",
        "name",
        "description",
        "meta",
        "provision",
        "scope",
        "request_definition",
        "source",
        "deleted",
        "deleted_at",
        "created_by",
        "created_at",
        "updated_at",
    ]
    for k in meta_data_keys:
        assert k in meta_data.keys(), f"{k} IS MISSING IN METADATA"


def test_pretty_request_definition(data_service: DataService):
    captured_output = io.StringIO()
    sys.stdout = captured_output
    data_service.pretty_request_definition()
    sys.stdout = sys.__stdout__
    assert len(captured_output.getvalue()) > 5


def test_get_request_definition(data_service: DataService):
    request_definition = data_service.get_request_definition()
    request_definition_keys = ["parameters"]
    for k in request_definition_keys:
        assert k in request_definition.keys(), f"{k} IS MISSING IN REQUEST DEFINITION"


def test_invoke_missing(data_service: DataService):
    with pytest.raises(Exception):
        data_service.invoke()


def test_invoke_valid_kwargs(data_service: DataService):
    result = data_service.invoke(q="Fusionbase GmbH")
    result_keys = [
        "@context",
        "@type",
        "fb_entity_id",
        "lei_code",
        "legal_name",
        "legal_form",
        "address",
        "registration_data",
        "founding_date",
        "members",
        "make_offers",
        "alternate_names",
    ]
    for key in result_keys:
        assert (
            key in result[0].keys()
        ), f'DIFFERENT RESPONSE EXPECTED FOR VALID INPUT: "Fusionbase GmbH"'


def test_invoke_valid(data_service: DataService):
    parameters = [{"name": "q", "value": "Fusionbase GmbH"}]
    result = data_service.invoke(parameters=parameters)
    result_keys = [
        "@context",
        "@type",
        "fb_entity_id",
        "lei_code",
        "legal_name",
        "legal_form",
        "address",
        "registration_data",
        "founding_date",
        "members",
        "make_offers",
        "alternate_names",
    ]

    for key in result_keys:
        assert (
            key in result[0].keys()
        ), f"DIFFERENT RESPONSE EXPECTED FOR VALID INPUT {parameters}"


def test_invoke_invalid(data_service: DataService):
    parameters = [{"name": "NotAParameter", "value": "Fusionbase GmbH"}]
    with pytest.raises(Exception):
        data_service.invoke(parameters=parameters)


def test_invoke_invalid_kwargs(data_service: DataService):
    with pytest.raises(Exception):
        data_service.invoke(qd="Fusionbase GmbH")


def test_apply_service(data_service: DataService):
    import pandas as pd

    df = pd.DataFrame([{"company": "Fusionbase GmbH"}])
    enriched_df = data_service.apply(df, [("q", "company")])
    assert "fusionbase_result" in enriched_df.columns.tolist()
    assert len(df) == len(enriched_df)
    assert len(df.columns) + 1 == len(enriched_df.columns)


def test_apply_service_with_callback(data_service: DataService):
    import pandas as pd

    df = pd.DataFrame([{"company": "Fusionbase GmbH"}])

    def cb(series, api_result):
        series["legal_name"] = api_result[0]["legal_name"]
        series["context"] = api_result[0]["@context"]
        return series

    enriched_df = data_service.apply(df, [("q", "company")], callback=cb)

    assert "legal_name" in enriched_df.columns.tolist()
    assert "context" in enriched_df.columns.tolist()
    assert len(df) == len(enriched_df)
    assert len(df.columns) + 2 == len(enriched_df.columns)
