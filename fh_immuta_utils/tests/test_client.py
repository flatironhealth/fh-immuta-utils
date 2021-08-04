from unittest.mock import patch, Mock

import pytest
from requests import Response

from fh_immuta_utils.client import ImmutaClient


MAKE_GLOB_REQUEST_HEADERS_TESTS = {
    "make_glob_request_headers_postgresql": (
        {
            "handler_type": "PostgreSQL",
            "hostname": "psql.foobar.io",
            "port": 443,
            "username": "foo",
            "password": "bar",
        },
        {
            "sql-hostname": "psql.foobar.io",
            "sql-port": "443",
            "sql-ssl": "true",
            "sql-username": "foo",
            "sql-password": "bar",
        },
    ),
    "make_glob_request_headers_athena": (
        {
            "handler_type": "Amazon Athena",
            "region": "us-east-1",
            "queryResultLocationBucket": "s3_location",
            "username": "foo",
            "password": "bar",
        },
        {
            "sql-authentication-type": "accessKey",
            "sql-aws-region": "us-east-1",
            "sql-aws-result-location": "s3_location",
            "sql-ssl": "true",
            "sql-username": "foo",
            "sql-password": "bar",
        },
    ),
    "make_glob_request_headers_snowflake": (
        {
            "handler_type": "Snowflake",
            "hostname": "psql.foobar.io",
            "port": 443,
            "username": "foo",
            "password": "bar",
            "warehouse": "IMMUTA_WH",
        },
        {
            "sql-hostname": "psql.foobar.io",
            "sql-port": "443",
            "sql-ssl": "true",
            "sql-username": "foo",
            "sql-password": "bar",
            "sql-warehouse": "IMMUTA_WH",
        },
    ),
}


@pytest.mark.parametrize(
    "config, expected",
    list(MAKE_GLOB_REQUEST_HEADERS_TESTS.values()),
    ids=list(MAKE_GLOB_REQUEST_HEADERS_TESTS.keys()),
)
@patch("fh_immuta_utils.client.ImmutaSession")
def test_skip_dataset_enrollment(mock_session, config, expected):
    client = ImmutaClient(session=mock_session)
    assert client.make_glob_request_headers(config) == expected


@patch("fh_immuta_utils.client.ImmutaSession")
def test_delete_data_source_no_id_no_name(mock_session):
    client = ImmutaClient(session=mock_session)
    id = None
    name = None
    with pytest.raises(Exception):
        client.delete_data_source(id=id, name=name)


@patch("fh_immuta_utils.client.ImmutaSession")
def test_delete_data_source_already_disabled(mock_session):
    client = ImmutaClient(session=mock_session)
    id = 1
    name = None
    client.delete = Mock()
    resp = Mock(spec=Response)
    resp.json.return_value = {"hardDelete": True}
    client.delete.return_value = resp
    client.delete_data_source(id=id, name=name)
    client.delete.assert_called_once()


GET_REMOTE_DATABASE_TEST_RESPONSE_TESTS = {
    "make_glob_request_headers_postgresql": (
        {
            "handler_type": "PostgreSQL",
            "database": "foobar",
            "hostname": "psql.foobar.io",
            "port": 443,
            "username": "foo",
            "password": "bar",
        },
        {
            "make_generic_odbc_request_headers": True,
            "make_athena_glob_request_headers": False,
        },
    ),
    "make_glob_request_headers_athena": (
        {
            "handler_type": "Amazon Athena",
            "database": "foobar",
            "region": "us-east-1",
            "queryResultLocationBucket": "s3_location",
            "username": "foo",
            "password": "bar",
        },
        {
            "make_generic_odbc_request_headers": False,
            "make_athena_glob_request_headers": True,
        },
    ),
    "make_glob_request_headers_snowflake": (
        {
            "handler_type": "Snowflake",
            "hostname": "psql.foobar.io",
            "database": "foobar",
            "port": 443,
            "username": "foo",
            "password": "bar",
            "warehouse": "IMMUTA_WH",
        },
        {
            "make_generic_odbc_request_headers": True,
            "make_athena_glob_request_headers": False,
        },
    ),
}


@pytest.mark.parametrize(
    "config, expected",
    list(GET_REMOTE_DATABASE_TEST_RESPONSE_TESTS.values()),
    ids=list(GET_REMOTE_DATABASE_TEST_RESPONSE_TESTS.keys()),
)
@patch("fh_immuta_utils.client.ImmutaSession")
@patch("fh_immuta_utils.client.ImmutaClient.make_generic_odbc_request_headers")
@patch("fh_immuta_utils.client.ImmutaClient.make_athena_glob_request_headers")
def test_get_remote_database_test_response(
    mock_make_athena_glob_request_headers,
    mock_make_generic_odbc_request_headers,
    mock_session,
    config,
    expected,
):
    client = ImmutaClient(session=mock_session)
    client.get_remote_database_test_response(config)
    if expected["make_generic_odbc_request_headers"]:
        mock_make_generic_odbc_request_headers.assert_called()
    if expected["make_athena_glob_request_headers"]:
        mock_make_athena_glob_request_headers.assert_called()
