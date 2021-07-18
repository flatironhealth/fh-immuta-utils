from unittest.mock import patch

import pytest

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
