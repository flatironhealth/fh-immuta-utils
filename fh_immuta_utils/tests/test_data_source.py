from collections import namedtuple
from typing import Dict, List, Any, Optional
from unittest.mock import patch

import pytest
from pydantic import ValidationError

import fh_immuta_utils.data_source as ds
from fh_immuta_utils.tagging import SKIP_STATS_JOB_TAG
from fh_immuta_utils.scripts.manage_data_sources import skip_dataset_enrollment

NameTestKeys = namedtuple(
    "NameTestKeys", ["handler_type", "schema", "table", "user_prefix", "expected_name"]
)

IMMUTA_DATASOURCE_NAME_TESTS = [
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['PostgreSQL']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table="barbazquxquux",
        user_prefix="quuz",
        expected_name=f"quuz_{ds.PREFIX_MAP['PostgreSQL']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table=f"{'a' * (ds.MAX_IMMUTA_NAME_LIMIT - 7)}",
        user_prefix="",
        expected_name=(
            f"{ds.PREFIX_MAP['PostgreSQL']}_foo_{'a' * (ds.MAX_IMMUTA_NAME_LIMIT - 7)}"
        ),
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table=f"{'a' * ds.MAX_IMMUTA_NAME_LIMIT}",
        user_prefix="",
        expected_name=(
            f"{ds.PREFIX_MAP['PostgreSQL']}_foo_{'a' * (ds.MAX_IMMUTA_NAME_LIMIT - 7)}"
        ),
    ),
    NameTestKeys(
        handler_type="Redshift",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Redshift']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="Amazon S3",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Amazon S3']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="Amazon Athena",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Amazon Athena']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="Snowflake",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Snowflake']}_foo_barbazquxquux",
    ),
]


POSTGRES_NAME_TESTS = [
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['PostgreSQL']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name="foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['PostgreSQL']}_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table="barbazquxquux",
        user_prefix="quuz",
        expected_name=f"quuz_{ds.PREFIX_MAP['PostgreSQL']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table=f"{'a'*(ds.MAX_POSTGRES_NAME_LIMIT - 7)}",
        user_prefix="",
        expected_name=(
            f"{ds.PREFIX_MAP['PostgreSQL']}_foo_{'a'*(ds.MAX_POSTGRES_NAME_LIMIT - 7)}"
        ),
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table=f"{'a'*ds.MAX_POSTGRES_NAME_LIMIT}",
        user_prefix="",
        expected_name=(
            f"{ds.PREFIX_MAP['PostgreSQL']}_foo_{'a'*(ds.MAX_POSTGRES_NAME_LIMIT - 7)}"
        ),
    ),
    NameTestKeys(
        handler_type="Redshift",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Redshift']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="Amazon S3",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Amazon S3']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="Amazon Athena",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Amazon Athena']}_foo_barbazquxquux",
    ),
    NameTestKeys(
        handler_type="Snowflake",
        schema="foo",
        table="barbazquxquux",
        user_prefix="",
        expected_name=f"{ds.PREFIX_MAP['Snowflake']}_foo_barbazquxquux",
    ),
]


@pytest.mark.parametrize(
    "handler_type,schema,table,user_prefix,expected_name", IMMUTA_DATASOURCE_NAME_TESTS
)
def test_make_immuta_datasource_name(
    handler_type: str, schema: str, table: str, user_prefix: str, expected_name: str
):
    name = ds.make_immuta_datasource_name(
        handler_type=handler_type, schema=schema, table=table, user_prefix=user_prefix
    )
    assert len(name) <= ds.MAX_IMMUTA_NAME_LIMIT
    assert (
        name[: ds.MAX_IMMUTA_NAME_LIMIT - 8]
        == expected_name[: ds.MAX_IMMUTA_NAME_LIMIT - 8]
    )


@pytest.mark.parametrize(
    "handler_type,schema,table,user_prefix,expected_name", POSTGRES_NAME_TESTS
)
def test_make_postgres_table_name(
    handler_type: str, schema: str, table: str, user_prefix: str, expected_name: str
):
    name = ds.make_postgres_table_name(
        handler_type=handler_type, schema=schema, table=table, user_prefix=user_prefix
    )
    assert len(name) <= ds.MAX_POSTGRES_NAME_LIMIT
    assert name == expected_name[: ds.MAX_POSTGRES_NAME_LIMIT]


MetadataTestKeys = namedtuple(
    "MetadataTestKeys", ["db_keys", "handler_type", "expected_type", "kwargs"]
)
HANDLER_METADATA_TESTS = [
    MetadataTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        kwargs={},
    ),
    MetadataTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        kwargs={},
    ),
    MetadataTestKeys(
        db_keys={"hostname": "qux", "warehouse": "IMMUTA_WH"},
        handler_type="Snowflake",
        expected_type=ds.SnowflakeHandlerMetadata,
        kwargs={},
    ),
    MetadataTestKeys(
        db_keys={
            "queryResultLocationBucket": "qux",
            "queryResultLocationDirectory": "quux",
            "region": "quz",
        },
        handler_type="Amazon Athena",
        expected_type=ds.AthenaHandlerMetadata,
        kwargs={},
    ),
    MetadataTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        kwargs={
            "bodataTableName": "foobar",
            "dataSourceName": "bazqux",
            "bodataSchemaName": "foo_schema",
        },
    ),
]


@pytest.mark.parametrize(
    "db_keys,handler_type,expected_type, kwargs", HANDLER_METADATA_TESTS
)
def test_make_handler_metadata(
    db_keys: Dict[str, str],
    handler_type: str,
    expected_type: Any,
    kwargs: Dict[str, str],
):
    base_config = {"username": "foo", "password": "bar", "database": "baz"}

    config = {**base_config, **db_keys}
    with pytest.raises(KeyError):
        ds.make_handler_metadata(table="foo", schema="bar", config=config, **kwargs)

    config["handler_type"] = handler_type
    # Validate for all required keys
    for k, v in {**base_config, **db_keys}.items():
        config.pop(k)
        with pytest.raises(ValidationError):
            ds.make_handler_metadata(table="foo", schema="bar", config=config, **kwargs)
        config[k] = v

    handler = ds.make_handler_metadata(
        table="foo", schema="bar", config=config, **kwargs
    )
    assert isinstance(handler, ds.Handler)
    assert isinstance(handler.metadata, expected_type)
    for k, v in {**base_config, **db_keys, **kwargs}.items():
        assert handler.metadata.dict()[k] == v


COLUMNS = [
    ds.DataSourceColumn(
        name="foo", dataType="Integer", remoteType="Integer", nullable=True
    ),
    ds.DataSourceColumn(
        name="bar", dataType="Integer", remoteType="Integer", nullable=False
    ),
    ds.DataSourceColumn(
        name="bat", dataType="Integer", remoteType="Integer", nullable=True
    ),
]

ObjectTestKeys = namedtuple(
    "ObjectTestKeys",
    [
        "db_keys",
        "handler_type",
        "expected_type",
        "columns",
        "query_engine_target_schema",
        "prefix_query_engine_names_with_schema",
        "prefix_query_engine_names_with_handler",
    ],
)
OBJECT_TESTS = [
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    ObjectTestKeys(
        db_keys={
            "queryResultLocationBucket": "qux",
            "queryResultLocationDirectory": "quux",
            "region": "quz",
        },
        handler_type="Amazon Athena",
        expected_type=ds.AthenaHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    ObjectTestKeys(
        db_keys={"hostname": "qux", "warehouse": "IMMUTA_WH"},
        handler_type="Snowflake",
        expected_type=ds.SnowflakeHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="foo_schema",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=True,
        prefix_query_engine_names_with_handler=False,
    ),
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=True,
    ),
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=True,
        prefix_query_engine_names_with_handler=True,
    ),
]


@pytest.mark.parametrize(
    "db_keys,handler_type,expected_type,columns,query_engine_target_schema,prefix_query_engine_names_with_schema,prefix_query_engine_names_with_handler",
    OBJECT_TESTS,
)
def test_to_immuta_objects(
    db_keys: Dict[str, str],
    handler_type: str,
    expected_type: Any,
    columns: List[ds.DataSourceColumn],
    query_engine_target_schema: str,
    prefix_query_engine_names_with_schema: bool,
    prefix_query_engine_names_with_handler: bool,
):
    base_config = {
        "username": "foo",
        "password": "bar",
        "database": "baz",
        "owner_profile_id": 0,
    }
    config = {**base_config, **db_keys}
    config["handler_type"] = handler_type
    source, handler, schema_evolution = ds.to_immuta_objects(
        table="foo",
        schema="bar",
        config=config,
        columns=columns,
        bodata_schema_name=query_engine_target_schema,
        prefix_query_engine_names_with_schema=prefix_query_engine_names_with_schema,
        prefix_query_engine_names_with_handler=prefix_query_engine_names_with_handler,
    )
    assert source.name == ds.make_immuta_datasource_name(
        table="foo",
        schema="bar",
        handler_type=handler_type,
        user_prefix="",
    )
    assert source.sqlTableName == ds.make_postgres_table_name(
        table="foo",
        schema="bar" if prefix_query_engine_names_with_schema else "",
        handler_type=handler_type if prefix_query_engine_names_with_handler else "",
        user_prefix="",
    )
    assert source.blobHandlerType == handler_type
    if config.get("skip_stats_job", False):
        assert SKIP_STATS_JOB_TAG.dict() in source.tags
    else:
        assert SKIP_STATS_JOB_TAG.dict() not in source.tags
    assert handler.metadata.bodataSchemaName == query_engine_target_schema
    assert isinstance(handler, ds.Handler)
    assert isinstance(handler.metadata, expected_type)
    assert isinstance(schema_evolution, ds.SchemaEvolutionMetadata)


TABLES = ["foo", "bar", "baz"]
BulkObjectTestKeys = namedtuple(
    "BulkObjectTestKeys",
    [
        "db_keys",
        "handler_type",
        "expected_type",
        "tables",
        "query_engine_target_schema",
        "prefix_query_engine_names_with_schema",
        "prefix_query_engine_names_with_handler",
    ],
)
BULK_OBJECT_TESTS = [
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        tables=[],
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux", "skip_stats_job": True},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="foo_schema",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=True,
        prefix_query_engine_names_with_handler=False,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux", "skip_stats_job": False},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=True,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=True,
        prefix_query_engine_names_with_handler=True,
    ),
    BulkObjectTestKeys(
        db_keys={
            "queryResultLocationBucket": "qux",
            "queryResultLocationDirectory": "quux",
            "region": "quz",
        },
        handler_type="Amazon Athena",
        expected_type=ds.AthenaHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux", "warehouse": "IMMUTA_WH"},
        handler_type="Snowflake",
        expected_type=ds.SnowflakeHandlerMetadata,
        tables=TABLES,
        query_engine_target_schema="",
        prefix_query_engine_names_with_schema=False,
        prefix_query_engine_names_with_handler=False,
    ),
]


@pytest.mark.parametrize(
    "db_keys,handler_type,expected_type,tables,query_engine_target_schema,prefix_query_engine_names_with_schema,prefix_query_engine_names_with_handler",
    BULK_OBJECT_TESTS,
)
def test_make_bulk_create_objects(
    db_keys: Dict[str, str],
    handler_type: str,
    expected_type: Any,
    tables: List[str],
    query_engine_target_schema: str,
    prefix_query_engine_names_with_schema: bool,
    prefix_query_engine_names_with_handler: bool,
):
    base_config = {
        "username": "foo",
        "password": "bar",
        "database": "baz",
        "owner_profile_id": 0,
    }
    config = {**base_config, **db_keys}
    config["handler_type"] = handler_type

    source, handlers, schema_evolution = ds.make_bulk_create_objects(
        tables=tables,
        schema="bar",
        config=config,
        bodata_schema_name=query_engine_target_schema,
        prefix_query_engine_names_with_schema=prefix_query_engine_names_with_schema,
        prefix_query_engine_names_with_handler=prefix_query_engine_names_with_handler,
    )
    assert len(handlers) == len(tables)
    assert source.blobHandlerType == handler_type
    if config.get("skip_stats_job", False):
        assert SKIP_STATS_JOB_TAG.dict() in source.tags
    else:
        assert SKIP_STATS_JOB_TAG.dict() not in source.tags

    table_names = []
    for handler in handlers:
        table_names.append(handler.metadata.dataSourceName)
        assert handler.metadata.bodataSchemaName == query_engine_target_schema
        assert isinstance(handler, ds.Handler)
        assert isinstance(handler.metadata, expected_type)

    for table in tables:
        assert (
            ds.make_immuta_datasource_name(
                table=table,
                schema="bar",
                handler_type=handler_type,
                user_prefix="",
            )
            in table_names
        )

    assert isinstance(schema_evolution, ds.SchemaEvolutionMetadata)


SCHEMA_EVOLUTION_METADATA_TESTS = {
    "schema_evolution_no_config": (
        {
            "owner_profile_id": 0,
            "handler_type": "Redshift",
        },
        ds.SchemaEvolutionMetadata(
            ownerProfileId=0,
            disabled=True,
            config=ds.SchemaEvolutionMetadataConfig(
                nameTemplate={
                    "dataSourceNameFormat": "rs_<schema>_<tablename>",
                    "queryEngineTableNameFormat": "<tablename>",
                    "queryEngineSchemaNameFormat": "<schema>",
                }
            ),
        ),
    ),
    "schema_evolution_no_config_change_handler": (
        {
            "owner_profile_id": 0,
            "handler_type": "Amazon Athena",
        },
        ds.SchemaEvolutionMetadata(
            ownerProfileId=0,
            disabled=True,
            config=ds.SchemaEvolutionMetadataConfig(
                nameTemplate={
                    "dataSourceNameFormat": "ath_<schema>_<tablename>",
                    "queryEngineTableNameFormat": "<tablename>",
                    "queryEngineSchemaNameFormat": "<schema>",
                }
            ),
        ),
    ),
    "schema_evolution_no_config_add_user_prefix": (
        {
            "owner_profile_id": 0,
            "handler_type": "Amazon Athena",
            "user_prefix": "foobar",
        },
        ds.SchemaEvolutionMetadata(
            ownerProfileId=0,
            disabled=True,
            config=ds.SchemaEvolutionMetadataConfig(
                nameTemplate={
                    "dataSourceNameFormat": "foobar_ath_<schema>_<tablename>",
                    "queryEngineTableNameFormat": "foobar_<tablename>",
                    "queryEngineSchemaNameFormat": "<schema>",
                }
            ),
        ),
    ),
    "schema_evolution_disabled": (
        {
            "owner_profile_id": 0,
            "handler_type": "Redshift",
            "schema_evolution": {
                "disable_schema_evolution": "true",
                "datasource_name_format": "foo",
                "query_engine_table_name_format": "bar",
                "query_engine_schema_name_format": "biz",
            },
        },
        ds.SchemaEvolutionMetadata(
            ownerProfileId=0,
            disabled=True,
            config=ds.SchemaEvolutionMetadataConfig(
                nameTemplate={
                    "dataSourceNameFormat": "foo",
                    "queryEngineTableNameFormat": "bar",
                    "queryEngineSchemaNameFormat": "biz",
                }
            ),
        ),
    ),
    "schema_evolution_enabled": (
        {
            "owner_profile_id": 0,
            "handler_type": "Redshift",
            "schema_evolution": {
                "disable_schema_evolution": "false",
                "datasource_name_format": "foo",
                "query_engine_table_name_format": "bar",
                "query_engine_schema_name_format": "biz",
            },
        },
        ds.SchemaEvolutionMetadata(
            ownerProfileId=0,
            disabled=False,
            config=ds.SchemaEvolutionMetadataConfig(
                nameTemplate={
                    "dataSourceNameFormat": "foo",
                    "queryEngineTableNameFormat": "bar",
                    "queryEngineSchemaNameFormat": "biz",
                }
            ),
        ),
    ),
}


@pytest.mark.parametrize(
    "config, expected",
    list(SCHEMA_EVOLUTION_METADATA_TESTS.values()),
    ids=list(SCHEMA_EVOLUTION_METADATA_TESTS.keys()),
)
def test_make_schema_evolution_metadata(
    config: Dict[str, Any], expected: ds.SchemaEvolutionMetadata
):
    assert isinstance(
        ds.make_schema_evolution_metadata(config), ds.SchemaEvolutionMetadata
    )
    assert ds.make_schema_evolution_metadata(config) == expected


SKIP_DATASET_ENROLLMENT_TESTS = {
    "schema_evolution_enabled_config_enabled": (
        {
            "is_schema_evolution_enabled": True,
            "schema_evolution": {
                "disable_schema_evolution": False,
            },
        },
        True,
    ),
    "schema_evolution_enabled_config_disabled": (
        {
            "is_schema_evolution_enabled": True,
            "schema_evolution": {
                "disable_schema_evolution": True,
            },
        },
        False,
    ),
    "schema_evolution_disabled_config_disabled": (
        {
            "is_schema_evolution_enabled": False,
            "schema_evolution": {
                "disable_schema_evolution": True,
            },
        },
        False,
    ),
    "schema_evolution_disabled_config_enabled": (
        {
            "is_schema_evolution_enabled": False,
            "schema_evolution": {
                "disable_schema_evolution": False,
            },
        },
        False,
    ),
}


@pytest.mark.parametrize(
    "config, expected",
    list(SKIP_DATASET_ENROLLMENT_TESTS.values()),
    ids=list(SKIP_DATASET_ENROLLMENT_TESTS.keys()),
)
@patch("fh_immuta_utils.scripts.manage_data_sources.is_schema_evolution_enabled")
def test_skip_dataset_enrollment(mock_is_schema_evolution_enabled, config, expected):
    config["hostname"] = "redshift.foobar.io"
    config["database"] = "data"
    mock_is_schema_evolution_enabled.return_value = config[
        "is_schema_evolution_enabled"
    ]
    assert skip_dataset_enrollment(None, config) == expected
