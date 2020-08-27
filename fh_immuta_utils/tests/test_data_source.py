from collections import namedtuple
from typing import Dict, List, Any
from pydantic import ValidationError

import pytest

import fh_immuta_utils.data_source as ds

NameTestKeys = namedtuple(
    "NameTestKeys", ["handler_type", "schema", "table", "user_prefix", "expected_name"]
)

IMMUTA_NAME_TESTS = [
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
        table=f"{'a'*(ds.MAX_IMMUTA_NAME_LIMIT - 7)}",
        user_prefix="",
        expected_name=(
            f"{ds.PREFIX_MAP['PostgreSQL']}_foo_{'a'*(ds.MAX_IMMUTA_NAME_LIMIT - 7)}"
        ),
    ),
    NameTestKeys(
        handler_type="PostgreSQL",
        schema="foo",
        table=f"{'a'*ds.MAX_IMMUTA_NAME_LIMIT}",
        user_prefix="",
        expected_name=(
            f"{ds.PREFIX_MAP['PostgreSQL']}_foo_{'a'*(ds.MAX_IMMUTA_NAME_LIMIT - 7)}"
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
]


@pytest.mark.parametrize(
    "handler_type,schema,table,user_prefix,expected_name", IMMUTA_NAME_TESTS
)
def test_make_immuta_table_name(
    handler_type: str, schema: str, table: str, user_prefix: str, expected_name: str
):
    name = ds.make_immuta_table_name(
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
        kwargs={"bodataTableName": "foobar", "dataSourceName": "bazqux"},
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
    "ObjectTestKeys", ["db_keys", "handler_type", "expected_type", "columns"]
)
OBJECT_TESTS = [
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
    ),
    ObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        columns=COLUMNS,
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
    ),
]


@pytest.mark.parametrize("db_keys,handler_type,expected_type,columns", OBJECT_TESTS)
def test_to_immuta_objects(
    db_keys: Dict[str, str],
    handler_type: str,
    expected_type: Any,
    columns: List[ds.DataSourceColumn],
):
    base_config = {"username": "foo", "password": "bar", "database": "baz"}
    config = {**base_config, **db_keys}
    config["handler_type"] = handler_type
    source, handler = ds.to_immuta_objects(
        table="foo", schema="bar", config=config, columns=columns
    )
    assert source.name == ds.make_immuta_table_name(
        table="foo", schema="bar", handler_type=handler_type, user_prefix=""
    )
    assert source.sqlTableName == ds.make_postgres_table_name(
        table="foo", schema="bar", handler_type=handler_type, user_prefix=""
    )
    assert source.blobHandlerType == handler_type
    assert isinstance(handler, ds.Handler)
    assert isinstance(handler.metadata, expected_type)


TABLES = ["foo", "bar", "baz"]
BulkObjectTestKeys = namedtuple(
    "BulkObjectTestKeys", ["db_keys", "handler_type", "expected_type", "tables"]
)
BULK_OBJECT_TESTS = [
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        tables=[],
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="PostgreSQL",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
    ),
    BulkObjectTestKeys(
        db_keys={"hostname": "qux"},
        handler_type="Redshift",
        expected_type=ds.PostgresHandlerMetadata,
        tables=TABLES,
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
    ),
]


@pytest.mark.parametrize("db_keys,handler_type,expected_type,tables", BULK_OBJECT_TESTS)
def test_make_bulk_create_objects(
    db_keys: Dict[str, str], handler_type: str, expected_type: Any, tables: List[str]
):
    base_config = {"username": "foo", "password": "bar", "database": "baz"}
    config = {**base_config, **db_keys}
    config["handler_type"] = handler_type

    source, handlers = ds.make_bulk_create_objects(
        tables=tables, schema="bar", config=config
    )
    assert len(handlers) == len(tables)
    assert source.blobHandlerType == handler_type

    table_names = []
    for handler in handlers:
        table_names.append(handler.metadata.bodataTableName)
        assert isinstance(handler, ds.Handler)
        assert isinstance(handler.metadata, expected_type)

    for table in tables:
        assert (
            ds.make_immuta_table_name(
                table=table, schema="bar", handler_type=handler_type, user_prefix=""
            )
            in table_names
        )
