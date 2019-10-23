from typing import Dict, Any, Optional, List, Tuple
import logging

from pydantic import BaseModel

LOGGER = logging.getLogger(__name__)

HANDLER_TYPES = {
    "PostgreSQL": "pg",
    "Microsoft SQL Server": "mssql",
    "Apache Hive": "hive",
    "Apache Impala": "impala",
    "Apache HDFS": "hdfs",
    "Azure Blob Storage": "azureblob",
    "Azure SQL Data Warehouse": "asdw",
    "Netezza": "netezza",
    "MariaDB": "mariadb",
    "DB2": "db2",
    "Oracle": "oracle",
    "MySQL": "mysql",
    "Elastic": "elastic",
    "Teradata": "teradata",
    "Greenplum": "greenplum",
    "Redshift": "redshift",
    "Amazon S3": "s3",
    "FTP": "ftp",
    "Persisted": "persisted",
    "Custom": "custom",
    "MEMSQL": "memsql",
    "Presto": "presto",
    "Amazon Athena": "athena",
    "Vertica": "vertica",
}

PREFIX_MAP = {
    "PostgreSQL": "pg",
    "Redshift": "rs",
    "Amazon S3": "s3",
    "Amazon Athena": "ath",
}
MAX_IMMUTA_NAME_LIMIT = 55
MAX_POSTGRES_NAME_LIMIT = 63


def blob_handler_type(handler_type: str) -> str:
    return HANDLER_TYPES.get(handler_type, handler_type)


def make_immuta_table_name(
    handler_type: str, schema: str, table: str, user_prefix: Optional[str]
) -> str:
    """
    Returns a table name that's guaranteed to be unique and within the max char limit (55)
    """
    table_name = ""
    if user_prefix:
        table_name = f"{user_prefix}_"
    table_name += f"{PREFIX_MAP[handler_type]}_{schema}_{table}"
    if table_name is None:
        return None
    if len(table_name) <= MAX_IMMUTA_NAME_LIMIT:
        return table_name
    import hashlib

    return (
        table_name[: MAX_IMMUTA_NAME_LIMIT - 8]
        + hashlib.md5(table_name.encode()).hexdigest()[:8]
    )


def make_postgres_table_name(
    handler_type: str, schema: str, table: str, user_prefix: Optional[str]
) -> str:
    """
    Returns table name that has a shortened prefix and conforms to the Postgres max char limit (63)
    """
    table_name = ""
    if user_prefix:
        table_name = f"{user_prefix}_"
    table_name += f"{PREFIX_MAP[handler_type]}_{schema}_{table}"
    if len(table_name) < MAX_POSTGRES_NAME_LIMIT:
        return table_name
    trunc_table_name = table_name[:MAX_POSTGRES_NAME_LIMIT]
    LOGGER.warning(
        "Postgres table name too long! Table %s truncated to %s",
        table_name,
        trunc_table_name,
    )
    return trunc_table_name


class BlobHandler(BaseModel):
    scheme: str = ""
    url: str = ""


class ProfileGroup(BaseModel):
    profiles: List[str] = []
    groups: List[str] = []


class DataSource(BaseModel):
    # Describes the type of underlying blob handler that will be used
    # with this Data Source, e.g. Custom, MS SQL.
    blobHandlerType: str
    private: Optional[bool] = None
    # organization: str
    # What to call the data source in Immuta
    name: Optional[str] = None
    # A list of full URLs providing the locations of all Blob Store handlers
    # to use with this Data Source.
    # Unnecessary when creating data source
    blobHandler: Optional[BlobHandler] = BlobHandler()
    # Users and Groups that should be added as owners to this Data Source.
    # Profiles must be a list of profile ID's and groups must be a list of group ids.
    owner: ProfileGroup = ProfileGroup()
    expert: ProfileGroup = ProfileGroup()
    ingest: ProfileGroup = ProfileGroup()
    sqlTableName: Optional[str] = None
    # The category of the Data Source
    # category: str = ""
    description: str = ""
    hasSamples: bool = False
    recordFormat: str = "json"
    # The type of Data Source, whether it is ingested (metadata will exist in Immuta)
    # or queryable (metadata is dynamically queried)
    type: str = "queryable"
    useDatesAsDirectory: bool = False


class DataSourceColumn(BaseModel):
    name: str
    dataType: str
    remoteType: str
    nullable: bool
    tags: List[Dict[str, str]] = []


class HandlerMetadata(BaseModel):
    database: str
    username: str
    password: str
    table: str
    fh_schema: str
    # The length in seconds that data from this handler can be cached.
    staleDataTolerance: int
    # Don't know what this is. . .
    # blobId: List[str]
    # Table name that is exposed by Immuta
    bodataTableName: str = ""
    # The name of the Data Source to which this handler corresponds
    dataSourceName: str = ""
    columns: Optional[List[DataSourceColumn]] = None
    format: str = "json"
    isChildDataSource: bool = False
    ssl: bool = True

    class Config:
        fields = {"fh_schema": "schema"}


class AthenaHandlerMetadata(HandlerMetadata):
    queryResultLocationBucket: str
    queryResultLocationDirectory: str
    region: str
    authenticationMethod: str = "accessKey"


class PostgresHandlerMetadata(HandlerMetadata):
    hostname: str
    port: int = 5432


class Handler(BaseModel):
    metadata: HandlerMetadata


class DataSourceDictionary(BaseModel):
    id: int
    dataSource: int
    metadata: List[DataSourceColumn]
    types: List[str]
    createdAt: str = ""
    updatedAt: str = ""


HANDLER_TO_METADATA_CLASS = {
    "PostgreSQL": PostgresHandlerMetadata,
    "Amazon Athena": AthenaHandlerMetadata,
    "Redshift": PostgresHandlerMetadata,
}


def make_bulk_create_objects(
    config: Dict[str, Any],
    schema: str,
    tables: List[str],
    user_prefix: Optional[str] = None,
) -> Tuple[DataSource, List[Handler]]:
    """
    Returns a (data source, metadata) tuple containing relevant details to bulk create new data
    sources in Immuta from the source schema
    """
    handlers = []
    for table in tables:
        external_table_name = make_postgres_table_name(
            config["handler_type"], schema, table, user_prefix
        )
        immuta_table_name = make_immuta_table_name(
            config["handler_type"], schema, table, user_prefix
        )

        handler = make_handler_metadata(
            table=table,
            schema=schema,
            config=config,
            bodataTableName=external_table_name,
            dataSourceName=immuta_table_name,
        )
        handlers.append(handler)

    ds = DataSource(
        blobHandlerType=config["handler_type"], recordFormat="json", type="queryable"
    )
    return (ds, handlers)


def to_immuta_objects(
    config: Dict[str, Any],
    schema: str,
    table: str,
    columns: List[DataSourceColumn],
    user_prefix: Optional[str] = None,
) -> Tuple[DataSource, Handler]:
    """
    Returns a tuple containing relevant details to create a new data source
    in Immuta from the source schema
    """
    external_table_name = make_postgres_table_name(
        handler_type=config["handler_type"],
        schema=schema,
        table=table,
        user_prefix=user_prefix,
    )
    immuta_table_name = make_immuta_table_name(
        config["handler_type"], schema, table, user_prefix
    )
    handler = make_handler_metadata(
        table=table,
        schema=schema,
        config=config,
        columns=columns,
        bodataTableName=external_table_name,
        dataSourceName=immuta_table_name,
    )
    ds = DataSource(
        name=immuta_table_name,
        sqlTableName=external_table_name,
        blobHandlerType=config["handler_type"],
        blobHandler=BlobHandler(scheme="https"),
        recordFormat="json",
        type="queryable",
        # category="foo",
        description="bar",
        # owner="foo",
    )
    return (ds, handler)


def make_handler_metadata(
    table: str, schema: str, config: Dict[str, Any], **kwargs
) -> Handler:

    # Ensure that required keys are set
    # Defaulting values in the BaseModel doesn't work
    # since we don't use defaults in this particular instance
    required_args = {
        "format": "json",
        "ssl": True,
        "staleDataTolerance": (24 * 60 * 60),
    }
    for k, v in required_args.items():
        config[k] = config.get(k, v)
    if config["handler_type"] == "Amazon Athena":
        metadata = AthenaHandlerMetadata(
            # Default value but has to exist in the final used dict
            authenticationMethod="accessKey",
            **kwargs,
            **locals(),
            **config,
        )
    elif config["handler_type"] in ["PostgreSQL", "Redshift"]:
        metadata = PostgresHandlerMetadata(
            # Default value but has to exist in the final used dict
            **kwargs,
            **locals(),
            **config,
        )
    handler = Handler(metadata=metadata)
    return handler
