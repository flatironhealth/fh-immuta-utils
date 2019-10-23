from fh_immuta_utils.client import ImmutaClient

client_config = {"base_url": "...", "api_key": "..."}

if __name__ == "__main__":
    client = ImmutaClient(**client_config)

    # populate
    db_host = "<<redshift host>>"
    db_username = "<<user>>"
    db_password = "<<password>>"
    db_port = 5439  # or a different port
    db_database = "<<database name>>"
    db_use_ssl = True

    # what we're doing here is that we're adding a list of known tables from the same schema.
    #
    remote_schemas_and_tables = {
        "public": [
            "table_1",
            "table_2",
            # ...
        ],
        # more schemas can be appended here
    }

    # do notice, no schema is required:
    handlers = []
    handler_type = "redshift"
    blob_handler_type = handler_type.title()

    for remote_schema in remote_schemas_and_tables.keys():
        for remote_table in remote_schemas_and_tables.get(remote_schema, []):
            handlers.append(
                {
                    "metadata": {
                        "ssl": db_use_ssl,
                        "isChildDataSource": False,
                        "userFiles": [],
                        "port": db_port,
                        "hostname": db_host,
                        "database": db_database,
                        "username": db_username,
                        "password": db_password,
                        "format": "csv",
                        "staleDataTolerance": (7 * 24 * 60 * 60),
                        "bodataTableName": remote_table,
                        "dataSourceName": "{}_{}".format(remote_schema, remote_table),
                        "table": remote_table,
                        "schema": remote_schema,
                    }
                }
            )

    # we end up with a flat list of handlers, no table structure is required:

    # the data source payload is rather counter-intuitive, as it does not play an important role
    # in this type of request, but is required nonetheless:
    data_source = {
        "useDatesAsDirectory": False,
        "private": True,
        "blobHandler": {"scheme": "https", "url": ""},
        "policyHandlerType": "None",
        "blobHandlerType": blob_handler_type,
        "recordFormat": "csv",
        "type": "queryable",
        "hasSamples": False,
    }

    client.create_data_sources(
        handler_type=handler_type, handlers=handlers, data_source=data_source
    )
