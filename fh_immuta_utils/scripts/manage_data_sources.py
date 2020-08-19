from functools import partial
import fnmatch
import os
import logging
import glob
from typing import (
    Callable,
    Dict,
    List,
    Any,
    Union,
    Iterator,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

import click
import yaml
import requests
from toolz.itertoolz import groupby
from toolz.dicttoolz import keyfilter

from fh_immuta_utils.authenticate import retrieve_credentials
from fh_immuta_utils.client import get_client
from fh_immuta_utils.config import parse_config
from fh_immuta_utils.data_source import (
    DataSource,
    Handler,
    to_immuta_objects,
    make_handler_metadata,
    make_bulk_create_objects,
)

if TYPE_CHECKING:
    from fh_immuta_utils.client import ImmutaClient

LOGGER = logging.getLogger(__name__)


@click.command(help="Enroll/update data sources")
@click.option("--config-file", required=True)
@click.option(
    "--glob-prefix",
    default="*.yml",
    help="Glob for specific data source spec files matching given prefix",
)
@click.option("--debug", is_flag=True, default=False, help="Debug logging")
@click.option("--dry-run", is_flag=True, default=False, help="Dry run")
def cli_entrypoint(config_file: str, glob_prefix: str, debug: bool, dry_run: bool):
    main(config_file=config_file, glob_prefix=glob_prefix, debug=debug, dry_run=dry_run)


def main(config_file: str, glob_prefix: str, debug: bool, dry_run: bool) -> bool:

    logging.basicConfig(
        format="[%(name)s][%(levelname)s][%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=(logging.DEBUG if debug else logging.INFO),
    )

    no_enrollment_errors = True

    config = parse_config(config_file=config_file)
    client = get_client(**config)

    dataset_spec_filepath = os.path.join(
        config["config_root"], "enrolled_datasets", glob_prefix
    )
    LOGGER.debug(f"Globbing for files in {dataset_spec_filepath}")
    for filepath in glob.glob(dataset_spec_filepath):
        LOGGER.info("Processing file: %s", filepath)
        with open(filepath) as handle:
            dataset_spec = yaml.safe_load(handle)
        credentials = retrieve_credentials(dataset_spec["credentials"])
        dataset_spec["username"] = credentials["username"]
        dataset_spec["password"] = credentials["password"]

        schema_table_mapping = get_tables_in_database(client, dataset_spec)

        failed_tables = set()
        data_sources_to_enroll = [
            (dataset_spec["schemas_to_enroll"], data_sources_enroll_iterator),
            (dataset_spec["schemas_to_bulk_enroll"], data_sources_bulk_enroll_iterator),
        ]

        for schemas, enroll_iter in data_sources_to_enroll:
            if not schemas:
                continue
            for schema_object in schemas:
                for (data_source, handler) in enroll_iter(  # type: ignore
                    client=client,
                    schema_table_mapping=schema_table_mapping,
                    schema_obj=schema_object,
                    config=dataset_spec,
                ):
                    LOGGER.debug("Data source: %s", data_source.json())
                    if isinstance(handler, list):
                        LOGGER.debug("Handler[0]: %s", handler[0].json())
                    elif isinstance(handler, Handler):
                        LOGGER.debug("Handler: %s", handler.json())
                    else:
                        raise TypeError(
                            f"Unexpected type for handler; Got: {type(handler)}"
                        )
                    if not dry_run:
                        if not create_data_source(
                            client=client, data_source=data_source, handler=handler
                        ):
                            failed_tables.add(data_source.name)
        if failed_tables:
            no_enrollment_errors = False
            LOGGER.warning("Tables that failed creation:")
            for table in failed_tables:
                LOGGER.warning(table)

    LOGGER.info("Finished enrollment")
    return no_enrollment_errors


def get_tables_in_database(
    client: "ImmutaClient", config: Dict[str, Any]
) -> Dict[str, List[Dict[str, str]]]:
    """Returns a list of schema_name: [tables...] mapping in the database
    specified by the config"""
    # Grab list of all tables in all schemas in the database
    tables_in_database = client.get_table_names(config)
    # Group the tables per schema
    return groupby("tableSchema", tables_in_database)


def data_sources_enroll_iterator(
    client: "ImmutaClient",
    schema_table_mapping: Dict[str, List[Dict[str, str]]],
    schema_obj: Dict[str, str],
    config: Dict[str, Any],
) -> Iterator[Tuple[DataSource, Handler]]:
    LOGGER.info("Processing schema_prefix: %s", schema_obj["schema_prefix"])

    matches_prefix = partial(fnmatch.fnmatch, pat=schema_obj["schema_prefix"])

    for schema, tables in keyfilter(matches_prefix, schema_table_mapping).items():
        LOGGER.info("Processing schema: %s", schema)
        for table in tables:
            if not fnmatch.fnmatch(table["tableName"], schema_obj["table_prefix"]):
                continue
            LOGGER.info("Processing table: %s.%s", schema, table["tableName"])
            handler = make_handler_metadata(
                config=config, table=table["tableName"], schema=schema
            )
            columns = client.get_column_types(
                config=config, data_source_type=config["handler_type"], handler=handler
            )

            data_source, handler = to_immuta_objects(
                schema=schema, table=table["tableName"], columns=columns, config=config
            )
            yield (data_source, handler)


def data_sources_bulk_enroll_iterator(
    client: "ImmutaClient",
    schema_table_mapping: Dict[str, List[Dict[str, str]]],
    schema_obj: Dict[str, str],
    config: Dict[str, Any],
) -> Iterator[Tuple[DataSource, List[Handler]]]:

    LOGGER.info("Processing schema_prefix: %s", schema_obj["schema_prefix"])

    matches_prefix = partial(fnmatch.fnmatch, pat=schema_obj["schema_prefix"])

    for schema, tables in keyfilter(matches_prefix, schema_table_mapping).items():
        LOGGER.info("Bulk creating for all tables in schema %s", schema)
        if len(tables) == 0:
            LOGGER.warning("No tables found for schema: %s", schema)
            continue
        data_source, handlers = make_bulk_create_objects(
            schema=schema,
            tables=[table["tableName"] for table in tables],
            config=config,
            user_prefix=config.get("prefix"),
        )
        yield (data_source, handlers)


def create_data_source(
    client: "ImmutaClient",
    data_source: DataSource,
    handler: Union[Handler, List[Handler]],
) -> bool:
    try:
        result = client.create_data_source(data_source, handler)
        if result:
            LOGGER.info(
                "Created data source %s, id: %d", data_source.name, result["id"]
            )
        return True
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Error creating data source %s: %s", data_source.name, err)
        return False


if __name__ == "__main__":
    cli_entrypoint()
