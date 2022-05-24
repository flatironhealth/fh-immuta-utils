#!/usr/bin/env python

"""
Script that can be invoked to grep for all existing data sources and then tag columns
within sources as per specified mapping.
"""

from collections import defaultdict
import logging
from typing import Dict, Set, Tuple

import click
from tqdm import tqdm

from fh_immuta_utils.client import get_client
from fh_immuta_utils.config import parse_config
from fh_immuta_utils.paginator import Paginator
from fh_immuta_utils.tagging import IMMUTA_SPECIAL_TAGS, Tagger


IMMUTA_API_PAGE_SIZE = 25_000


@click.command(help="Tag existing data sources based on provided tagging info")
@click.option("--config-file", required=True)
@click.option(
    "--search-text",
    help="Will match all data sources that contain this string anywhere in their name",
)
@click.option(
    "--search-schema",
    help="Will match all data sources that match this schema",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log the data stores that would be updated without affecting any change",
)
@click.option("--debug", is_flag=True, default=False, help="Debug logging")
def cli_entrypoint(
    config_file: str, search_text: str, search_schema: str, dry_run: bool, debug: bool
):
    return main(
        config_file=config_file,
        search_text=search_text,
        search_schema=search_schema,
        dry_run=dry_run,
        debug=debug,
    )


def main(
    config_file: str, search_text: str, search_schema: str, dry_run: bool, debug: bool
):
    logging.basicConfig(
        format="[%(name)s][%(levelname)s][%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=(logging.DEBUG if debug else logging.INFO),
    )
    config = parse_config(config_file=config_file)
    client = get_client(**config)
    tagger = Tagger(config_root=config["config_root"])

    logging.info("Making tags")
    tagger.make_tags(client)

    logging.info("Gathering data sources to tag")
    data_sources_to_tag = []
    with Paginator(
        client.get_data_source_list,
        search_text=search_text,
        search_schema=search_schema,
        size=IMMUTA_API_PAGE_SIZE,
    ) as paginator:
        for data_source in paginator:
            data_sources_to_tag.append(
                {
                    "id": data_source["id"],
                    "name": data_source["name"],
                    "handler_type": data_source["blobHandlerType"],
                    "connection_string": data_source["connectionString"],
                }
            )

    logging.info("Getting current data source and column tags")
    # data_source_name: {tag_1_name, tag_2_name, ...}
    current_data_source_tag_names_map: Dict[str, Set[str]] = defaultdict(set)
    # data_source_name: {column_1_name: {tag_1_name, tag_2_name, ...}, ...}
    current_column_tag_names_map: Dict[str, Dict[str, Set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for tag_info in client.get_all_data_source_and_column_tags()["hits"]:
        data_source = tag_info["Data Source"]
        tag_name = tag_info["Tag Name"]
        column_name = tag_info["Column Name"]

        if tag_name in IMMUTA_SPECIAL_TAGS:
            continue

        if tag_info["Type"] == "Data Source":
            current_data_source_tag_names_map[data_source].add(tag_name)
        elif tag_info["Type"] == "Column":
            current_column_tag_names_map[data_source][column_name].add(tag_name)

    logging.info("Determining changes in data source tags")
    new_tag_name_to_data_source_ids: Dict[str, Set[int]] = defaultdict(set)
    removed_tag_name_and_data_source_ids: Set[Tuple[int, str]] = set()
    for data_source in data_sources_to_tag:
        data_source_name = data_source["name"]
        data_source_tags = tagger.get_tags_for_data_source(
            name=data_source_name,
            handler_type=data_source["handler_type"],
            connection_string=data_source["connection_string"],
        )
        data_source_tag_names = {tag_data["name"] for tag_data in data_source_tags}

        current_data_source_tag_names = current_data_source_tag_names_map[
            data_source_name
        ]
        new_data_source_tag_names = (
            data_source_tag_names - current_data_source_tag_names
        )

        for tag_name in new_data_source_tag_names:
            new_tag_name_to_data_source_ids[tag_name].add(data_source["id"])

        removed_data_source_tag_names = (
            current_data_source_tag_names - data_source_tag_names
        )
        for tag_name in removed_data_source_tag_names:
            removed_tag_name_and_data_source_ids.add((data_source["id"], tag_name))

    logging.info("Updating new data source tags")
    progress_iterator = tqdm(
        new_tag_name_to_data_source_ids.items(),
        total=len(new_tag_name_to_data_source_ids),
    )
    for tag_name, data_source_ids in progress_iterator:
        progress_iterator.set_description(
            desc=f"[Tagging Data Sources] Tag: {tag_name}, # Data Sources: {len(data_source_ids)} :"
        )
        if not dry_run:
            client.update_data_source_tags_in_bulk(
                ids=list(data_source_ids),
                tag_data=[
                    {
                        "name": tag_name,
                        "source": "curated",
                    }
                ],
            )
    logging.info("Updating removed data source tags")
    progress_iterator = tqdm(removed_tag_name_and_data_source_ids)
    for data_source_id, tag_name in progress_iterator:
        progress_iterator.set_description(
            desc=f"[Removing Data Source Tag] ID: {data_source_id}, Tag: {tag_name}"
        )
        if not dry_run:
            client.delete_data_source_tag(data_source_id, tag_name)

    logging.info("Determining data sources with new column tags")
    # Compute the columns and associated column tag names we want for each data source.
    #
    # Since we cannot get the set of all columns of a data source without making
    # an Immuta API call for each data source (which would be expensive), we
    # compute the set of all relevant columns for each data source by searching
    # for all data sources with a given relevant column. E.g. if we want to tag
    # all columns with name `example`, we find the relevant data sources by
    # searching for data sources with the column `example`, rather than getting
    # the set of all columns of all data sources and then filtering the data
    # sources.
    #
    # data_source_name: {column_1_name: {tag_1_name, tag_2_name, ...}, ...}
    wanted_column_tag_names_map: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
    for column in tqdm(
        tagger.tag_map_datadict.keys(),
        total=len(tagger.tag_map_datadict),
    ):
        tag_names = set(tagger.get_tags_for_column(column))
        with Paginator(
            client.get_data_source_list,
            search_text=search_text,
            search_schema=search_schema,
            columns=[column],
            size=IMMUTA_API_PAGE_SIZE,
        ) as paginator:
            for data_source in paginator:
                data_source_name = data_source["name"]
                wanted_column_tag_names_map[data_source_name][column] = tag_names

    # Compute the data sources with column tags to update by checking if the current
    # data dictionary matches the data dictionary we want.
    data_sources_with_column_tags_to_update = set()
    for data_source in data_sources_to_tag:
        data_source_name = data_source["name"]
        data_source_id = data_source["id"]
        if (
            current_column_tag_names_map[data_source_name]
            != wanted_column_tag_names_map[data_source_name]
        ):
            data_sources_with_column_tags_to_update.add(data_source_id)

    logging.info(
        f"Updating {len(data_sources_with_column_tags_to_update)} data dictionaries with changed column tags"
    )
    progress_iterator = tqdm(data_sources_to_tag)
    for data_source in progress_iterator:
        data_source_id = data_source["id"]
        data_source_name = data_source["name"]
        progress_iterator.set_description(
            desc=f"[Tagging Columns] ID: {data_source_id}, Name: {data_source_name} :"
        )

        if data_source_id not in data_sources_with_column_tags_to_update:
            continue

        dictionary = client.get_data_source_dictionary(id=data_source_id)
        enriched_columns = tagger.enrich_columns_with_tagging(dictionary.metadata)
        if enriched_columns == dictionary.metadata:
            logging.warning(
                f"Expected a change to column tags for data source: {data_source_name}, but no change found. Skipping."
            )
            continue
        logging.debug(
            f"Enriched columns for {data_source_name}:"
            f" {dictionary.dict()['metadata']}"
        )
        logging.info(
            f"Change detected to column tags. Updating data source {data_source_name}'s data dictionary."
        )
        dictionary.metadata = enriched_columns
        if not dry_run:
            client.update_data_source_dictionary(
                id=data_source_id, dictionary=dictionary
            )
    logging.info("FIN.")


if __name__ == "__main__":
    cli_entrypoint()
