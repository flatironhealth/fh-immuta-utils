#!/usr/bin/env python

"""
Script that can be invoked to grep for all existing data sources and then tag columns
within sources as per specified mapping.
"""

import logging

import click
from tqdm import tqdm

from fh_immuta_utils.client import get_client
from fh_immuta_utils.config import parse_config
from fh_immuta_utils.paginator import Paginator
from fh_immuta_utils.tagging import Tagger


@click.command()
@click.option("--config-file", required=True)
@click.option(
    "--search-text",
    help="Will match all data sources that contains this string anywhere in their name",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log the data stores that would be updated without affecting any change",
)
@click.option("--debug", is_flag=True, default=False, help="Debug logging")
def cli_entrypoint(config_file: str, search_text: str, dry_run: bool, debug: bool):
    return main(
        config_file=config_file, search_text=search_text, dry_run=dry_run, debug=debug
    )


def main(config_file: str, search_text: str, dry_run: bool, debug: bool):
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
    with Paginator(client.get_data_source_list, search_text=search_text) as paginator:
        for data_source in paginator:
            data_sources_to_tag.append(
                {"id": data_source["id"], "name": data_source["name"]}
            )

    progress_iterator = tqdm(data_sources_to_tag)
    for data_source in progress_iterator:
        progress_iterator.set_description(
            desc=f"Tagging ID: {data_source['id']}, Name: {data_source['name']} :"
        )
        dictionary = client.get_data_source_dictionary(id=data_source["id"])
        enriched_columns = tagger.enrich_columns_with_tagging(dictionary.metadata)
        if enriched_columns == dictionary.metadata:
            logging.debug(
                f"No change for data source: {data_source['name']}. Skipping."
            )
            continue
        logging.debug(
            f"Enriched columns for {data_source['name']}: {dictionary.dict()['metadata']}"
        )
        logging.info(f"Change detected. Updating data source {data_source['name']}.")
        dictionary.metadata = enriched_columns
        if not dry_run:
            client.update_data_source_dictionary(
                id=data_source["id"], dictionary=dictionary
            )
    logging.info("FIN.")


if __name__ == "__main__":
    cli_entrypoint()
