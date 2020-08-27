import logging

import click
from tqdm import tqdm

from fh_immuta_utils.client import get_client
from fh_immuta_utils.config import parse_config
from fh_immuta_utils.paginator import Paginator

LOGGER = logging.getLogger(__name__)


@click.command(help="Bulk delete data sources that match a given prefix")
@click.option("--config-file", required=True)
@click.option(
    "--search-text",
    help="Delete the data sources that contains this string in their name",
)
@click.option(
    "--hard-delete",
    is_flag=True,
    default=False,
    help="Permanently remove the data-sources",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log the data stores that would be removed instead of deleting them",
)
@click.option("--debug", is_flag=True, default=False, help="Debug logging")
def main(
    config_file: str, search_text: str, hard_delete: bool, dry_run: bool, debug: bool
):
    logging.basicConfig(
        format="[%(name)s][%(levelname)s][%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=(logging.DEBUG if debug else logging.INFO),
    )
    config = parse_config(config_file=config_file)
    client = get_client(**config)

    logging.info("Gathering data-stores to delete")
    data_sources_to_delete = []
    with Paginator(client.get_data_source_list, search_text=search_text) as paginator:
        for data_source in paginator:
            data_sources_to_delete.append(
                {"id": data_source["id"], "name": data_source["name"]}
            )

    if dry_run:
        logging.info("bulk-delete dry run")
        for data_source in data_sources_to_delete:
            logging.info(
                f"Data source Id: {data_source['id']}. Name: {data_source['name']}"
            )
    elif hard_delete:
        logging.info(
            f"Hard deleting {len(data_sources_to_delete)} data sources. "
            "The data sources will not be able to be restored in the future"
        )
        for data_source in tqdm(data_sources_to_delete, desc="Deleting"):
            logging.debug(f"Hard deleting {data_source['name']}")
            client.delete_data_source(data_source["id"])
    else:
        logging.info(
            f"Disabling {len(data_sources_to_delete)} data sources. "
            "The data sources can be restored in the future"
        )
        for data_source in tqdm(data_sources_to_delete, desc="Disabling"):
            logging.debug(f"Disabling {data_source['name']}")
            client.disable_data_source(data_source["id"])


if __name__ == "__main__":
    main()
