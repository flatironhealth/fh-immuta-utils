import fnmatch
from collections import defaultdict
import logging
import os
import glob
from functools import partial
from typing import Any, Dict, List, Iterator, Tuple, TYPE_CHECKING

import yaml

from pydantic import BaseModel
from toolz.dicttoolz import keyfilter

from .data_source import DataSourceColumn

if TYPE_CHECKING:
    from immuta_utils.client import ImmutaClient

LOGGER = logging.getLogger(__name__)


class Tag(BaseModel):
    name: str
    source: str = "curated"


class Tagger(object):
    """ Wrapper around managing tags. """

    def __init__(self, config_root: str) -> None:
        # column_name: [tag1, tag2, ...]
        self.tag_map_datadict: Dict[str, List[str]] = {}

        # (handler_type, database): {"prefix": [tag1, tag2, ..]}
        self.tag_map_datasource: Dict[Tuple, Dict[str, List[str]]] = {}

        self.read_configs(config_root=config_root)

    def read_configs(self, config_root: str) -> None:
        for tag_file in glob.glob(os.path.join(config_root, "tags", "*.yml")):
            logging.debug("Reading tag file: %s", tag_file)
            with open(tag_file) as handle:
                contents = yaml.safe_load(handle)
                self.tag_map_datadict = {
                    **self.tag_map_datadict,
                    **contents.get("TAG_MAP", {}),
                }

        for datasource_file in glob.glob(
            os.path.join(config_root, "enrolled_datasets", "*.yml")
        ):
            logging.debug("Reading enrolled data source file: %s", datasource_file)
            with open(datasource_file) as handle:
                contents = yaml.safe_load(handle)
                handler_type = contents.get("handler_type")
                database = contents.get("database")
                tag_map_entry = {(handler_type, database): contents.get("tags", {})}
                self.tag_map_datasource = {
                    **self.tag_map_datasource,
                    **tag_map_entry,
                }

    def get_tags_for_column(self, column_name: str) -> List[str]:
        return self.tag_map_datadict.get(column_name, [])

    def get_tags_for_data_source(
        self, name: str, handler_type: str, connection_string: str
    ) -> List[Dict[str, Any]]:
        """
        Finds tags whose key matches a data source name using Unix shell-style wildcard matching.
        e.g. if key is "ath_foo*", all data source names with prefix "ath_foo" will get that key's tags
        :param name: the data source name
        :param handler_type: the type of handler for this data source (e.g. Redshift, Amazon Athena, etc.)
        :param connection_string: the remote database connection string for the data source
        :return: list of tag dicts
        """
        tags_for_data_source = []
        # remote database is not returned by the API so we strip it from the connection string
        database = connection_string[connection_string.rfind("/") + 1 :]
        tag_dict = self.tag_map_datasource.get((handler_type, database), {})

        for prefix, tag_list in tag_dict.items():
            if fnmatch.fnmatch(name, prefix):
                for tag in tag_list:
                    tags_for_data_source.append({"name": tag, "source": "curated"})
        return tags_for_data_source

    def is_root_tag(self, tag_to_check: str) -> bool:
        """
        Determines if tag is the true root by checking all available tags from config
        """
        tag_map_datasource_dicts: Dict[str, List[str]] = {}
        for tag_dict in self.tag_map_datasource.values():
            tag_map_datasource_dicts = {**tag_map_datasource_dicts, **tag_dict}
        all_tags = {**self.tag_map_datadict, **tag_map_datasource_dicts}
        tag_list = []
        for k, v in all_tags.items():
            tag_list.append(v)
        # flatten the list of lists into a single list of all items
        flat_tag_list = [item for sublist in tag_list for item in sublist]

        for tag in flat_tag_list:
            if "." in tag and tag.split(".")[0] == tag_to_check:
                return True
        return False

    def tags_to_make(self) -> Iterator[Tuple[str, List[str]]]:
        """
        Yields a dict of either str or List[str] where the key is a parent tag
        and the value is a list of child tags if any, or the parent tag itself if no children.
        """

        all_tags: Dict[str, List[str]] = defaultdict(list)
        for tag_list in self.tag_map_datadict.values():
            for tag in tag_list:
                parent = tag.split(".")[0]
                if tag not in all_tags[parent]:
                    all_tags[parent].append(tag)

        for tag_dict in self.tag_map_datasource.values():
            for tag_list in tag_dict.values():
                for tag in tag_list:
                    parent = tag.split(".")[0]
                    if tag not in all_tags[parent]:
                        all_tags[parent].append(tag)

        for root_tag, children in all_tags.items():
            if len(children) == 1 and children[0] == root_tag:
                children = []
            yield (root_tag, children)

    @classmethod
    def create_message_body_for_tag_creation(
        cls, root_tag: str, children: List[str]
    ) -> Dict[str, Any]:
        if not children:
            return {
                "tags": [Tag(name=root_tag).dict(by_alias=True, exclude_unset=True)]
            }

        return {
            "rootTag": {"name": root_tag, "deleteHierarchy": False},
            "tags": [
                Tag(name=child).dict(by_alias=True, exclude_unset=True)
                for child in children
            ],
        }

    def make_tags(self, client: "ImmutaClient") -> None:
        LOGGER.debug("Creating tags")
        for (root_tag, children) in self.tags_to_make():
            LOGGER.debug(f"Creating root tag: {root_tag}, children: {children}")
            client.create_tag(
                tag_data=self.create_message_body_for_tag_creation(
                    root_tag=root_tag, children=children
                )
            )

    def enrich_columns_with_tagging(
        self, columns: List[DataSourceColumn]
    ) -> List[DataSourceColumn]:
        """Append column tags to a pre-existing list of columns.

        Returns
        -------
        enriched_columns:
            A copy of the list of data source columns with tags added.

        """
        enriched_columns = []
        for c in columns:
            column = c.copy(deep=True)
            # We need to set the source when adding tags, otherwise the API silently rejects the tags list
            # We don't know what other possible values exist for source, but all our currently made tags
            # are curated.
            column.tags = [
                {"name": tag, "source": "curated"}
                for tag in self.get_tags_for_column(column_name=column.name)
            ]
            enriched_columns.append(column)
        return enriched_columns
