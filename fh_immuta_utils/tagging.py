from collections import defaultdict
import logging
import os
import glob
from typing import Any, Dict, List, Iterator, Tuple, TYPE_CHECKING

import yaml

from pydantic import BaseModel

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

        # prefix_schema: [tag1, tag2, ...]
        self.tag_map_datasource: Dict[str, List[str]] = {}

        # tag_name: [iam_group_1, iam_group_2, ...]
        self.tag_groups: Dict[str, List[str]] = {}

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
                self.tag_groups = {**self.tag_groups, **contents.get("TAG_GROUPS", {})}

        for datasource_file in glob.glob(
            os.path.join(config_root, "enrolled_datasets", "*.yml")
        ):
            logging.debug("Reading enrolled data source file: %s", datasource_file)
            with open(datasource_file) as handle:
                contents = yaml.safe_load(handle)
                self.tag_map_datasource = {
                    **self.tag_map_datasource,
                    **contents.get("tags", {}),
                }

    def get_tags_for_column(self, column_name: str) -> List[str]:
        return self.tag_map_datadict.get(column_name, [])

    def get_tags_for_data_source(self, name: str) -> List[Dict[str, Any]]:
        """
        Finds tags whose key matches the prefix of the data source name.
        e.g. if key is "ath_foo", all data sources with prefix "ath_foo" will get that key's tags
        :param name: data source name
        :return: list of tag dicts
        """
        tags = []
        for k, v in self.tag_map_datasource.items():
            if name.startswith(k):
                for tag in v:
                    tags.append({"name": tag, "source": "curated"})
        return tags

    def is_root_tag(self, tag_to_check: str) -> bool:
        for tag in self.tag_groups:
            if "." in tag and tag.split(".")[0] == tag_to_check:
                return True
        return False

    def get_allowed_groups_for_tag(self, tag: str) -> List[str]:
        """ Returns IAM groups allowed access to view things with given tag """
        return self.tag_groups.get(tag, [])

    def tags_to_make(self) -> Iterator[Tuple[str, List[str]]]:
        """
        Yields a dict of either str or List[str] where the key is a parent tag
        and the value is a list of child tags if any, or the parent tag itself if no children.
        """

        all_tags: Dict[str, List[str]] = defaultdict(list)
        datasource_datadict_tags = {**self.tag_map_datadict, **self.tag_map_datasource}
        for tag_list in datasource_datadict_tags.values():
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
                "tags": [Tag(name=root_tag).dict(by_alias=True, skip_defaults=True)]
            }

        return {
            "rootTag": {"name": root_tag, "deleteHierarchy": False},
            "tags": [
                Tag(name=child).dict(by_alias=True, skip_defaults=True)
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
