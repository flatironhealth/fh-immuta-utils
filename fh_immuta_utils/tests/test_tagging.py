from collections import namedtuple
from unittest import mock
from typing import Dict, List, Any
import pytest

import fh_immuta_utils.tagging as tg
import fh_immuta_utils.data_source as ds

TAG_MAP = {"col_foo": ["foo", "foobar"], "col_bar": ["bar.baz"]}
DATA_SOURCE_TAGS = {"ath_succeed": ["eeny", "meeny.miny"], "rs_fail": ["moe"]}


@pytest.fixture
def tagger():
    with mock.patch("fh_immuta_utils.tagging.Tagger.read_configs", return_value=None):
        obj = tg.Tagger(config_root="")
        obj.tag_map_datadict = TAG_MAP
        obj.tag_map_datasource = DATA_SOURCE_TAGS
    return obj


@pytest.mark.parametrize("tag,is_root", [("foo", False), ("bar", True)])
def test_is_root_tag(tagger: tg.Tagger, tag: str, is_root: bool):
    assert tagger.is_root_tag(tag) == is_root


@pytest.mark.parametrize(
    "col,expected", [("col_foo", TAG_MAP["col_foo"]), ("bad_col", [])]
)
def test_get_tags_for_column(tagger: tg.Tagger, col: str, expected: List[str]):
    assert tagger.get_tags_for_column(column_name=col) == expected


def test_tags_to_make(tagger):
    assert list(tagger.tags_to_make()) == [
        ("foo", []),
        ("foobar", []),
        ("bar", ["bar.baz"]),
        ("eeny", []),
        ("meeny", ["meeny.miny"]),
        ("moe", []),
    ]


def test_get_tags_for_data_source(tagger):
    expected = [
        {"name": "eeny", "source": "curated"},
        {"name": "meeny.miny", "source": "curated"},
    ]
    assert tagger.get_tags_for_data_source(name="ath_succeed_foo") == expected
    assert tagger.get_tags_for_data_source(name="ath_fail_bar") == []


TagMsgBody = namedtuple("TagMsgBody", ["root_tag", "children", "expected"])
TAG_MSG_BODY = [
    TagMsgBody(root_tag="foo", children=[], expected={"tags": [{"name": "foo"}]}),
    TagMsgBody(
        root_tag="bar",
        children=["baz"],
        expected={
            "rootTag": {"name": "bar", "deleteHierarchy": False},
            "tags": [{"name": "baz"}],
        },
    ),
    TagMsgBody(
        root_tag="foobar",
        children=["foo", "bar"],
        expected={
            "rootTag": {"name": "foobar", "deleteHierarchy": False},
            "tags": [{"name": "foo"}, {"name": "bar"}],
        },
    ),
]


@pytest.mark.parametrize("root_tag,children,expected", TAG_MSG_BODY)
def test_create_message_body_for_tag_creation(
    tagger: tg.Tagger, root_tag: str, children: List[str], expected: Dict[str, Any]
):
    assert (
        tagger.create_message_body_for_tag_creation(
            root_tag=root_tag, children=children
        )
        == expected
    )


def test_enrich_columns_with_tagging(tagger: tg.Tagger):
    columns = [
        ds.DataSourceColumn(name="col_foo", dataType="", remoteType="", nullable=False),
        ds.DataSourceColumn(name="col_bar", dataType="", remoteType="", nullable=False),
        ds.DataSourceColumn(name="bad_col", dataType="", remoteType="", nullable=False),
    ]
    enriched_cols = tagger.enrich_columns_with_tagging(columns)
    assert len(enriched_cols) == len(columns)
    for col in enriched_cols:
        assert len(col.tags) == len(TAG_MAP.get(col.name, []))
