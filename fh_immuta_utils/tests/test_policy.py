import os
from unittest import mock

import pytest

import fh_immuta_utils.policy as pol
import fh_immuta_utils.tagging as tg


@pytest.fixture
def tagger():
    with mock.patch("fh_immuta_utils.tagging.Tagger.read_configs", return_value=None):
        obj = tg.Tagger(config_root="")
        obj.tag_map_datadict = {}
        obj.tag_map_datasource = {}
    return obj


@pytest.fixture
def policy_dict():
    return pol.PolicyConfig(
        config_root=f"{os.path.dirname(os.path.abspath(__file__))}/fixtures"
    )


@pytest.fixture
def data_policy_actions_dict(policy_dict):
    return policy_dict.data_policy_config["correct_policy"]["actions"]


@pytest.fixture
def data_policy_circumstances_dict(policy_dict):
    return policy_dict.data_policy_config["correct_policy"]["circumstances"]


@pytest.fixture
def subscription_policy_actions_dict(policy_dict):
    return policy_dict.subscription_policy_config["valid_db_sub_policy"]["actions"]


@pytest.fixture
def subscription_policy_staged_bool(policy_dict):
    return policy_dict.subscription_policy_config["valid_db_sub_policy"]["staged"]


def test_make_policy_exceptions(data_policy_actions_dict):
    conditions = data_policy_actions_dict[0]["rules"][0]["exceptions"]["conditions"]
    iam_groups = conditions[0]["iam_groups"]
    expected_exceptions = pol.PolicyExceptions(
        operator="and",
        conditions=[
            pol.GroupCondition(group=pol.PolicyGroup(name="group1", iam="okta"))
        ],
    )
    exceptions = pol.make_policy_exceptions(iam_groups=iam_groups, operator="and")
    assert exceptions == expected_exceptions


def test_make_policy_circumstance(data_policy_circumstances_dict, tagger):
    circumstance_type = data_policy_circumstances_dict[0]["type"]
    tags = data_policy_circumstances_dict[0]["tags"]
    expected_circumstance = pol.ColumnTagCircumstance(
        operator="or",
        columnTag=pol.ColumnTag(name=tags[0], hasLeafNodes=tagger.is_root_tag(tags[0])),
    )
    circumstance = pol.make_policy_circumstance(
        tag=tags[0],
        tagger=tagger,
        circumstance_type=circumstance_type,
    )
    assert circumstance == expected_circumstance


def test_make_policy_circumstance_bad_type(data_policy_circumstances_dict, tagger):
    circumstance_type = "foo"
    tags = data_policy_circumstances_dict[0]["tags"]
    with pytest.raises(ValueError):
        return pol.make_policy_circumstance(
            tag=tags[0],
            tagger=tagger,
            circumstance_type=circumstance_type,
        )


def test_make_policy_rule(data_policy_actions_dict, tagger):
    example_rule = data_policy_actions_dict[0]["rules"][0]
    rule_type = example_rule["type"]
    config_field_tags = example_rule["config"]["fields"]["tags"]
    exceptions_config = example_rule["exceptions"]
    iam_groups = exceptions_config["conditions"][0]["iam_groups"]
    operator = exceptions_config["operator"]

    expected_rule = pol.PolicyRule(
        type=rule_type,
        exceptions=pol.make_policy_exceptions(iam_groups=iam_groups, operator=operator),
        config=pol.MaskingRuleConfig(
            fields=[
                pol.ColumnTag(
                    name=config_field_tags[0],
                    hasLeafNodes=tagger.is_root_tag(config_field_tags[0]),
                )
            ],
            maskingConfig=pol.MaskingConfig(type="Consistent Value"),
        ),
    )
    rule = pol.make_policy_rule(
        rule_type=rule_type,
        exceptions_config=exceptions_config,
        config_field_tags=config_field_tags,
        tagger=tagger,
    )
    assert rule == expected_rule


def test_make_data_policy_action(data_policy_actions_dict, tagger):
    action_type = data_policy_actions_dict[0]["type"]
    rules_config = data_policy_actions_dict[0]["rules"]
    rule1 = rules_config[0]
    rule1_conditions = rule1["exceptions"]["conditions"]
    rule1_iam_groups = rule1_conditions[0]["iam_groups"]
    rule2 = rules_config[1]
    rule2_conditions = rule2["exceptions"]["conditions"]
    rule2_iam_groups = (
        rule2_conditions[0]["iam_groups"] + rule2_conditions[1]["iam_groups"]
    )
    expected_action = pol.MaskingAction(
        type=action_type,
        rules=[
            pol.PolicyRule(
                type=rule1["type"],
                exceptions=pol.make_policy_exceptions(
                    iam_groups=rule1_iam_groups,
                    operator=rule1["exceptions"]["operator"],
                ),
                config=pol.MaskingRuleConfig(
                    fields=[
                        pol.ColumnTag(
                            name=rule1["config"]["fields"]["tags"][0],
                            hasLeafNodes=tagger.is_root_tag(
                                rule1["config"]["fields"]["tags"][0]
                            ),
                        )
                    ],
                    maskingConfig=pol.MaskingConfig(type="Consistent Value"),
                ),
            ),
            pol.PolicyRule(
                type=rule2["type"],
                exceptions=pol.make_policy_exceptions(
                    iam_groups=rule2_iam_groups,
                    operator=rule2["exceptions"]["operator"],
                ),
                config=pol.MaskingRuleConfig(
                    fields=[
                        pol.ColumnTag(
                            name=rule2["config"]["fields"]["tags"][0],
                            hasLeafNodes=tagger.is_root_tag(
                                rule2["config"]["fields"]["tags"][0]
                            ),
                        ),
                        pol.ColumnTag(
                            name=rule2["config"]["fields"]["tags"][1],
                            hasLeafNodes=tagger.is_root_tag(
                                rule2["config"]["fields"]["tags"][1]
                            ),
                        ),
                    ],
                    maskingConfig=pol.MaskingConfig(type="Consistent Value"),
                ),
            ),
        ],
    )
    action = pol.make_data_policy_action(
        action_type=action_type,
        rules_config=rules_config,
        tagger=tagger,
    )
    assert action == expected_action


def test_make_data_policy_action_bad_type(data_policy_actions_dict, tagger):
    action_type = "foo"
    rules_config = data_policy_actions_dict[0]["rules"]
    with pytest.raises(ValueError):
        return pol.make_data_policy_action(
            action_type=action_type,
            rules_config=rules_config,
            tagger=tagger,
        )


def test_make_subscription_policy_action(subscription_policy_actions_dict, tagger):
    exceptions_config = subscription_policy_actions_dict[0]["exceptions"]
    exception_conditions = exceptions_config["conditions"]
    iam_groups = exception_conditions[0]["iam_groups"]
    allow_discovery = subscription_policy_actions_dict[0]["allowDiscovery"]
    automatic_subscription = subscription_policy_actions_dict[0][
        "automaticSubscription"
    ]

    expected_action = pol.SubscriptionPolicyAction(
        type="subscription",
        subscriptionType="policy",
        allowDiscovery=False,
        automaticSubscription=True,
        exceptions=pol.make_policy_exceptions(
            iam_groups=iam_groups, operator=exceptions_config["operator"]
        ),
    )
    action = pol.make_subscription_policy_action(
        exceptions_config, allow_discovery, automatic_subscription, tagger
    )
    assert action == expected_action


def test_subscription_policy_staged(subscription_policy_staged_bool):
    assert subscription_policy_staged_bool == False
