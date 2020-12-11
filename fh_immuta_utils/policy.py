import os
from typing import Dict, Any, Optional, List, Set
from enum import Enum, unique
import logging
import glob

import yaml
from pydantic import BaseModel, Field

from .tagging import Tagger


@unique
class CircumstanceType(Enum):
    TAG = "tags"
    COLUMN_TAG = "columnTags"


@unique
class ActionType(Enum):
    MASKING = "masking"


class PolicyGroup(BaseModel):
    name: str
    iam: Optional[str] = ""


class ColumnTag(BaseModel):
    name: str
    # Set to True if it's a root tag
    hasLeafNodes: bool


class DataSourceTag(BaseModel):
    name: str
    # Set to True if it's a root tag
    hasLeafNodes: bool


class PolicyAuthorization(BaseModel):
    auth: str
    value: str
    iam: str


class PolicyCondition(BaseModel):
    type: str


class GroupCondition(PolicyCondition):
    type: str = "groups"
    group: PolicyGroup
    field: Optional[str] = ""

    def dict(
        self,
        *,
        include: Set[str] = None,
        exclude: Set[str] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        """
        The 'field' key should not exist in the payload if it has no value
        as Immuta's API will reject the request. This is because group condition
        can be created in different contexts, some of which require setting 'field'
        and others where 'field' makes no sense.
        """
        if self.field == "":
            exclude = {"field"}
        return super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )


class AuthorizationCondition(PolicyCondition):
    type: str = "authorizations"
    authorization: PolicyAuthorization
    field: str


class PurposeCondition(PolicyCondition):
    type: str = "purposes"
    value: str
    field: str


class PolicyCircumstance(BaseModel):
    operator: str
    type: str


class ColumnTagCircumstance(PolicyCircumstance):
    type: str = "columnTags"
    columnTag: ColumnTag


class TagCircumstance(PolicyCircumstance):
    type: str = "tags"
    tag: DataSourceTag


class PolicyExceptions(BaseModel):
    # Must be one of 'and', 'or'
    operator: str
    conditions: List[PolicyCondition]


class PolicyRuleConfig(BaseModel):
    policy_fields: Optional[List[ColumnTag]] = Field(required=False, alias="fields")


class MaskingConfig(BaseModel):
    type: str
    # Unsure what this is
    metadata: Optional[Dict] = {}


class MaskingRuleConfig(PolicyRuleConfig):
    maskingConfig: MaskingConfig


class PolicyRule(BaseModel):
    type: str
    # Policies will not necessarily have any exceptions ("apply to everyone")
    exceptions: Optional[PolicyExceptions]
    config: PolicyRuleConfig


class PolicyAction(BaseModel):
    type: str


class SubscriptionPolicyAction(BaseModel):
    type: str = "subscription"
    subscriptionType: str
    exceptions: Optional[PolicyExceptions] = None
    allowDiscovery: bool = False
    automaticSubscription: bool = True


class DataPolicyAction(PolicyAction):
    rules: Optional[List[PolicyRule]] = None
    description: Optional[str] = ""


class MaskingAction(DataPolicyAction):
    type: str = "masking"


class GlobalPolicy(BaseModel):
    name: str
    id: Optional[int] = None
    type: str
    template: bool = False
    # Don't know what this is supposed to be.
    # No mention in the docs
    ownerRestrictions: Optional[Any] = None
    circumstances: List[PolicyCircumstance]

    def dict(
        self,
        *,
        include: Set[str] = None,
        exclude: Set[str] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        # Remove keys that should not exist in the payload if they have no values
        exclude = set()
        if not self.id:
            exclude.add("id")
        return super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )


class GlobalDataPolicy(GlobalPolicy):
    type: str = "data"
    actions: List[Dict]


class GlobalSubscriptionPolicy(GlobalPolicy):
    type: str = "subscription"
    staged: bool = True
    actions: List[Dict]


class PolicyConfig:
    """
    Wrapper around managing data & subscription policy configuration
    """

    def __init__(self, config_root: str) -> None:
        self.data_policy_config: Dict[str, Any] = {}
        self.subscription_policy_config: Dict[str, Any] = {}
        self.read_configs(config_root=config_root)

    def _read_data_configs(self, config_root: str) -> None:
        for data_policy_file in glob.glob(
            os.path.join(config_root, "policies/data", "*.yml")
        ):
            logging.debug("Reading data policy file: %s", data_policy_file)
            with open(data_policy_file) as handle:
                contents = yaml.safe_load(handle)
                self.data_policy_config = {
                    **self.data_policy_config,
                    **contents.get("DATA_POLICIES", {}),
                }

    def _read_subscription_configs(self, config_root: str) -> None:
        for subscription_policy_file in glob.glob(
            os.path.join(config_root, "policies/subscription", "*.yml")
        ):
            logging.debug(
                "Reading subscription policy file: %s", subscription_policy_file
            )
            with open(subscription_policy_file) as handle:
                contents = yaml.safe_load(handle)
                self.subscription_policy_config = {
                    **self.subscription_policy_config,
                    **contents.get("SUBSCRIPTION_POLICIES", {}),
                }

    def read_configs(self, config_root: str) -> None:
        self._read_data_configs(config_root=config_root)
        self._read_subscription_configs(config_root=config_root)


def make_policy_exceptions(
    iam_groups: List[str], operator: str = "or"
) -> PolicyExceptions:
    conditions = []
    for group in iam_groups:
        # TODO: Generalize for other IAMs and conditions
        conditions.append(GroupCondition(group=PolicyGroup(name=group, iam="okta")))
    return PolicyExceptions(operator=operator, conditions=conditions)


def make_policy_circumstance(
    tag: str, tagger: Tagger, circumstance_type: Any, operator: str = "or"
) -> Any:
    try:
        circumstance_type = CircumstanceType(circumstance_type)
    except ValueError:
        raise

    if circumstance_type is CircumstanceType.TAG:
        return TagCircumstance(
            operator=operator,
            tag=DataSourceTag(name=tag, hasLeafNodes=tagger.is_root_tag(tag)),
        )
    elif circumstance_type is CircumstanceType.COLUMN_TAG:
        return ColumnTagCircumstance(
            operator=operator,
            columnTag=ColumnTag(name=tag, hasLeafNodes=tagger.is_root_tag(tag)),
        )


def make_policy_object_from_json(json_policy: Dict[str, Any]) -> GlobalPolicy:
    circumstances = []
    exceptions: Optional[PolicyExceptions] = None
    if json_policy["circumstances"] is not None:
        for circumstance in json_policy["circumstances"]:
            if circumstance["type"] == "columnTags":
                circumstances.append(ColumnTagCircumstance(**circumstance))
            elif circumstance["type"] == "tags":
                circumstances.append(TagCircumstance(**circumstance))
            else:
                raise TypeError(
                    f"Unsupported type for policy circumstance: {circumstance['type']}"
                )

    actions = []
    for action in json_policy["actions"]:
        if action.get("exceptions"):
            conditions = []
            for condition in action["exceptions"]["conditions"]:
                if condition["type"] == "groups":
                    conditions.append(GroupCondition(**condition))
                elif condition["type"] == "authorizations":
                    conditions.append(AuthorizationCondition(**condition))
                else:
                    raise TypeError(
                        f"Unsupported type for policy condition: {condition['type']}"
                    )
            exceptions = PolicyExceptions(
                operator=action["exceptions"]["operator"], conditions=conditions
            )
        else:
            exceptions = None
        actions.append(action)

    if json_policy["type"] == "subscription":
        return GlobalSubscriptionPolicy(
            id=json_policy["id"],
            name=json_policy["name"],
            type=json_policy["type"],
            template=json_policy["template"],
            circumstances=circumstances,
            actions=actions,
        )
    elif json_policy["type"] == "data":
        return GlobalDataPolicy(
            id=json_policy["id"],
            name=json_policy["name"],
            type=json_policy["type"],
            template=json_policy["template"],
            circumstances=circumstances,
            actions=actions,
        )
    else:
        raise TypeError(f"Unsupported type for Global policy: {json_policy['type']}")


def make_subscription_policy_action(
    exceptions_config: Dict,
    allow_discovery: bool,
    automatic_subscription: bool,
    tagger: Tagger,
) -> SubscriptionPolicyAction:

    iam_groups = []
    for condition in exceptions_config["conditions"]:
        iam_groups.extend(condition["iam_groups"])
    return SubscriptionPolicyAction(
        type="subscription",
        subscriptionType="policy",
        allowDiscovery=allow_discovery,
        automaticSubscription=automatic_subscription,
        exceptions=make_policy_exceptions(
            iam_groups=iam_groups, operator=exceptions_config["operator"]
        ),
    )


def make_global_subscription_policy(
    policy_name: str, policy_config: Dict, tagger: Tagger
) -> GlobalSubscriptionPolicy:

    actions: List[SubscriptionPolicyAction] = []

    if policy_config.get("actions"):
        for action_grouping in policy_config["actions"]:
            action = make_subscription_policy_action(
                exceptions_config=action_grouping["exceptions"],
                allow_discovery=action_grouping.get("allowDiscovery", False),
                automatic_subscription=action_grouping.get(
                    "automaticSubscription", True
                ),
                tagger=tagger,
            )
            actions.append(action)
    else:
        raise KeyError(f"Missing actions for subscription policy: {policy_name}")

    circumstances: List[Any] = []
    if policy_config.get("circumstances"):
        for circumstance_grouping in policy_config["circumstances"]:
            for tag in circumstance_grouping["tags"]:
                circumstance = make_policy_circumstance(
                    tag=tag,
                    tagger=tagger,
                    circumstance_type=circumstance_grouping["type"],
                    operator=circumstance_grouping["operator"],
                )
                circumstances.append(circumstance)
    else:
        raise KeyError(f"Missing circumstances for subscription policy: {policy_name}")

    return GlobalSubscriptionPolicy(
        name=policy_name,
        circumstances=circumstances,
        actions=actions,
        staged=policy_config.get("staged", True),
    )


def make_policy_rule(
    rule_type: str,
    exceptions_config: Dict,
    config_field_tags: List[str],
    tagger: Tagger,
) -> PolicyRule:
    config_rule_fields: List[ColumnTag] = []
    # TODO: handle other types of config
    for tag in config_field_tags:
        config_rule_fields.append(
            ColumnTag(name=tag, hasLeafNodes=tagger.is_root_tag(tag))
        )

    # TODO: pass in conditions instead of iam_groups to make_policy_exceptions after subscription policy refactor
    iam_groups = []
    for condition in exceptions_config["conditions"]:
        iam_groups.extend(condition["iam_groups"])
    return PolicyRule(
        type=rule_type,
        exceptions=make_policy_exceptions(
            iam_groups=iam_groups,
            operator=exceptions_config["operator"],
        ),
        # TODO: Support other config types
        config=MaskingRuleConfig(
            fields=config_rule_fields,
            maskingConfig=MaskingConfig(type="Consistent Value"),
        ),
    )


def make_data_policy_action(
    action_type: Any, rules_config: Dict, tagger: Tagger
) -> MaskingAction:
    try:
        action_type = ActionType(action_type)
    except ValueError:
        raise

    rules = []
    for rule in rules_config:
        rules.append(
            make_policy_rule(
                rule_type=rule["type"],
                exceptions_config=rule["exceptions"],
                config_field_tags=rule["config"]["fields"]["tags"],
                tagger=tagger,
            )
        )

    if action_type is ActionType.MASKING:
        return MaskingAction(
            type=ActionType.MASKING.value,
            rules=rules,
        )
    else:
        raise TypeError(f"Unsupported type for action: {action_type}")


def make_global_data_policy(
    policy_name: str, policy_config: Dict, tagger: Tagger
) -> GlobalDataPolicy:
    """
    Returns a GlobalDataPolicy object containing lists of actions and circumstances.
    Actions define what the policy restricts, how it restricts, and for whom it restricts.
    Circumstances define where and how the policy is applied to data sources in Immuta.
    """
    actions: List[MaskingAction] = []
    circumstances: List[Any] = []

    if policy_config.get("actions"):
        for action_grouping in policy_config["actions"]:
            action = make_data_policy_action(
                action_type=action_grouping["type"],
                rules_config=action_grouping["rules"],
                tagger=tagger,
            )
            actions.append(action)
    else:
        raise KeyError(f"Missing actions for data policy: {policy_name}")

    if policy_config.get("circumstances"):
        for circumstance_grouping in policy_config["circumstances"]:
            for tag in circumstance_grouping["tags"]:
                circumstance = make_policy_circumstance(
                    tag=tag,
                    tagger=tagger,
                    circumstance_type=circumstance_grouping["type"],
                    operator=circumstance_grouping["operator"],
                )
                circumstances.append(circumstance)
    else:
        raise KeyError(f"Missing circumstances for data policy: {policy_name}")

    return GlobalDataPolicy(
        name=policy_name,
        circumstances=circumstances,
        actions=actions,
    )
