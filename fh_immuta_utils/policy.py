from typing import Dict, Any, Optional, List, Set

from pydantic import BaseModel, Field

from .tagging import Tagger


class PolicyGroup(BaseModel):
    name: str
    iam: str


class ColumnTag(BaseModel):
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
    tag: ColumnTag


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
    allowDiscovery: bool = True


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
    name: str
    type: str = "subscription"
    actions: List[Dict]


def make_policy_exceptions(
    iam_groups: List[str], operator: str = "or"
) -> PolicyExceptions:
    conditions = []
    for group in iam_groups:
        # TODO: Generalize for other IAMs
        conditions.append(GroupCondition(group=PolicyGroup(name=group, iam="okta")))
    # TODO: Support other operators
    return PolicyExceptions(operator=operator, conditions=conditions)


def make_policy_circumstance(
    tags: List[str], tagger: Tagger
) -> List[PolicyCircumstance]:
    circumstances = []
    for tag in tags:
        circumstances.append(
            ColumnTagCircumstance(
                operator="or",
                columnTag=ColumnTag(name=tag, hasLeafNodes=tagger.is_root_tag(tag)),
            )
        )
    return circumstances


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


def make_global_subscription_policy(
    policy_name: str, tags: List[str], iam_groups: List[str], tagger: Tagger
) -> GlobalSubscriptionPolicy:
    actions: List[SubscriptionPolicyAction] = []
    actions.append(
        SubscriptionPolicyAction(
            type="subscription",
            subscriptionType="policy",
            exceptions=make_policy_exceptions(iam_groups=iam_groups),
        )
    )
    return GlobalSubscriptionPolicy(
        name=policy_name,
        circumstances=make_policy_circumstance(tags=tags, tagger=tagger),
        actions=actions,
    )


def make_policy_rule(
    rule_type: str, iam_groups: List[str], tags: List[str], tagger: Tagger
) -> PolicyRule:
    config_rule_fields: List[ColumnTag] = []
    for tag in tags:
        config_rule_fields.append(
            ColumnTag(name=tag, hasLeafNodes=tagger.is_root_tag(tag))
        )

    return PolicyRule(
        type=rule_type,
        exceptions=make_policy_exceptions(iam_groups=iam_groups, operator="and"),
        # TODO: Support other types
        config=MaskingRuleConfig(
            fields=config_rule_fields,
            maskingConfig=MaskingConfig(type="Consistent Value"),
        ),
    )


def make_global_data_policy(
    policy_name: str, tags: List[str], iam_groups: List[str], tagger: Tagger
) -> GlobalDataPolicy:
    actions: List[MaskingAction] = []
    actions.append(
        MaskingAction(
            type="masking",
            rules=[
                make_policy_rule(
                    rule_type="masking", iam_groups=iam_groups, tags=tags, tagger=tagger
                )
            ],
        )
    )
    return GlobalDataPolicy(
        name=policy_name,
        circumstances=make_policy_circumstance(tags=tags, tagger=tagger),
        actions=actions,
    )
