# Policies

## Data Policies

fh-immuta-utils supports creating global policies to limit access to data sources based on tags.
The intended use-case is where access to sensitive data is granted on the basis of membership in IAM groups,
where sensitive columns in data sources are tagged as such.

The repo provides a script that can create, update, and remove policies based on provided policy spec files.

As an example, consider the following data policy:

``` yaml
DATA_POLICIES:
  policy_1:
    actions:
      - type: "masking"  # mask fields
        description: ""
        rules:
          - type: "masking"  # using hashing
            config:
              fields:
                tags: ["tag1"]  # for fields tagged with "tag1"
            exceptions:
              operator: "and"
              conditions:
                - type: "groups"
                  iam_groups: ["group1"]  # except if user is a member of "group1"
    circumstances:
      - operator: "or"
        type: "columnTags"
        tags: ["tag1"]  # on data sources with columns tagged "tag1"
```

To apply RBAC based on this policy, you can run the following:

``` bash
$ conda activate fh-immuta-utils
$ fh-immuta-utils policies --config-file foo.yml --type data
```

Running the script above will generate a new global data policy named `policy_1_access_policy` that will:
* **action** --> mask fields tagged `tag1` using hashing for everyone except when user is a member of group `group1`
* **circumstance** --> on data sources with columns tagged `tag1`

The policy can be configured many ways. However, only the following are currently supported values by `fh-immuta-utils`:

1. **Operators**: these allow for stringing together separate conditions, and separate circumstances. The supported values are `or` and `and`.
2. **Types**:
    1. `Masking` is the only currently supported `action` and `rule` type.
    2. `Groups` is the only currently supported `condition` type.
    3. `columnTags` and `tags` are the only currently supported `circumstance` types:
        1. `columnTags` applies the policy to data sources with specific column tags
        2. `tags` applies the policy to data sources with specific tags on the data source itself

## Subscription Policies

fh-immuta-utils also allows creating subscription policies, where access to a data source is based on
membership in a particular IAM group. Subscription policies in Immuta dictate who can subscribe to data sources, 
which allows for querying those data sources. Similar to data policies, they are based on **actions** and **circumstances**:

* **Actions** define how the policy restricts, and for whom it restricts.
* **Circumstances** define where and how the policy is applied to data sources in Immuta.

As an example, consider the following subscription policy:

```yaml
SUBSCRIPTION_POLICIES:
  subscription_1:
    staged: false
    actions: # do not subscribe to a data source
      - exceptions:
          operator: "or"
          conditions:
            - type: "groups"
              iam_groups: ["group01"] # except if the user is member of "group01"
        allowDiscovery: false
        automaticSubscription: true
    circumstances:
      - operator: "or"
        type: "tags"
        tags: ["tag01"] # for data sources tagged with "tag01"
```
To apply RBAC based on this policy, you can run the following:

``` bash
$ conda activate fh-immuta-utils
$ fh-immuta-utils policies --config-file foo.yml --type subscription
```

*Note*: To apply both data and subscription policies, skip the `--type` argument which defaults to
both data and subscription policies being applied.

Running the script above will generate a new global subscription policy named `subscription_1_subscription_policy` that will:
* **action** --> deny subscription except for users who are in the group `group01`
* **circumstance** --> for data sources tagged with `tag01`

Similar to data policies, subscription policies can be configured many ways. However, only the following are currently supported values by `fh-immuta-utils`:

1. **Operators**: these allow for stringing together separate exceptions, and separate circumstances. The supported values are `or` and `and`.
2. **Types**:
    1. `Groups` is the only currently supported `condition` type.
    2. `columnTags` and `tags` are the only currently supported `circumstance` types:
        1. `columnTags` applies the policy to data sources with specific column tags
        2. `tags` applies the policy to data sources with specific tags on the data source itself
