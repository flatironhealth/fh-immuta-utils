Policies
========

fh-immuta-utils supports creating global policies to limit access to data sources based on tags.
The intended use-case is where access to sensitive data is granted on the basis of membership in IAM groups,
where sensitive columns in data sources are tagged as such.

The repo provides a script that can create, update, and remove policies based on provided policy spec files.

As an example, imagine that the following data policy spec file is used:

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
$ fh-immuta-utils policies --config-file foo.yml
```

Running the script above will generate a new global data policy named `policy_1_access_policy` that will: 
* **action** --> mask fields tagged `tag1` using hashing for everyone except when user is a member of group `group1`
* **circumstance** --> on data sources with columns tagged `tag1`

Operators allow for stringing together separate conditions, and separate circumstances. The accepted values are `or` and `and`.

Available types:
* `Masking` is the only currently implemented `action` and `rule` type.
* `Groups` is the only currently implemented `condition` type.
* `columnTags` and `tags` are the only currently implemented `circumstance` types:
  * `columnTags` applies the policy to data sources with specific column tags
  * `tags` applies the policy to data sources with specific tags on the data source itself
