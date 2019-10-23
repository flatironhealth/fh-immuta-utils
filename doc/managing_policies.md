Policies
========

fh-immuta-utils supports creating global policies to limit access to data sources based on tags.
The intended use-case is where access to sensitive data is granted on the basis of membership in IAM groups,
where sensitive columns in data sources are tagged as such.

The repo provides a script that can create/remove policies based on provided tagging spec files.

As an example, imagine that the following tag spec file is used for tagging data sources:

``` yaml
---
TAG_MAP:
  ssn:
    - phi.ssn

TAG_GROUPS:
  phi.ssn:
    - "Medical Staff"
    - "Researchers"
```

The result of applying these tags would be that all columns named 'ssn' will have a tag called 'phi.ssn' attached to them.

To apply RBAC based on that tagging, you can run the following:

``` bash
$ conda activate fh-immuta-utils
$ python fh_immuta_utils/scripts/manage_policies.py --config-file foo.yml
```

Running the script above will generate a new global policy named `phi.ssn` that will specify that
for all columns tagged with `phi.ssn`, only users in IAM groups `Medical Staff` and `Researchers` should have access.
