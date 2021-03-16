# Configuration

## Base Config File
The scripts inside fh-immuta-utils expect a config file specifying how to connect to your Immuta instance as well as the details around whatever state you're trying to manage.

An example config file is as follows:

``` yaml
---
base_url: immuta.foo.com
config_root: configs
auth_config:
  scheme: ApiKeyAuth
  apiKey: MyApiKey
```

The `config_root` key specifies the directory containing all the state configuration for things like data sources, tags, etc.

The supported auth schemes can be found in `config.py`.

## Data Source State

All specs for enrolled data sources should be placed inside a directory named `enrolled_datasets` within the `config_root`.
fh-immuta-utils globs for all YAML files within that directory and then processes each.

### Schema Evolution

Schema evolution (aka schema monitoring) can be enabled or disabled across all query-backed data sources for a remote
database/server. For more information on schema evolution, please refer to the internal Immuta documentation page titled
"Schema Monitoring".

Note the following:

* Enabling or disabling schema evolution requires a schema value is present in the `schemas_to_bulk_enroll` key in the
  configuration. **When enabling schema evolution for the first time for a remote database**, a bulk enroll of a
  non-enrolled schema must be run to correctly create the schema evolution record in Immuta
* Data sources enrolled with schema evolution automatically have table evolution enabled. See the internal Immuta
  documentation page titled "Schema Monitoring" for more information on table evolution.
* Schemas and tables are enrolled with the currently authenticated user running `fh-immuta-utils` as the owner
* Enabling or disabling schema evolution for specific schemas within a remote database/server is not currently
  supported

Defaults for the naming templates are shown in the example configurations below. Any of the macros shown in the UI in
the schema evolution section when creating or editing a data source can be used in the template strings. The available
template strings are:

1. **datasource_name_format**: Template for Immuta data source names
2. **query_engine_table_name_format**: Template for Query Engine SQL table names
3. **query_engine_schema_name_format**: Template for the Query Engine schema to use when enrolling tables

### Example Query-Backed Data Source Configurations

#### PostgreSQL

An example of a state file for a PostgreSQL database is as follows:

``` yaml
hostname: my-database.foo.com
port: 5439
database: db-name
# Prefix to prepend to name of data source created in Immuta
user_prefix:
handler_type: PostgreSQL
# List of schemas to enroll where for each schema,
# we only want to enroll tables with a specific name/prefix
schemas_to_enroll:
  # Will glob in database for all schemas starting with this prefix.
  # - schema_prefix: "foo"
  #   table_prefix: "bar"
# List of schemas where we want to enroll all tables in each schema
schemas_to_bulk_enroll:
  - schema_prefix: "baz"
# Schema evolution enablement and naming templates
schema_evolution:
  disable_schema_evolution: true
  datasource_name_format: "<user_prefix>_<handler_prefix>_<schema>_<tablename>"
  query_engine_table_name_format: "<user_prefix>_<handler_prefix>_<schema>_<tablename>"
  query_engine_schema_name_format: "<schema>"
credentials:
  # Read from environment variable
  source: ENV
  key: USER_PASSWORD
  username: service_user
# Tags to apply directly to data sources created by this config file.
# Key follows the pattern <prefix>_<schema>, where prefix matches with PREFIX_MAP in data_source.py
tags:
  pg_baz: ["tag1", "tag2.subtag2"]
  pg_foo: ["tag3", "tag4"]
```

**Note:** For AWS Redshift, use the same format as above, replacing the `handler_type` value with `Redshift`.

#### AWS Athena

An example of a state file for an AWS Athena database is as follows:

``` yaml
---
# Should be the region where your Athena database lives
region: us-east-1
hostname: us-east-1
database: my-database
# Prefix to prepend to name of data source created in Immuta
user_prefix:
handler_type: Amazon Athena
queryResultLocationBucket: bucket-where-results-should-be-stored
queryResultLocationDirectory: prefix-in-bucket-for-storing-results
# List of schemas to enroll where for each schema,
# we only want to enroll tables with a specific name/prefix
schemas_to_enroll:
  # Will glob in database for all schemas starting with this prefix.
  - schema_prefix: foo
    table_prefix: bar
# List of schemas where we want to enroll all tables in each schema
schemas_to_bulk_enroll:
# Schema evolution enablement and naming templates
schema_evolution:
  disable_schema_evolution: true
  datasource_name_format: "<user_prefix>_<handler_prefix>_<schema>_<tablename>"
  query_engine_table_name_format: "<user_prefix>_<handler_prefix>_<schema>_<tablename>"
  query_engine_schema_name_format: "<schema>"
credentials:
  # Read from an instance of Hashicorp Vault
  source: VAULT
  key: path/to/vault/secret
# Tags to apply directly to data sources created by this config file.
# Key follows the pattern <prefix>_<schema>, where prefix matches with PREFIX_MAP in data_source.py
tags:
  ath_foo: ["tag1", "tag2.subtag2"]
```

## Data Source Column Tags

Information about what tags to create and how to attach them to columns in data sources should be specified through YAML files created under a `tags` directory within `config_root`.

An example of a file containing the required bits of tagging information is as follows:

``` yaml
# Mapping of data source column names to Immuta tags
# All columns found with the following keys in any data source will have the specified list of tags attached to them.
TAG_MAP:
  ssn:
    - phi.ssn
  dob:
    - phi.dob

```
