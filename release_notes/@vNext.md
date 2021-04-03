# 0.5.0 (2021-03-24)

## Compatible with Immuta version `<=2020.3.4`

### Changed
- BREAKING CHANGE: Data source-level tag keys defined in dataset configuration file will match data source names
  using Unix shell-style wildcard matching. This aligns with data source schema and table prefix matching used for the
  `schemas_to_enroll` and `schemas_to_bulk_enroll` keys.
- BREAKING CHANGE: Data sources will enroll under their original schema name from the remote database in the Query
  Engine unless the `query_engine_target_schema` parameter is provided to override in the dataset configuration file.
  This aligns with the Immuta UI default value.
- BREAKING CHANGE: Opt-in is now required in the dataset configuration file to prefix Query Engine table names with
  the handler and schema using the `prefix_query_engine_names_with_schema` and `prefix_query_engine_names_with_handler`
  flags. This aligns with the Immuta UI default values.
- BREAKING CHANGE: The default value for schema monitoring template parameter `query_engine_table_name_format_default`
  no longer includes handler and schema prefixes. This aligns with the Immuta UI default value.
### Fixed
- Data source-level tags apply only to data sources created by the dataset configuration file in which they are defined.
  This now matches what the documentation originally stated.
- `user_prefix` from dataset configuration files is now being pulled correctly for schema monitoring templating.
