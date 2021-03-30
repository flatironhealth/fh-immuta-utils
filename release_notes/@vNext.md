# 0.5.0 (2021-03-24)

## Compatible with Immuta version `<=2020.3.4`

### Added

### Changed
- BREAKING CHANGE: Data source-level tag keys defined in dataset configuration file will match data source names 
  using Unix shell-style wildcard matching. This aligns with data source schema and table prefix matching used for the
  `schemas_to_enroll` and `schemas_to_bulk_enroll` keys.
- BREAKING CHANGE: Data sources will enroll under their original schema name from the remote database in the Query
  Engine unless the `query_engine_target_schema` parameter is provided to override in the dataset configuration file.
  This aligns with the Immuta UI default value.
- BREAKING CHANGE: Opt in is now required in the dataset configuration file to prefix data source and query engine table
  names with handler and schema names using the `prefix_names_with_schema` and `prefix_names_with_handler` flags. This
  aligns with the Immuta UI default values.
- BREAKING CHANGE: Defaults for schema monitoring template parameters `datasource_name_format_default` and
  `query_engine_table_name_format_default` do not include handler and schema name prefixes to align with the Immuta UI
  default values.
### Fixed
- Data source-level tags apply only to data sources created by the dataset configuration file in which they are defined.
  This now matches what the documentation originally stated.
- `user_prefix` from dataset configuration files is now being pulled correctly for schema monitoring templating.
  