# 0.5.2 (2021-08-03)

## Compatible with Immuta version `>=2021.1.2`

### Added
- Ability to search by data source schema when bulk deleting or tagging data sources

### Fixed
- Bug preventing remote columns from persisting after a column is tagged
- Bug that forced data source dictionary updates when tagging because comparisons were failing
- Bug preventing correct evaluation of the schema evolution status check for Snowflake remote databases
- Data source deletion logic to account for updated API response to allow for hard deletes
