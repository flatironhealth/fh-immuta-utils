# 0.4.0 (2020-03-01)

## Compatible with Immuta version `>=2020.3.4`

### Added
- Ability to control schema evolution settings for a remote database/server from configuration.
### Removed
- Immuta data source and SQL table name truncation logic. SQL table name max length is now 255 characters in the Query
  Engine, and the Immuta data source name max length is at least 255 characters. Users with previously truncated data
  source or SQL table names should delete and re-enroll those data sources to enroll the full names.
