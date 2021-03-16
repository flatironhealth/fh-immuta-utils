# 0.4.0 (2020-03-01)

## Compatible with Immuta version `>=2020.3.4`

### Added
- Ability to control schema evolution settings for a remote database/server from configuration.
### Changed
- Immuta data source and query engine table name truncation character limits. Limits for both are now set to 255 to
  align with the accepted limits by the Immuta application. Users with previously truncated data source or query engine
  table names should delete and re-enroll those data sources to enroll the full names.
