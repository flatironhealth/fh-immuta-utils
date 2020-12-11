# fh-immuta-utils Changelog

## 0.3.0 (2020-12-11)
### Added
- BREAKING CHANGE (compatible with Immuta version `>=2020.3.2`): Allow for automatic subscription parameter to be read 
from subscription policy configs. Automatic subscription for subscription policies defaults to `True` to stay in line 
with the Immuta API and UI. 
### Fixed
- Change `iam` parameter for `PolicyGroup` to optional to address Immuta API no longer returning the `iam` parameter 
when getting policies.

## 0.2.0 (2020-11-23)
### Added
- Add ability to create subscription policies from configs

## 0.1.0 (2020-10-07)
### Changed
- BREAKING CHANGE: Refactor data policy management. Data policy configurations are now logically separated from tagging 
configurations, and match the payload expected by the Immuta API.