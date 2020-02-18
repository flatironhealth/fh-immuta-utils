#!/usr/bin/env xonsh
$PROJECT = 'fh-immuta-utils'
$ACTIVITIES = [
	'version_bump',
	'tag',
	'pypi',
	'push_tag',
	'ghrelease'
]

$GITHUB_ORG = 'flatironhealth'
$GITHUB_REPO = 'fh-immuta-utils'

$VERSION_BUMP_PATTERNS = [
   # These note where/how to find the version numbers
   ('fh_immuta_utils/__init__.py', '__version__\s*=.*', '__version__ = "$VERSION"'),
   ('setup.py', 'version\s*=.*,', 'version="$VERSION",'),
   ('Dockerfile', 'ENV LIBRARY_VERSION.*', 'ENV LIBRARY_VERSION $VERSION'),
   ('conda.recipe/meta.yaml', 'version:\s*:.*', 'version: "$VERSION"'),
]
