Managing data sources
=====================

# Creation

Assuming that you've created state files for data sources as specified in the [Configuration](managing_configs.md) docs,
you can invoke a script within the repo to manage all datasets for you in your Immuta instance.

``` bash
$ fh-immuta-utils data-sources manage --config-file foo.yml
```

For a list of supported args, please run with `--help`.

# Deletion

``` bash
$ fh-immuta-utils data-sources bulk-delete --config-file foo.yml --search-text foo
```

Note that unless triggered with `--hard-delete`, the script will only disable a data source in Immuta and not actually delete it.
For a list of supported args, please run with `--help`.

# Tagging

Tagging of data sources either during or post-creation.
If data sources are created using fh-immuta-utils, they'll be tagged during creation.
Note that tagging is not done for data sources that are bulk-created, as the endpoint used in Immuta's API for that purpose
doesn't support applying tags during creation.


To ensure that tags are kept up-to-date in existing data sources, you can run the following script:

``` bash
$ fh-immuta-utils data-sources tag-existing --config-file foo.yml
```
For a list of supported args, please run with `--help`.
