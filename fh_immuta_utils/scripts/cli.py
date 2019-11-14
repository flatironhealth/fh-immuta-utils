"""Global cli combining all of the commands into one larger piece

"""

import click

from .bulk_delete_data_source import main as bulk_delete_data_source_command
from .manage_data_sources import cli_entrypoint as manage_data_sources_command
from .manage_policies import cli_entrypoint as manage_policies_command
from .tag_existing_data_sources import (
    cli_entrypoint as tag_existing_data_sources_command,
)

main_cli = click.Group()
data_source_cli = click.Group(
    "data-source", help="Manage data sources along with their tagging"
)
data_source_cli.add_command(bulk_delete_data_source_command, "bulk-delete")
data_source_cli.add_command(manage_data_sources_command, "manage")
data_source_cli.add_command(tag_existing_data_sources_command, "tag-existing")
main_cli.add_command(data_source_cli, "data-source")
main_cli.add_command(manage_policies_command, "policies")

if __name__ == "__main__":
    main_cli()
