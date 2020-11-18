#!/usr/bin/env python

"""
Script that can be invoked to create, update or delete existing governance policies.
"""

import logging
from typing import Optional, TYPE_CHECKING

from tqdm import tqdm
import click

from fh_immuta_utils.client import get_client
from fh_immuta_utils.config import parse_config
from fh_immuta_utils.tagging import Tagger
from fh_immuta_utils.policy import (
    make_global_data_policy,
    make_global_subscription_policy,
    GlobalPolicy,
    PolicyConfig,
)

if TYPE_CHECKING:
    from fh_immuta_utils.client import ImmutaClient


@click.command(help="Create/Update policies that specify RBAC rules")
@click.option("--config-file", required=True)
@click.option(
    "--search-text",
    help=(
        "When deleting, will match all policies that contain this string anywhere in"
        " their name. Ignored otherwise."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log the policies that would be updated without affecting any change",
)
@click.option(
    "--delete", is_flag=True, default=False, help="Delete any matching policies"
)
@click.option("--debug", is_flag=True, default=False, help="Debug logging")
@click.option(
    "--type",
    is_flag=False,
    default="both",
    help="Apply data or subscription or both policies",
)
def cli_entrypoint(
    config_file: str,
    search_text: str,
    dry_run: bool,
    delete: bool,
    debug: bool,
    type: str,
):
    return main(
        config_file=config_file,
        search_text=search_text,
        dry_run=dry_run,
        delete=delete,
        debug=debug,
        type=type,
    )


def main(
    config_file: str,
    search_text: str,
    dry_run: bool,
    delete: bool,
    debug: bool,
    type: str,
):
    logging.basicConfig(
        format="[%(name)s][%(levelname)s][%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=(logging.DEBUG if debug else logging.INFO),
    )
    config = parse_config(config_file=config_file)
    client = get_client(**config)

    if delete:
        if search_text is None:
            logging.error(
                "--delete must be invoked with an explicit value for --search-text"
            )
            return False
        return delete_existing_policies(
            client=client, dry_run=dry_run, search_text=search_text, debug=debug
        )
    else:
        return create_or_update_policies(
            client=client,
            config_root=config["config_root"],
            dry_run=dry_run,
            debug=debug,
            type=type,
        )


def delete_existing_policies(
    client: "ImmutaClient", dry_run: bool, search_text: Optional[str], debug: bool
) -> bool:
    progress_iterator = tqdm(client.get_global_policies(search_text=search_text))

    for policy in progress_iterator:
        progress_iterator.set_description(desc=f"Policy {policy.name}")
        logging.info(f"Deleting policy with name {policy.name}, ID: {policy.id}")
        if not dry_run:
            client.delete_global_policy(id=policy.id)
    logging.info("Fin.")
    return True


def create_or_update_policies(
    client: "ImmutaClient", config_root: str, dry_run: bool, debug: bool, type: str
) -> bool:
    logging.info("Gathering existing policies")
    existing_policies = {}

    for policy in client.get_global_policies():
        existing_policies[policy.name] = policy

    logging.debug(f"Existing policies: {existing_policies.keys()}")

    tagger = Tagger(config_root=config_root)
    policy_config = PolicyConfig(config_root=config_root)

    # if it is not subscription-only, do the data-policies work
    if type != "subscription":
        create_or_update_data_policies(
            client=client,
            dry_run=dry_run,
            tagger=tagger,
            existing_policies=existing_policies,
            policy_config=policy_config,
        )

    # if it is not data-only, do the subscription-policies work
    if type != "data":
        create_or_update_subscription_policies(
            client=client,
            dry_run=dry_run,
            tagger=tagger,
            existing_policies=existing_policies,
            policy_config=policy_config,
        )

    logging.info("Fin.")
    return True


def create_or_update_single_policy(
    client: "ImmutaClient",
    dry_run: bool,
    existing_policies: dict,
    policy_name: str,
    policy: GlobalPolicy,
) -> bool:
    logging.debug(f"Policy to create/update: {policy.json()}")
    if policy_name in existing_policies.keys():
        policy.id = existing_policies[policy_name].id
        logging.debug(f"Existing policy: {existing_policies[policy_name].json()}")
        if existing_policies[policy_name] == policy:
            logging.info(f"No change for policy {policy_name}. Skipping.")
            return False
        logging.info(f"Updating existing policy with name {policy_name}.")
        if not dry_run:
            client.update_global_policy(
                policy=policy, id=existing_policies[policy_name].id
            )
    else:
        logging.info(f"Creating new policy with name {policy_name}.")
        if not dry_run:
            client.create_global_policy(policy=policy)
    return True


def create_or_update_data_policies(
    client: "ImmutaClient",
    dry_run: bool,
    tagger: Tagger,
    existing_policies: dict,
    policy_config: PolicyConfig,
) -> bool:

    progress_iterator = tqdm(policy_config.data_policy_config.keys())
    for data_policy in progress_iterator:
        progress_iterator.set_description(desc=f"Data Policy: {data_policy}")
        policy_name = f"{data_policy}_access_policy"
        policy = make_global_data_policy(
            policy_name=policy_name,
            policy_config=policy_config.data_policy_config[data_policy],
            tagger=tagger,
        )
        create_or_update_single_policy(
            client=client,
            dry_run=dry_run,
            existing_policies=existing_policies,
            policy_name=policy_name,
            policy=policy,
        )

    return True


def create_or_update_subscription_policies(
    client: "ImmutaClient",
    dry_run: bool,
    tagger: Tagger,
    existing_policies: dict,
    policy_config: PolicyConfig,
) -> bool:

    progress_iterator = tqdm(policy_config.subscription_policy_config.keys())
    for subscription_policy in progress_iterator:
        progress_iterator.set_description(
            desc=f"Subscription Policy: {subscription_policy}"
        )
        policy_name = f"{subscription_policy}_subscription_policy"
        policy = make_global_subscription_policy(
            policy_name=policy_name,
            policy_config=policy_config.subscription_policy_config[subscription_policy],
            tagger=tagger,
        )
        create_or_update_single_policy(
            client=client,
            dry_run=dry_run,
            existing_policies=existing_policies,
            policy_name=policy_name,
            policy=policy,
        )

    return True


if __name__ == "__main__":
    cli_entrypoint()
