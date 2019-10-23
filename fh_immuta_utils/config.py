""" Provides methods to read immuta-utils config files """
import os
from typing import Dict, Any

import yaml

from .exceptions import BadImmutaConfigException

REQUIRED_KEYS = ["base_url", "config_root", "auth_config"]


def parse_config(config_file: str) -> Dict[str, Any]:
    """ Validates the given config file and returns as dict """
    with open(config_file) as handle:
        config = yaml.safe_load(handle)
    for key in REQUIRED_KEYS:
        if key not in config:
            raise BadImmutaConfigException(
                f"Must specify value for {key} in config file {config_file}"
            )
    # If config_root is relative, replace with absolute path
    if not os.path.isabs(config["config_root"]):
        config["config_root"] = os.path.abspath(
            os.path.join(os.path.dirname(config_file), config["config_root"])
        )
    config["config_root"] = os.path.abspath(config["config_root"])
    auth_config = config["auth_config"]
    if auth_config["scheme"] == "ApiKeyAuth":
        if "apiKey" not in auth_config:
            raise BadImmutaConfigException(
                "apiKey required when using key-based auth scheme"
            )
    if auth_config["scheme"] == "UsernamePasswordAuth":
        for key in ["username", "password", "iamid"]:
            if key not in auth_config:
                raise BadImmutaConfigException(
                    f"Must specify value for {key} when using UsernamePasswordAuth"
                )
    if auth_config["scheme"] == "OAuth2Auth":
        for key in ["refresh_token", "client_id", "client_secret"]:
            if key not in auth_config:
                raise BadImmutaConfigException(
                    f"Must specify value for {key} when using OAuth2Auth"
                )
    return config
