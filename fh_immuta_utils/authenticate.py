# Copyright 2018 Immuta, Inc. Licensed under the Immuta Software License
# Version 0.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
#    http://www.immuta.com/licenses/Immuta_Software_License_0.1.txt
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import abc
import logging
import os
import warnings
from typing import Dict, Any
import six.moves.urllib_parse as urlparse

import requests
from .exceptions import InvalidTokenError
from .exceptions import UnknownAuthenticationScheme
from .exceptions import ImmutaCredentialsError


class AuthScheme(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        self._session = kwargs.get("session")
        if not self._session:
            self._session = requests.Session()

    @abc.abstractmethod
    def authenticate(self, base_url, ca_certs):
        """Authenticate the user and return an auth token"""
        return ""

    def parse_token_from_response(self, resp_json):
        if not resp_json or "token" not in resp_json:
            raise InvalidTokenError("Unable to obtain token from Immuta")
        return resp_json["token"]

    def handle_response(self, response):
        if 400 <= response.status_code <= 600:
            error_msg = "Error fetching token %d, %s" % (
                response.status_code,
                response.reason,
            )
            raise InvalidTokenError(error_msg)
        return self.parse_token_from_response(response.json())


class UsernamePasswordAuth(AuthScheme):
    AUTH_URL_TEMPLATE = "bim/iam/{iamid}/user/authenticate"

    def __init__(self, iamid, username, password, **kwargs):
        super(UsernamePasswordAuth, self).__init__(**kwargs)
        self.iamid = iamid
        self.username = username
        self.password = password

    def authenticate(self, base_url, ca_certs):
        url = urlparse.urljoin(
            base_url, self.AUTH_URL_TEMPLATE.format(iamid=self.iamid)
        )
        auth_json = {"username": self.username, "password": self.password}
        response = self._session.post(url, json=auth_json, verify=ca_certs)
        return self.handle_response(response)


class ApiKeyAuth(AuthScheme):
    AUTH_URL_TEMPLATE = "bim/apikey/authenticate"

    def __init__(self, apikey, **kwargs):
        super(ApiKeyAuth, self).__init__(**kwargs)
        self.apikey = apikey

    def authenticate(self, base_url, ca_certs):
        url = urlparse.urljoin(base_url, self.AUTH_URL_TEMPLATE)
        response = self._session.post(
            url, json={"apikey": self.apikey}, verify=ca_certs
        )
        return self.handle_response(response)


class OAuth2Auth(AuthScheme):
    AUTH_URL_TEMPLATE = "bim/oauth/token"

    def __init__(self, refresh_token, client_id, client_secret):
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret

    def parse_token_from_response(self, resp_json):
        if not resp_json or "access_token" not in resp_json:
            raise InvalidTokenError("Unable to obtain access token from Immuta")
        return resp_json["access_token"]

    def authenticate(self, base_url, ca_certs):
        url = urlparse.urljoin(base_url, self.AUTH_URL_TEMPLATE)
        response = self._session.post(
            url,
            json={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            verify=ca_certs,
        )
        return self.handle_response(response)


class ImmutaRequestsAuth(requests.auth.AuthBase):
    """Immuta's authentication for the python requests module"""

    def __init__(self, base_url, auth_scheme, ca_certs):
        self.auth_scheme = auth_scheme
        self.token = None
        self.ca_certs = ca_certs
        self.base_url = base_url

    def _add_auth_header_and_retry(self, r, **kwargs):
        self.token = self.auth_scheme.authenticate(self.base_url, self.ca_certs)
        # We want to reuse the original request, so consume content and close the request
        r.content
        r.close()
        prep = r.request.copy()
        prep.headers["Authorization"] = self.__get_auth_string()
        _r = r.connection.send(prep, **kwargs)
        return _r

    def handle_401(self, r, **kwargs):
        if r.status_code == 401:
            return self._add_auth_header_and_retry(r, **kwargs)
        return r

    def handle_500(self, r, **kwargs):
        if self.token is None and r.status_code == 500:
            return self._add_auth_header_and_retry(r, **kwargs)
        return r

    def __call__(self, r):
        if self.token:
            r.headers["Authorization"] = self.__get_auth_string()
        r.register_hook("response", self.handle_401)
        # This is necessary because some of the API endpoints don't handle OAuth correctly.
        # Eg. When posting to /tag without a bearer token, instead of throwing a 401,
        # the API returns 500.
        r.register_hook("response", self.handle_500)
        return r

    def __get_auth_string(self):
        return "Bearer {0}".format(self.token)


def build_auth_scheme(**kwargs):
    """Generates an AuthScheme instance based on the given input.
    When the required set of parameters is found for an AuthScheme implementation, it will be returned.
    The order or precedence is: ApiKeyAuth, UsernamePasswordAuth, and OAuth2Auth.
    If no AuthScheme implementation is found for the given parameters, a UnknownAuthenticationScheme exception is thrown

    Implementation required parameters:
        - ApiKeyAuth:
            - apikey
        - UsernamePasswordAuth:
            - username
            - password
            - iamid
        - OAuth2Auth:
            - refresh_token
            - client_id
            - client_secret

    :param kwargs: dictionary of the named parameters
    :return: An AuthScheme implementation
    """

    if kwargs.get("apiKey"):
        return ApiKeyAuth(kwargs["apiKey"])

    if kwargs.get("username") and kwargs.get("password") and kwargs.get("iamid"):
        return UsernamePasswordAuth(**kwargs)

    if (
        kwargs.get("refresh_token")
        and kwargs.get("client_id")
        and kwargs.get("client_secret")
    ):
        return OAuth2Auth(**kwargs)

    raise UnknownAuthenticationScheme()


def retrieve_credentials_from_vault(credentials_dict: Dict[str, Any]) -> Dict[str, str]:
    """ Expects that you're logged into Hashicorp Vault. """
    import hvac

    client = hvac.Client()
    if not client.is_authenticated():
        raise ImmutaCredentialsError(
            "Vault is not authenticated. Log in to vault first"
        )
    secret = client.secrets.kv.v2.read_secret_version(path=credentials_dict["key"])
    retrieved_credentials = secret["data"]["data"]
    return retrieved_credentials


def retrieve_credentials_from_environment(
    credentials_dict: Dict[str, Any]
) -> Dict[str, str]:
    """ Will throw if env var is unset. """
    try:
        credentials_dict["value"] = os.environ[credentials_dict["key"]]
    except KeyError:
        raise ImmutaCredentialsError(f"No value for env var {credentials_dict['key']}")
    return credentials_dict


def retrieve_credentials_from_local_dict(
    credentials_dict: Dict[str, Any]
) -> Dict[str, str]:
    return credentials_dict


def retrieve_credentials(credentials_dict: Dict[str, Any]) -> Dict[str, str]:
    func = CREDENTIALS_RESOLVERS[credentials_dict["source"]]
    return func(credentials_dict)


CREDENTIALS_RESOLVERS = {
    "VAULT": retrieve_credentials_from_vault,
    "ENV": retrieve_credentials_from_environment,
    "LOCAL": retrieve_credentials_from_local_dict,
}
