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
from typing import Optional, List, Dict, Any, Union, Sequence, Iterator

import requests
import six.moves.urllib_parse as urlparse
from requests import HTTPError
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

from .authenticate import ImmutaRequestsAuth, build_auth_scheme, retrieve_credentials
from .data_source import (
    DataSource,
    DataSourceColumn,
    DataSourceDictionary,
    Handler,
    blob_handler_type,
)
from .policy import GlobalPolicy, make_policy_object_from_json
from .log import LoggingMixin

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

MAX_RETRIES = 5


class ImmutaSession(requests.Session):
    def __init__(self, immuta_url, auth_scheme=None, ca_certs=True):
        super(ImmutaSession, self).__init__()
        if immuta_url[-1] != "/":
            immuta_url = immuta_url + "/"
        self.immuta_url = immuta_url
        self.auth = ImmutaRequestsAuth(immuta_url, auth_scheme, ca_certs)
        self.verify = ca_certs
        retries = Retry(
            total=MAX_RETRIES, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )
        self.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))

    def request(self, method, url, *args, **kwargs):
        url = urlparse.urljoin(self.immuta_url, url)
        kwargs["verify"] = self.verify
        return super(ImmutaSession, self).request(method, url, *args, **kwargs)


class ImmutaClient(LoggingMixin):
    def __init__(
        self, base_url=None, auth_scheme=None, ca_certs=True, session=None, **kwargs
    ):
        if session:
            self._session = session
            self.base_url = session.immuta_url
        else:
            if not auth_scheme:
                auth_scheme = build_auth_scheme(**kwargs)
            self._session = ImmutaSession(base_url, auth_scheme, ca_certs)
            self.base_url = base_url

    def __remove_blob_handler_attributes(self, request_prefix, blob_handler):
        allowed = None
        if request_prefix == "elastic":
            allowed = [
                "bodataTableName",
                "dataSourceName",
                "masked",
                "eventTimeAttribute",
                "policyAttributes",
                "staleDataTolerance",
                "elasticProperties",
                "directoryStructure",
                "query",
            ]
        elif request_prefix in ["s3", "hdfs"]:
            allowed = ["ingestUserId", "ingestAPIKey"]
        if allowed:
            for key in blob_handler["metadata"].keys():
                if key not in allowed:
                    del blob_handler["metadata"][key]

    def _buildVisibilitySchema(self, policy_handler):
        json_policies = policy_handler["jsonPolicies"]
        fields = set()
        for policy in json_policies:
            if policy["type"] == "rowOrObjectRestriction":
                for rule in policy["rules"]:
                    for condition in rule["config"]["qualifications"]["conditions"]:
                        fields.add(condition["field"])
        return {"fields": list(fields)}

    @classmethod
    def make_glob_request_headers(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        if config["handler_type"] in ["PostgreSQL", "Redshift"]:
            return cls.make_generic_odbc_request_headers(config)
        if config["handler_type"] == "Amazon Athena":
            return cls.make_athena_glob_request_headers(config)
        raise TypeError

    @classmethod
    def make_athena_glob_request_headers(cls, config: Dict[str, Any]) -> Dict[str, str]:
        return {
            "sql-authentication-type": "accessKey",
            "sql-aws-region": config["region"],
            "sql-aws-result-location": config["queryResultLocationBucket"],
            "sql-ssl": "true",
            "sql-username": config["username"],
            "sql-password": config["password"],
        }

    @classmethod
    def make_generic_odbc_request_headers(
        cls, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "sql-hostname": config["hostname"],
            "sql-port": f"{config['port']}",
            "sql-ssl": "true",
            "sql-username": config["username"],
            "sql-password": config["password"],
        }

    @classmethod
    def make_get_request_params(
        cls,
        search_text: Optional[str] = None,
        public_only: Optional[bool] = None,
        name_only: Optional[bool] = False,
        mode: Optional[int] = 0,
        size: Optional[int] = 50,
        offset: Optional[int] = 0,
    ) -> Dict[str, Any]:
        params = {
            "searchText": search_text,
            "publicOnly": public_only,
            "nameOnly": name_only,
            "mode": mode,
            "size": size,
            "offset": offset,
        }
        clean_params = {k: v for k, v in params.items() if v is not None}
        return clean_params

    def get(self, path, *args, **kwargs):
        resp = self._session.get(path, *args, **kwargs)
        if resp.status_code != 200:
            self.log.error(
                f"Error in request. Response status: {resp.status_code}, text:"
                f" {resp.text}"
            )
        resp.raise_for_status()
        return resp.json()

    def post(self, path, data, *args, **kwargs):
        resp = self._session.post(path, json=data, *args, **kwargs)
        resp.raise_for_status()
        return resp

    def put(self, path, data, *args, **kwargs):
        resp = self._session.put(path, json=data, *args, **kwargs)
        resp.raise_for_status()
        return resp

    def delete(self, path, *args, **kwargs):
        resp = self._session.delete(path, *args, **kwargs)
        resp.raise_for_status()
        return resp

    def get_api_key(self):
        endpoint = "bim/apikey"
        res = self._session.post(endpoint)
        res.raise_for_status()
        return res.json().get("apikey", None)

    def revoke_api_key(self, keyid) -> bool:
        endpoint = "bim/apikey/{}".format(keyid)
        res = self._session.delete(endpoint)
        res.raise_for_status()
        return True

    def create_tag(
        self, tag_data: Dict[str, Any], raise_on_existing_tag: bool = False
    ) -> bool:
        res = self._session.post("tag", json=tag_data)
        if res.status_code == 400:
            if (
                "overlap with existing hierarchies" in res.json()["message"]
                and not raise_on_existing_tag
            ):
                return True
        res.raise_for_status()
        return True

    def get_table_names(self, config: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Queries the given database and returns a list of all tables with their schema names.
        NOTE: This is an undocumented API endpoint that might disappear in a future release of Immuta!
        """
        headers = self.make_glob_request_headers(config)
        return self.get(
            f"{blob_handler_type(config['handler_type'])}/database/{config['database']}/table",
            headers=headers,
        )

    def get_column_types(
        self, data_source_type: str, handler: Handler, config: Dict[str, Any]
    ) -> List[DataSourceColumn]:
        """
        Returns info on all columns that exist in the external data source specified via the handler.
        NOTE: This is an undocumented API endpoint that might disappear in a future release of Immuta!
        """
        headers = self.make_glob_request_headers(config)
        data = handler.dict(
            by_alias=True,
            include={"metadata": {"database", "table", "fh_schema", "format"}},
        )
        res = self.post(
            f"{blob_handler_type(data_source_type)}/handler/getColumnTypes",
            headers=headers,
            data=data,
        )
        res.raise_for_status()
        return [DataSourceColumn(**c) for c in res.json()]

    def get_data_source_dictionary(self, id: int) -> DataSourceDictionary:
        res = self._session.get(f"dictionary/{id}")
        res.raise_for_status()
        return DataSourceDictionary(**res.json())

    def update_data_source_dictionary(
        self, id: int, dictionary: DataSourceDictionary
    ) -> bool:
        res = self._session.put(
            f"dictionary/{id}", data=dictionary.json(skip_defaults=True)
        )
        res.raise_for_status()
        return True

    def create_data_sources(self, handler_type, handlers, data_source):
        post_body = {"handler": handlers, "dataSource": data_source}
        url = "{}/handler".format(handler_type)
        handler_response = self._session.post(url, json=post_body)
        if handler_response.status_code == 200:
            self.log.debug("Bulk data source create job submitted successfully")
            return True
        self.log.error("Error: %s", handler_response.text)
        handler_response.raise_for_status()

    def create_data_source(
        self,
        data_source: DataSource,
        handler: Union[Handler, Sequence[Handler]],
        policy_handler=None,
        handler_base_url=None,
        dictionary=None,
    ):
        request_prefix = blob_handler_type(data_source.blobHandlerType)
        is_bulk_insert = isinstance(handler, list)
        if is_bulk_insert:
            handlers = [h.dict(by_alias=True, skip_defaults=True) for h in handler]
        elif isinstance(handler, Handler):  # type check here to make mypy happy
            handlers = handler.dict(by_alias=True, skip_defaults=True)
        else:
            raise RuntimeError(
                "Invalid format for given blob handler. Expected either a list or a"
                " BlobHandler object"
            )
        post_body = {
            "handler": handlers,
            "dataSource": data_source.dict(by_alias=True, skip_defaults=True),
        }
        if policy_handler:
            post_body["policyRules"] = policy_handler["jsonPolicies"]
        self.log.debug(post_body)
        handler_response = self._session.post(
            f"{request_prefix}/handler", json=post_body
        )
        self.log.debug("Response: %s", handler_response.text)
        # There's no ID given back for bulk create requests
        if handler_response.status_code == 200:
            if not is_bulk_insert:
                return self.get_data_source(handler_response.json()["dataSourceId"])
            return None
        if "already exists" in handler_response.text:
            self.log.info("Data source with name %s already exists", data_source.name)
            return None
        self.log.error("Error: %s", handler_response.text)
        handler_response.raise_for_status()

    def update_data_source(
        self,
        data_source: DataSource,
        handler: Handler = None,
        policy_handler=None,
        handler_base_url=None,
        dictionary=None,
    ):

        request_prefix = blob_handler_type(data_source.blobHandlerType)

        if policy_handler:
            visibility_schema = self._buildVisibilitySchema(policy_handler)
            policy_handler["dataSourcePolicyHandler"] = data_source.get("policyHandler")
            if (data_source.get("policyHandler") or {}).get("handlerId"):
                handler_id = data_source["policyHandler"]["handlerId"]
                access_key = None
                handler_url = f"{handler_base_url}/policy/handler/{handler_id}"
                resp = self._session.put(handler_url, json=policy_handler)
                resp.raise_for_status()
            else:
                policy_handler = self.create_policy_handler(policy_handler)
                handler_id = policy_handler["id"]
                handler_url = (
                    f"{handler_base_url}/policy/handler/{policy_handler['id']}"
                )
                access_key = policy_handler["accessKey"]

            data_source["policyHandler"] = {
                "url": handler_url,
                "handlerId": handler_id,
                "visibilitySchema": visibility_schema,
            }
            if access_key:
                data_source["policyHandler"]["accessKey"] = access_key

        # update the data source
        resp = self._session.put(f"dataSource/{data_source['id']}", json=data_source)
        resp.raise_for_status()
        saved_data_source = resp.json()

        # update the dictionary if it was passed in
        if dictionary:
            resp = self._session.put(f"dictionary/{data_source['id']}", json=dictionary)
            resp.raise_for_status()

        # update the blob handler if it was passed in
        if handler:
            blob_handler_id = data_source["blobHandler"]["url"].split("/")[-1]
            self.__remove_blob_handler_attributes(
                blob_handler_type(data_source), handler
            )
            handler_url = (
                f"{handler_base_url}/{request_prefix}/handler/{blob_handler_id}"
            )
            resp = self._session.put(handler_url, json=handler)
            resp.raise_for_status()

        return saved_data_source

    def get_data_source(self, id=None, update_stats=False, name=None):
        if not id:
            if not name:
                raise Exception("Either id or name must be provided but were not.")
            return self.get_data_source_by_name(name)

        endpoint = f"dataSource/{id}"
        return self.get(endpoint, params={"updateStats": str(update_stats).lower()})

    def get_data_source_list(
        self,
        search_text=None,
        public_only=None,
        name_only=False,
        mode=0,
        size=50,
        offset=0,
    ):
        params = self.make_get_request_params(
            search_text=search_text,
            public_only=public_only,
            name_only=name_only,
            mode=mode,
            size=size,
            offset=offset,
        )
        return self.get("dataSource", params=params)

    def get_data_source_by_name(self, name):
        endpoint = "dataSource/name/{}".format(name)
        return self.get(endpoint)

    def get_global_policies(
        self,
        search_text: Optional[str] = None,
        public_only: Optional[bool] = None,
        mode: Optional[int] = None,
        name_only: Optional[bool] = False,
    ) -> Iterator[GlobalPolicy]:
        params = self.make_get_request_params(
            search_text=search_text,
            public_only=public_only,
            name_only=name_only,
            mode=mode,
            size=None,
            offset=None,
        )
        res = self.get("policy/global", params=params)
        for policy in res:
            yield (make_policy_object_from_json(policy))

    def create_policy_handler(self, handler: Dict[str, str]) -> Dict[str, str]:
        handler_response = self._session.post("policy/handler", json=handler)
        handler_response.raise_for_status()
        return handler_response.json()

    def create_global_policy(self, policy: GlobalPolicy) -> Union[int, bool]:
        res = self._session.post("policy/global", data=policy.json(by_alias=True))
        if res.status_code == 200:
            return res.json()["id"]
        if res.status_code == 422 and res.json()["validation"][0]["code"] == "unique":
            self.log.warn(f"Policy with name {policy.name} already exists.")
        res.raise_for_status()
        return False

    def update_global_policy(
        self, policy: GlobalPolicy, id: Optional[int]
    ) -> Dict[str, Any]:
        res = self._session.put(f"policy/global/{id}", data=policy.json(by_alias=True))
        res.raise_for_status()
        return res.json()

    def delete_global_policy(self, id: Optional[int]) -> bool:
        res = self.delete(f"policy/global/{id}", params={"policyId": id})
        res.raise_for_status()
        return True

    def disable_data_source(
        self, id: Optional[int] = None, name: Optional[str] = None
    ) -> int:
        """
        Disable a data source with the id or the name provided.
        If both id and name are provided, the name will be ignored
        :param id: data source Id
        :param name: data source name
        :return: the data source id
        """
        data_source = self.get_data_source(id=id, name=name)
        if data_source["deleted"]:
            self.log.warning(
                f"Data source \"{data_source['name']}\" id: {data_source['id']} is"
                " disabled already"
            )
        else:
            self.delete(f"dataSource/{data_source['id']}")
        return data_source["id"]

    def restore_data_source(
        self, id: Optional[int] = None, name: Optional[str] = None
    ) -> int:
        """
        Restore a data source that was previously disabled with the id or the name provided.
        If both id and name are provided, the name will be ignored
        :param id: data source Id
        :param name: data source name
        :return: the data source id
        """
        data_source = self.get_data_source(id=id, name=name)
        if not data_source["deleted"]:
            self.log.warning(
                f"Data source \"{data_source['name']}\" id: {data_source['id']} is"
                " enabled already"
            )
        else:
            self.put(f"dataSource/{data_source['id']}", data={"deleted": False})
        return data_source["id"]

    def delete_data_source(
        self, id: Optional[int] = None, name: Optional[str] = None
    ) -> int:
        """
        Deletes completely a data source with the id or the name provided.
        If both id and name are provided, the name will be ignored
        :param id: data source Id
        :param name: data source name
        :return: the data source id
        """
        if not id and not name:
            raise Exception("Either id or name must be provided but were not.")
        try:
            if not id and name:
                id = self.get_data_source_by_name(name=name)["id"]

            # To completely remove a data source, we need to disabled it first,
            # but Immuta uses the same endpoint for both actions,
            # Due to this reason, we run `self.delete()` twice
            # unless the data source was already disabled
            result = self.delete(f"dataSource/{id}")

            if not result.json()["hardDelete"]:
                # if the data source was only disabled, delete it again
                self.delete(f"dataSource/{id}")
        except HTTPError as e:
            if e.response.status_code == 404:
                self.log.warning(
                    f"Data source {id or name} not found, it might have been deleted"
                    " already"
                )
            else:
                raise

        assert isinstance(id, int)
        return id

    def tag_data_source(self, id: int, tag_data: List[Dict[str, Any]]) -> bool:
        """
        Adds tags to the data source directly, not to columns within the data source.
        :param id: data source id
        :param tag_data: list of tag dicts to apply to the data source
        :return: True
        """
        res = self._session.post(f"tag/datasource/{id}", json=tag_data)
        res.raise_for_status()
        return True


def get_client(base_url: str, auth_config: Dict[str, Any], **kwargs) -> ImmutaClient:
    return ImmutaClient(base_url=f"https://{base_url}", **auth_config)
