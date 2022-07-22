from __future__ import annotations
import requests


class DataStreamCollection:
    def __init__(self, auth: dict, connection: dict, config=None) -> None:

        if config is None:
            config = {}

        self.auth = auth
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.requests = requests.Session()
        self.requests.headers.update({"x-api-key": self.auth["api_key"]})

    def list_all_collections(self):
        r = self.requests.get(f"{self.base_uri}/data-stream-collections/list/all")
        collections = r.json()
        return collections

    def create(self, name, description=""):
        name = name.strip()
        description = description.strip()
        assert len(name) > 3, "NAME_REQUIRED"

        # Make sure name does not exist already
        # TODO: API ensures that already. Implement get DSC by name endpoint.
        dscs = self.list_all_collections()

        assert name.lower() not in [
            col["name"].lower() for col in dscs
        ], "COLLECTION_NAME_ALREADY_EXISTS"

        data = {"collection_name": name, "description": description}
        r = self.requests.post(
            f"{self.base_uri}/data-stream-collection/create", data=data
        )
        result = r.json()
        return result

    def add_data_streams(self, collection_key, data_stream_keys=[]):
        assert len(data_stream_keys) > 0, "NO_DATA_STREAM_KEYS_PROVIDED"
        data = {"collection_key": collection_key, "data_stream_keys": data_stream_keys}
        r = self.requests.post(
            f"{self.base_uri}/data-stream-collection/add/data-streams", data=data
        )
