import requests


class ACL:
    def __init__(self, auth={}, connection={}, config={}) -> None:
        self.auth = auth
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.requests = requests.Session()
        self.requests.headers.update({"x-api-key": self.auth["api_key"]})

    def add_acl_user_item(self, acl_definition):

        if not isinstance(acl_definition, dict) or not all(
            k in acl_definition
            for k in ("user_key", "resource", "action", "context", "resource_type")
        ):
            raise Exception("ACL_DEFINITION_HAS_INCORRECT_FORMAT")

        print(f"{self.base_uri}/acl/add/user/item")
        response = self.requests.post(
            f"{self.base_uri}/acl/add/user/item", json=acl_definition
        )
        return response.content
