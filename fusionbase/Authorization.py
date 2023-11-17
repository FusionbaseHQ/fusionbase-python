from __future__ import annotations
from typing import List
import requests


class Authorization:
    def __init__(self, auth: dict, connection: dict, config=None) -> None:

        if config is None:
            config = {}

        self.auth = auth
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.requests = requests.Session()
        self.requests.headers.update({"x-api-key": self.auth["api_key"]})


    def create_role_definition(self, name) -> dict:
        r = self.requests.post(f"{self.base_uri}/authorization/role-definition/create", json={'name':name})
        return r.json()

    def create_role_assignment(self, linked_identity: str, name: str, resources: List[dict]) -> dict:
        json = {
            "linked_identity": linked_identity,
            "name": name,
            "resources": resources
        }

        r = self.requests.post(f"{self.base_uri}/authorization/role-assignment/create", json=json)
        return r.json()
    
    def role_assignment_add_resource(self, role_assignement_key: str, resource: dict) -> dict:
        r = self.requests.post(f"{self.base_uri}/authorization/role-assignment/{role_assignement_key}/add-resource", json=resource)
        return r.json()
    
    def role_assignment_remove_resource(self, role_assignement_key: str, resource: dict) -> dict:
        r = self.requests.post(f"{self.base_uri}/authorization/role-assignment/{role_assignement_key}/remove-resource", json=resource)
        return r.json()
    
    def create_action_definition(self, name) -> dict:
        r = self.requests.post(f"{self.base_uri}/authorization/action-definition/create", json={'name':name})
        return r.json()
    
    def create_action_assignment(self, linked_identity: str, resource: dict, allowed_actions: List[str] | None = None, denied_actions: List[str] | None = None) -> dict:
        json = dict(
            linked_identity=linked_identity,
            resource=resource,
            allowed_actions=allowed_actions,
            denied_actions=denied_actions
        )
        r = self.requests.post(f"{self.base_uri}/authorization/action-assignment/create", json=json)
        return r.json()
    
    def allow_action(self, action_assignment_key: str, actions: str | List[str]) -> dict:
        json = {'actions': actions}
        r = self.requests.post(f"{self.base_uri}/authorization/action-assignment/{action_assignment_key}/allow", json=json)
        return r.json()
    
    def deny_action(self, action_assignment_key: str, actions: str | List[str]) -> dict:
        json = {'actions': actions}
        r = self.requests.post(f"{self.base_uri}/authorization/action-assignment/{action_assignment_key}/deny", json=json)
        return r.json()