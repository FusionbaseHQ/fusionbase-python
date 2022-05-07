import json
from typing import Union

import requests
from rich.console import Console
from rich.table import Table

from fusionbase.exceptions.ResponseEvaluator import ResponseEvaluator


class DataService:

    def __init__(self, auth: dict, connection: dict, log: bool = False) -> None:
        """
        Used to initialise a new DataService Object
        :param auth: the standard authentication object to authenticate yourself towards the fusionbase API
        Example:
        auth = {"api_key": " ***** Hidden credentials *****"}

        :param connection: the standard authentication object used to verify e.g which uri should be used
        Example:
        connection={"base_uri": "https://api.fusionbase.com/api/v1"}

        :param log: Whether the the output of any given operation should be logged to console
        """

        self.auth = auth
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.requests = requests.Session()
        self.requests.headers.update({'x-api-key': self.auth["api_key"]})
        self.log = log
        self.console = Console()
        self.evaluator = ResponseEvaluator()

    def _log(self, message, force=False):
        if not self.log and not force:
            return None
        else:
            self.console.log(message)

    def get_meta_data(self, key: Union[str, int]) -> dict:
        """
        Retrieves the metadata from a Service by giving a Service specific key
        :param key: The key of the service either as a string or integer value
        :return: The metadata for the given service as a python dictionary
        """

        r = self.requests.get(
            f"{self.base_uri}/data-service/get/{key}")

        self.evaluator.evaluate(r)
        meta = r.json()
        return meta

    def pretty_meta_data(self, key: Union[str, int]) -> None:
        """
        Retrieves the metadata from a Service by giving a Service specific key and prints it nicely to console
        :param key: The key of the service either as a string or integer value
        """
        meta_data = self.get_meta_data(key=key)
        table = Table(title=meta_data["name"]["en"],
                      caption=meta_data["description"]["en"])

        table.add_column("Property", justify="right",
                         style="magenta", no_wrap=True)
        table.add_column("Value", style="cyan")

        def __add_row(table, label, data, key):
            if key in data:
                table.add_row(label, str(data[key]))
            return table

        table = __add_row(table, "Key", meta_data, "_key")
        table = __add_row(table, "Unique Label", meta_data, "unique_label")
        table = __add_row(table, "Source", meta_data["source"], "label")
        table = __add_row(table, "Source Key", meta_data["source"], "_key")

        print("\n" * 2)
        self._log(table, True)
        print("\n" * 2)

    def request_definition(self, key: Union[str, int]) -> dict:
        """
        Retrieves the request definition (such as required parameters) from a Service by giving a Service specific key and prints it nicely to console
        :param key: The key of the service either as a string or integer value
        :return: The request definition for the given service as a python dictionary
        """
        meta_data = self.get_meta_data(key=key)
        return meta_data['request_definition']

    def pretty_request_definition(self, key: Union[str, int]) -> None:
        """
       Retrieves the request definition (such as required parameters) from a Service by giving a Service specific key and prints it nicely to console
       :param key: The key of the service either as a string or integer value
       """
        request_definition = self.request_definition(key=key)
        parameters = request_definition['parameters']

        print("\n")
        for _, p in enumerate(parameters):
            table = Table(title=p['name'])

            table.add_column("Property", justify="right",
                             style="magenta", no_wrap=True)
            table.add_column("Value", style="cyan")

            def __add_row(table, label, data, key):
                if key in data:
                    table.add_row(label, str(data[key]))
                return table

            table = __add_row(table, "Description", p['description'], 'en')
            table = __add_row(table, "Definition", p['definition'], 'en')
            table = __add_row(table, "Required", p, 'required')

            if p.get('schema') is not None:
                table = __add_row(table, "Type", p['schema'], 'type')

            table = __add_row(table, "Sample", p['sample'], 'value')
            self._log(table, True)
            print("\n")

        print("\n" * 2)

    def __validate_parameters(self, key: Union[str, int], given_parameters: Union[dict, list]):
        expected_parameters = self.request_definition(key=key)['parameters']

        assert len(given_parameters) <= len(
            expected_parameters), "MORE PARAMETERS GIVEN THAN EXPECTED"

        expected_names = [p['name'] for p in expected_parameters]

        for _, p in enumerate(given_parameters):
            keys = p.keys()

            assert 'name' in keys, "INPUT PARAMETERS SHOULD FOLLOW DICT SCHEMA {'name': '', 'value': ''}"
            assert 'value' in keys, "INPUT PARAMETERS SHOULD FOLLOW DICT SCHEMA {'name': '', 'value': ''}"

        for _, p in enumerate(given_parameters):
            assert p['name'] in expected_names, f"THE GIVEN PARAMETER NAMED {p['name']} WAS NOT AN EXPECTED " \
                                                f"PARAMETER NAME "

    def invoke(self, key: Union[str, int], parameters: Union[dict, list]) -> dict:
        """
        Invokes a given Dataservice defined by its key and returns the requested result
        :param key: The key of the service either as a string or integer value
        :param parameters: The parameters to invoke the Dataservice with provided as either a dict if its one parameter
        or a list of dictionaries if you want to provide more than one input
        :return: The output for the given service invocation as a python dictionary
        """
        if isinstance(parameters, dict):
            parameters = [parameters]

        elif not isinstance(parameters, list):
            raise Exception('PARAMETERS_MUST_BE_EITHER_LIST_OR_DICT')

        self.__validate_parameters(key=key, given_parameters=parameters)

        r = self.requests.post(
            url=f"{self.base_uri}/data-service/invoke",
            headers={
                "Content-Type": "application/json; charset=utf-8",
            },
            data=json.dumps({
                "inputs": parameters,
                "data_service_key": key
            })
        )

        self.evaluator.evaluate(r)
        return r.json()
