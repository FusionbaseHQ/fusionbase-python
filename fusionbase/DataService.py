from __future__ import annotations

import json
import os
import platform
import shelve
import tempfile
from datetime import datetime
from pathlib import Path, PurePath
from typing import Union
import types

import requests
from rich.console import Console
from rich.table import Table

try:
    import pandas as pd
except ImportError as e:
    pd = None

from fusionbase.exceptions.ResponseEvaluator import ResponseEvaluator


def cache():
    """
    A Helper function to cache service data to specified cache
    """

    def decorator(func):
        def new_func(*args, **kwargs):
            # get self argument from wrapped function to access the temporary directory
            self = args[0]

            if self.cache == None:
                self.cache = 0

            if not isinstance(self.cache, int):
                raise TypeError(
                    f"Parameter cache must be of type int but was {type(self.cache)}"
                )

            if self.cache < 0:
                raise ValueError(
                    f"Parameter cache must a positive integer or 0 if you want caching disabled, but was: {self.cache}!"
                )

            if self.cache > 0:
                cache_filepath = os.path.join(self.tmp_dir, f"cache_{self.key}.shelve")

                # generate a key to store the data to cache
                args_cleaned = args[1:]
                key = args_cleaned + tuple(sorted(kwargs.items()))
                key = f"{str(key)}_{self.key}"

                with shelve.open(cache_filepath) as db:
                    # check if cached value exists for current input
                    if key not in db:
                        # data not yet cached or cache to old
                        db[key] = {
                            "time_of_caching": datetime.now(),
                            "value": func(*args, **kwargs),
                        }
                    else:
                        # value exists
                        # calculate age of cached result
                        time_of_caching = db[key]["time_of_caching"]
                        now = datetime.now()
                        difference = now - time_of_caching
                        difference_minutes = difference.total_seconds() / 60

                        # check if cached result is older than the provided threshold
                        # cache new value with new timestamp
                        if difference_minutes > self.cache:
                            db[key] = {
                                "time_of_caching": datetime.now(),
                                "value": func(*args, **kwargs),
                            }

                    # otherwise it is cached -> return either way (return just the function value)
                    return db[key].get("value")
            else:
                # cache was not specified so not caching at all
                return func(*args, **kwargs)

        return new_func

    return decorator


class DataService:
    def __init__(
        self,
        key: Union[str, int],
        auth: dict,
        connection=None,
        log: bool = False,
        config: dict = None,
        cache: int = 0,
    ) -> None:
        """
        Used to initialise a new DataService Object
        :param key: The key of the service either as a string or integer value
        :param auth: the standard authentication object to authenticate yourself towards the fusionbase API
        Example:
        auth = {"api_key": " ***** Hidden credentials *****"}

        :param connection: the standard authentication object used to verify e.g which uri should be used
        Example:
        connection={"base_uri": "https://api.fusionbase.com/api/v1"}

        :param log: Whether the the output of any given operation should be logged to console

        :param cache (int): the time in minutes data should be cached (0 [default] if caching should be disabled)
                      e.g. cache = 5 * 24 * 60 -> caching for 5 days
        """

        if config is None:
            config = {}

        if connection is None:
            connection = {"base_uri": "https://api.fusionbase.com/api/v1"}
        if not isinstance(key, int) and not isinstance(key, str):
            raise TypeError(
                f"Key must be either of type int or str but was {type(key)}"
            )
        else:
            self.__key = key

        self.auth = auth
        self.cache = cache
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.requests = requests.Session()
        self.requests.headers.update({"x-api-key": self.auth["api_key"]})
        self.log = log
        self.console = Console()
        self.evaluator = ResponseEvaluator()
        self.get_meta_data()

        for k, v in self.get_meta_data().items():
            setattr(self, k, v)

        if "cache_dir" in config:
            self.tmp_dir = PurePath(Path(config["cache_dir"]))
        else:
            self.tmp_dir = PurePath(
                Path(
                    "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()
                ),
                "fusionbase",
            )
        # Ensure that tmp/cache directory exists
        Path(self.tmp_dir).mkdir(parents=True, exist_ok=True)

    @property
    def key(self):
        return self.__key

    def __str__(self) -> str:
        meta_data = self.get_meta_data()
        return f"""{meta_data["name"]["en"]}
        =============================
        Key             -> {meta_data["_key"]}
        Source          -> {meta_data["source"]["label"]}
        Source Key      -> {meta_data["source"]["_key"]}
        --------------------
        Description: 
        {meta_data["description"]["en"]}
        """

    def _log(self, message: str, force=False, rule=False) -> None:
        if not self.log and not force:
            return None
        else:
            if not rule:
                self.console.print(message)
            if rule:
                self.console.rule(message)

    def get_meta_data(self) -> dict:
        """
        Retrieves the metadata from a Service by giving a Service specific key
        :return: The metadata for the given service as a python dictionary
        """

        r = self.requests.get(f"{self.base_uri}/data-service/get/{self.key}")

        self.evaluator.evaluate(r)
        meta = r.json()
        return meta

    def pretty_meta_data(self) -> None:
        """
        Retrieves the metadata from a Service by giving a Service specific key and prints it nicely to console
        """
        meta_data = self.get_meta_data()
        table = Table(
            title=meta_data["name"]["en"], caption=meta_data["description"]["en"]
        )

        table.add_column("Property", justify="right", style="magenta", no_wrap=True)
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

    def get_request_definition(self) -> dict:
        """
        Retrieves the request definition (such as required parameters) from a Service by giving a Service specific
        key and prints it nicely to console :return: The request definition for the given service as a python
        dictionary
        """
        meta_data = self.get_meta_data()
        return meta_data["request_definition"]

    def pretty_request_definition(self) -> None:
        """
        Retrieves the request definition (such as required parameters) from a Service by giving a Service specific
        key and prints it nicely to console
        """
        request_definition = self.get_request_definition()
        parameters = request_definition["parameters"]

        print("\n")
        for _, p in enumerate(parameters):
            table = Table(title=p["name"])

            table.add_column("Property", justify="right", style="magenta", no_wrap=True)
            table.add_column("Value", style="cyan")

            def __add_row(table, label, data, key):
                if key in data:
                    table.add_row(label, str(data[key]))
                return table

            table = __add_row(table, "Description", p["description"], "en")
            table = __add_row(table, "Definition", p["definition"], "en")
            table = __add_row(table, "Required", p, "required")

            if p.get("schema") is not None:
                table = __add_row(table, "Type", p["schema"], "type")

            table = __add_row(table, "Sample", p["sample"], "value")
            self._log(table, True)
            print("\n")

        print("\n" * 2)

    def __validate_parameters(self, given_parameters: Union[dict, list]):
        expected_parameters = self.get_request_definition()["parameters"]

        assert len(given_parameters) <= len(
            expected_parameters
        ), "MORE PARAMETERS GIVEN THAN EXPECTED"

        expected_names = [p["name"] for p in expected_parameters]

        for _, p in enumerate(given_parameters):
            keys = p.keys()

            assert (
                "name" in keys
            ), "INPUT PARAMETERS SHOULD FOLLOW DICT SCHEMA {'name': '', 'value': ''}"
            assert (
                "value" in keys
            ), "INPUT PARAMETERS SHOULD FOLLOW DICT SCHEMA {'name': '', 'value': ''}"

        for _, p in enumerate(given_parameters):
            assert p["name"] in expected_names, (
                f"THE GIVEN PARAMETER NAMED {p['name']} WAS NOT AN EXPECTED "
                f"PARAMETER NAME "
            )

    @cache()
    def invoke(self, parameters: Union[dict, list] = None, **kwargs) -> list:
        """
        Invokes a given Dataservice defined by its key and returns the requested result

        :param parameters: The parameters to invoke the Dataservice with provided as either a dict if its one parameter
        or a list of dictionaries if you want to provide more than one input

        **kwargs: You can also provide the parameters as keyword arguments, e.g. if the service requires an
        address_string just call the function like this: service.invoke(address_string='Your Address String')

        :return: The output for the given service invocation as a python dictionary
        """
        if len(kwargs.items()) == 0 and parameters is not None:
            if isinstance(parameters, dict):
                parameters = [parameters]

        elif parameters is None and len(kwargs.items()) != 0:
            parameters = [
                {"name": key, "value": str(value)} for key, value in kwargs.items()
            ]
        else:
            raise Exception(
                "Either parameters or keyword arguments have to be provided"
            )

        self.__validate_parameters(given_parameters=parameters)

        r = self.requests.post(
            url=f"{self.base_uri}/data-service/invoke",
            headers={"Content-Type": "application/json; charset=utf-8",},
            data=json.dumps({"inputs": parameters, "data_service_key": self.key}),
        )

        self.evaluator.evaluate(r)
        return r.json()

    def apply(self, df, input_mappings: list = [], callback: types.FunctionType = None):
        """
        Invokes a given Dataservice on a DataFrame to enrich it

        :param df: The pandas DataFrame to which you want to apply the fusionbase service.
        :param input_mappings: List of tuples that contain the mapping from DataFrame row to service input parameter.
        :param callback: A callback function to manipulate the fusionbase service result and the series row of the DataFrame.

        :return: The enriched pandas DataFrame
        """
        # Make sure pandas is installed
        if pd is None:
            raise ModuleNotFoundError("You must install pandas to use this feature.")

        def _generate_parameters(row, _input_mappings):
            parameter_mappings = []
            for _i_map in _input_mappings:
                parameter_mappings.append({"name": _i_map[0], "value": row[_i_map[1]]})
            return parameter_mappings

        def _series_invoke(series):
            if isinstance(callback, types.FunctionType):
                return callback(
                    series,
                    self.invoke(
                        parameters=_generate_parameters(series, input_mappings)
                    ),
                )
            else:
                series["fusionbase_result"] = self.invoke(
                    parameters=_generate_parameters(series, input_mappings)
                )
                return series

        df = df.apply(_series_invoke, axis=1)

        return df

    def clear_cache(self):
        """Used to clear the cache files for the current service
        """
        cache_file = os.path.join(self.tmp_dir, f"cache_{self.key}.shelve.db")
        if os.path.exists(cache_file):
            os.remove(cache_file)
        else:
            self._log("NO CACHE FILE FOUND FOR THIS SERVICE!")
