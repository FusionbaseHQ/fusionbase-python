from __future__ import annotations

import pickle
import csv

# import concurrent.futures
import gzip
import hashlib
import math
import os
import platform
import re
from sys import getsizeof, stderr
from itertools import chain
from collections import deque

import tempfile
import urllib.parse
from itertools import islice
from pathlib import Path, PurePath
from typing import IO, Union
import warnings

import requests
from requests_toolbelt import MultipartEncoder

# from rich.console import Console
from rich import print
from rich.prompt import Prompt
from rich.table import Table
from rich.rule import Rule
from rich.progress import (
    Progress,
    SpinnerColumn,
    TimeRemainingColumn,
    DownloadColumn,
    MofNCompleteColumn,
)

from pathos.pools import ProcessPool as PPool
import asyncio
import aiohttp
import aiofiles

try:
    import pandas as pd
except ImportError as e:
    pd = None

# Use orjson if available, otherwise fallback to built-in
try:
    import orjson as json
except ImportError as e:
    import json

from fusionbase.utils.DataChunker import DataChunker
from fusionbase.exceptions.DataStreamNotExistsError import DataStreamNotExistsError
from fusionbase.exceptions.ResponseEvaluator import ResponseEvaluator
from fusionbase.constants.ResultType import ResultType


class DataStream:
    def __init__(
        self,
        auth: dict,
        key: Union[str, int] = None,
        label: str = None,
        connection: dict = {"base_uri": "https://api.fusionbase.com/api/v1"},
        config: dict = None,
        log: bool = False,
    ) -> None:
        """
        Used to initialise a new DataStream Object
        :param key: The key of the datastream provided as an integer or string
        :param auth: the standard authentication object to authenticate yourself towards the fusionbase, API
        Example:
        auth = {"api_key": " ***** Hidden credentials *****"}

        :param connection: the standard authentication object used to verify e.g which uri should be used
        Example:
        connection={"base_uri": "https://api.fusionbase.com/api/v1"}

        :param config: Let the user specify e.g a specific caching directory
        :param log: Whether the the output of any given operation should be logged to console
        """
        if config is None:
            config = {}

        if key is None and label is None:
            raise TypeError(f"Either key or label has to be provided")

        elif not isinstance(key, int) and label is None and not isinstance(key, str):
            raise TypeError(
                f"Key must be either of type int or str but was {type(key)}"
            )
        else:
            self.__key = key
            self.__label = label

        self.__log = log
        self.auth = auth
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.evaluator = ResponseEvaluator()
        self.requests = requests.Session()
        self.requests.headers.update({"x-api-key": self.auth["api_key"]})

        # Instantiate data chunker
        self.data_chunker = DataChunker(config=config)

        if self.__label is not None and self.__key is None:
            self.__key = self._get_meta_data_by_label()["_key"]
        elif self.__key is not None:
            # CHECK IF STREAM EXISTS
            _meta_data = self.get_meta_data()
            # Not all users have access to label
            try:
                self.__label = _meta_data.get("unique_label")
            except Exception as e:
                print(e)
                pass

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

    def __str__(self) -> str:
        new_line_indent = "\n    "
        meta_data = self.get_meta_data()
        return f"""{meta_data["name"]["en"]}
    =============================
    Key             -> {meta_data["_key"]}{"{}Unique Label    -> {}".format(new_line_indent, meta_data['unique_label']) if meta_data.get('unique_label') is not None  else ""}    
    Entries         -> {meta_data["meta"]["entry_count"]}
    Props           -> {meta_data["meta"]["main_property_count"]}
    Data Version    -> {meta_data["data_version"]}
    Data Update     -> {meta_data["data_updated_at"]}
    Store Version   -> {meta_data["store_version"]}
    Source          -> {meta_data["source"]["label"]}
    Source Key      -> {meta_data["source"]["_key"]}
    """

    @property
    def key(self):
        return self.__key

    @property
    def label(self):
        return self.__label

    @property
    def log(self):
        return self.__log

    @log.setter
    def log(self, log: bool):
        self.__log = log

    def _log(self, message: str, force=False, rule=False) -> None:
        if not self.log and not force:
            return None
        else:
            if not rule:
                print(message)
            if rule:
                print(Rule(title=message))

    @staticmethod
    def _is_gzipped_file(file_obj) -> bool:
        with gzip.open(file_obj, "r") as fh:
            try:
                fh.read(1)
                fh.close()
                return True
            except gzip.BadGzipFile:
                return False

    @staticmethod
    def _calc_skip_limit_tuples(
        limit: int,
        max_batches: int = 20,
        initial_skip: int = 0,
        average_row_size_bytes: int = -1,
        stream_rows: int = 0,
    ):
        def chunks(it, size):
            it = iter(it)
            return iter(lambda: tuple(islice(it, size)), ())

        # Calculate hard batch size limits based on internal memory and the maximum that the fusionbase api accepts
        # f(x) = -14.5 * m + 2.000.000 with a maximum of 10.000.000
        ONE_AND_A_HALF_GB = 1610612736
        if average_row_size_bytes >= 0:
            # The factor 1.2 is a safety margin, since the fusionbase api also implies a hard limit. The hardlimit here is currently just estimated based on the first 150 rows
            # TODO Retrieve actual hard limit from fusionbase api
            hard_limit = math.floor(ONE_AND_A_HALF_GB / (average_row_size_bytes * 1.2))
        else:
            hard_limit = 5_000

        if limit / hard_limit < max_batches:
            hard_limit = math.ceil(limit / max_batches)
            if hard_limit == 1:
                hard_limit = 2

        # Chunks are bigger than 5k entries, i.e. they exceed the hard limit
        # Make chunks into smaller batches of 4357 each
        # if skip_limits[0][1] <= hard_limit or True:
        # 4357 is just a random prime number, could by any number < 5k
        skip_limit_chunks = list(chunks(range(0, limit + 0), hard_limit - 1))
        skip_limits = [
            (chunk[0] + initial_skip, chunk[-1] + 1 - chunk[0])
            for i, chunk in enumerate(skip_limit_chunks)
        ]

        # Remove tuples that contain higher starting numbers the the actual data stream size
        # It is indended to filter it twice
        skip_limits = list(filter(lambda x: x[0] <= stream_rows, skip_limits))
        return skip_limits

    def __get_memory_size(self, o, handlers={}, verbose=False):
        """ Returns the approximate memory footprint an object and all of its contents.
        See: https://code.activestate.com/recipes/577504/

        Automatically finds the contents of the following builtin containers and
        their subclasses:  tuple, list, deque, dict, set and frozenset.
        To search other containers, add handlers to iterate over their contents:

            handlers = {SomeContainerClass: iter,
                        OtherContainerClass: OtherContainerClass.get_elements}

        """
        dict_handler = lambda d: chain.from_iterable(d.items())
        all_handlers = {
            tuple: iter,
            list: iter,
            deque: iter,
            dict: dict_handler,
            set: iter,
            frozenset: iter,
        }
        all_handlers.update(handlers)  # user handlers take precedence
        seen = set()  # track which object id's have already been seen
        default_size = getsizeof(0)  # estimate sizeof object without __sizeof__

        def sizeof(o):
            if id(o) in seen:  # do not double count the same object
                return 0
            seen.add(id(o))
            s = getsizeof(o, default_size)

            if verbose:
                print(s, type(o), repr(o), file=stderr)

            for typ, handler in all_handlers.items():
                if isinstance(o, typ):
                    s += sum(map(sizeof, handler(o)))
                    break
            return s

        return sizeof(o)

    def _estimate_average_row_size(self):
        """
        Returns the average row size in bytes based on the first 180 rows

        Returns:
            float: Average row size in bytes based on the first 180 rows
        """
        try:
            request_url = (
                f"{self.base_uri}/data-stream/get/{self.key}?skip={0}&limit={180}"
            )
            r = requests.get(request_url, headers={"x-api-key": self.auth["api_key"]})
            size = math.ceil(
                self.__get_memory_size(r.json().get("data")) / len(r.json().get("data"))
            )
            return size
        except Exception as e:
            print(e)
            return 0

    async def persistent_session(self):
        self.aiohttp_session = session = aiohttp.ClientSession(
            headers={"x-api-key": self.auth["api_key"]}
        )
        yield
        await session.close()

    def pretty_meta_data(self) -> None:
        """
        Retrieves the metadata from a Stream by giving a Service specific key and prints it nicely to console
        """
        meta_data = self.get_meta_data()
        table = Table(title=meta_data["name"]["en"])

        table.add_column("Property", justify="right", style="magenta", no_wrap=True)
        table.add_column("Value", style="cyan")

        def __add_row(table, label, data, key):
            if key in data:
                table.add_row(label, str(data[key]))
            return table

        table = __add_row(table, "Key", meta_data, "_key")
        table = __add_row(table, "Unique Label", meta_data, "unique_label")
        table = __add_row(table, "# Entries", meta_data["meta"], "entry_count")
        table = __add_row(table, "# Props", meta_data["meta"], "main_property_count")
        table = __add_row(table, "Data Version", meta_data, "data_version")
        table = __add_row(table, "Data Update", meta_data, "data_updated_at")
        table = __add_row(table, "Store Version", meta_data, "store_version")
        table = __add_row(table, "Source", meta_data["source"], "label")
        table = __add_row(table, "Source Key", meta_data["source"], "_key")

        print("\n" * 2)
        self._log(table, True)
        print("\n" * 2)

    def get_meta_data(self) -> Union[None, dict]:
        """
        Retrieves the metadata from a Stream by giving a Stream specific key
        :return: The metadata for the given service as a python dictionary
        """

        r = self.requests.get(f"{self.base_uri}/data-stream/get/{self.key}/meta")

        self.evaluator.evaluate(r)
        meta = r.json()
        return meta

    def _get_meta_data_by_label(self) -> dict:
        # 1. Get ID by the label
        r = self.requests.get(f"{self.base_uri}/data-stream/get/label/{self.label}")
        self.evaluator.evaluate(response=r)
        data = r.json()
        # 2. Get the data stream
        if data is None or r.status_code != 200 or "_key" not in data:
            self._log(
                f"Datastream with unique label [bold]{self.label}[/bold] does not exist"
            )
            return None

        self.__key = data["_key"]
        data_stream_meta_data = self.get_meta_data()
        return data_stream_meta_data

    def _get_schema_by_label(self) -> dict:
        """
        Returns the schema of a datastream by providing the unique label
        :param label: The unique label of the datastream
        :return: The schema of the datastream as a dict
        """
        meta_data = self._get_meta_data_by_label()
        dics = meta_data["data_item_collections"]
        for i, dic in enumerate(dics):
            # Add to 'key' for making it update compatible
            k = dic["_key"]
            dic["_key"]
            dic = {**{"key": k}, **dic}
            dics[i] = dic
        return dics

    def update_metadata(self, meta_data: dict) -> int:
        """
        Used to update the metadata of a datastream
        :param meta_data: The metadata dict containing the
        :return: The HTTP Statuscode of the API Call
        """
        assert "data_stream_key" in meta_data, "NO_DATA_STREAM_KEY_SET"
        print("NOTICE: MAKE SURE TO USE 'key' INSTEAD OF '_key'")

        # TODO VERIFY METADATA STRUCTURE
        allowed_keys = ["attributes"]

        r = self.requests.patch(
            f"{self.base_uri}/data-stream/meta/update", json=meta_data
        )
        self.evaluator.evaluate(response=r)

        self._log(r.text)
        return r.status_code

    def set_source(self, source_key: Union[int, str], stream_specific: dict) -> bool:
        """
        Used to set the source of a provided Datastream
        :param stream_specific:
        :param data_stream_key: The key of the Datastream
        :param source_key: The key of the Datasource
        :return:
        """
        assert "uri" in stream_specific, "STREAM_SPECIFIC_URI_IS_REQUIRED"
        update_dict = {
            "data_stream_key": self.key,
            "source_key": source_key,
            "stream_specific_uri": stream_specific["uri"],
        }
        r = self.requests.patch(
            f"{self.base_uri}/data-stream/meta/set-source", json=update_dict
        )

        self.evaluator.evaluate(response=r)

        if r.status_code != 200:
            response_data = r.json()
            if isinstance(response_data["detail"], list):
                for d in response_data["detail"]:
                    self._log(d["msg"])
            else:
                self._log(response_data)
            return False

        self._log(
            "✅ [green]Source of the stream has been successfully updated.[/green]"
        )
        return True

    def _update(self, data: list[dict], data_file: IO = None):

        if data_file is not None:
            data_file = ("data.json.gz", data_file, "application/json")

        m = MultipartEncoder(
            fields={
                "key": self.key,
                "data": json.dumps(data) if data is not None else "[]",
                "data_file": data_file,
            }
        )

        result = self.requests.post(
            f"{self.base_uri}/data-stream/add/data",
            data=m,
            headers={"Content-Type": m.content_type},
            stream=False,
        )  # files={"data_file": data_file}, data=update_definition

        self.evaluator.evaluate(response=result)
        result = result.json()

        # Check if update was successfull
        if "detail" in result:
            for d in result["detail"]:
                # Currently only check if data stream key exists
                if d["type"] == "data_warning.empty":
                    raise DataStreamNotExistsError

        assert "_key" in result, "ERROR_UPDATE"

        return {"success": True, "detail": result}

    def update(
        self,
        data: list[dict] = None,
        data_file_path: IO = None,
        chunk: bool = False,
        chunk_size: int = 1,
    ) -> dict:
        """
        Used to update a Datastream
        :param unique_label: The unique label of the datastream
        :param data: The data provided as a json or a list of dictionaries
        :param data_file_path: You can also provide the data as a gzipped file
        """
        assert (
            self.label is None and self.key is None
        ) == False, "NO_KEY_OR_UNIQUE_LABEL_GIVEN"

        if pd is None:
            raise ModuleNotFoundError("You must install pandas to use this feature.")

        stream_meta = dict()
        if self.key is None:
            stream_meta = self._get_meta_data_by_label(self.label)
        else:
            stream_meta["_key"] = self.key

        if not chunk:
            data_file = None
            # Check if file is used as data
            if isinstance(data_file_path, str):
                assert self._is_gzipped_file(
                    data_file_path
                ), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
                data_file = open(data_file_path, "rb")
                # Set data to None to only use the file
                data = None

            if isinstance(data, pd.core.frame.DataFrame):
                data = data.to_dict("records")

            result = self._update(data, data_file)

            # Close file
            if data_file is not None:
                try:
                    data_file.close()
                except AttributeError:
                    pass
        else:
            if isinstance(data_file_path, str):
                data_chunk_file_paths = self.data_chunker.chunk_file(
                    data_file_path=data_file_path,
                    chunk_size=chunk_size,
                    common_file_key=str(self.label),
                )
            elif isinstance(data, pd.core.frame.DataFrame):
                data_chunk_file_paths = self.data_chunker.chunk_dataframe(
                    df=data, chunk_size=chunk_size, common_file_key=str(self.label)
                )
            elif isinstance(data, list):
                data_chunk_file_paths = self.data_chunker.chunk_list(
                    data=data, chunk_size=chunk_size, common_file_key=str(self.label)
                )
            else:
                raise Exception(
                    "Unsupported data type provided: Use a Python list, DataFrame or .json.gz file"
                )

            for data_chunk_file_index, data_chunk_file_path in enumerate(
                data_chunk_file_paths
            ):
                data_file = open(data_chunk_file_path, "rb")
                result = self._update(None, data_file)

                assert result.get("success") == True, "UPDATE_ERROR"

                # Close file
                if data_file is not None:
                    try:
                        data_file.close()
                    except AttributeError:
                        pass

        return result

    def replace(
        self,
        cascade: bool = True,
        inplace: bool = False,
        data: list[dict] = None,
        data_file_path: IO = None,
        chunk: bool = False,
        chunk_size: int = 1,
        sanity_check: bool = True,
    ) -> dict:
        """
        Used to replace the data of a datastream
        :param cascade: If you want to cascade the data (Default is True)
        :param inplace: If the replacing should happen inplace (Default is False)
        :param data: The data provided as a json or a list of dictionaries
        :param data_file_path: You can also provide the data as a gzipped file
        :param chunk: Flag whether the data should send in chunks to Fusionbase
        :param chunk_size: Size of the data chunks in number of rows
        :param sanity_check:
        :return: The Success dictionary response returned by the Fusionbase API
        """

        assert isinstance(cascade, bool) and isinstance(
            inplace, bool
        ), "CASCADE_AND_INPLACE_MUST_BE_BOOLEAN"

        # Sanity check, this method can be destructive
        if sanity_check:
            print(" " * 80)
            print("[bold red underline]YOU ARE ABOUT TO REPLACE A DATA STREAM.")
            print(
                "[bold red underline]IF CASCADE == TRUE AND OR INPLACE == TRUE THERE IS NO WAY BACK!"
            )
            print(
                "[bold red underline]EVEN IN CASE BOTH ARE FALSE THERE IS NO IMPLEMENTED WAY BACK"
            )
            print("[blink bold red on white underline]DO YOU KNOW WHAT YOU ARE DOING?")
            print("\n")
            sanity_check = Prompt.ask(
                "Do you want to proceed? Enter [italic]yes[/italic]", default="no"
            )

            if sanity_check != "yes":
                print("Replace aborted.")
                return {
                    "success": False,
                    "upsert_type": "REPLACE",
                    "detail": "USER_ABORT",
                }
        else:
            print(
                f"[blink bold red underline]YOU ARE ABOUT TO REPLACE DATA STREAM '{self.label}' ({self.key})  -- God bless you and good luck."
            )

        if not chunk:
            data_file = None

            # Check if file is used as data
            if isinstance(data_file_path, str):
                assert self._is_gzipped_file(
                    data_file_path
                ), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
                data_file = open(data_file_path, "rb")
                # Set data to None to only use the file
                data = None

            sent_data_file = None
            if data_file is not None:
                # assert self._is_gzipped_file(data_file), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
                sent_data_file = ("data.json.gz", data_file, "application/json")

            m = MultipartEncoder(
                fields={
                    "key": self.key,
                    "inplace": "true" if inplace else "false",
                    "cascade": "true" if cascade else "false",
                    "data": json.dumps(data) if data is not None else "[]",
                    "data_file": sent_data_file,
                }
            )

            result = self.requests.post(
                f"{self.base_uri}/data-stream/replace",
                data=m,
                headers={"Content-Type": m.content_type},
                stream=False,
            )  # files={"data_file": data_file}, data=update_definition

            self.evaluator.evaluate(response=result)
            result = result.json()

            # Close file
            if data_file is not None:
                try:
                    data_file.close()
                except AttributeError:
                    pass

        # Chunk
        else:
            if isinstance(data_file_path, str):
                data_chunk_file_paths = self.data_chunker.chunk_file(
                    data_file_path=data_file_path,
                    chunk_size=chunk_size,
                    common_file_key=str(self.label),
                )
            elif isinstance(data, pd.core.frame.DataFrame):
                data_chunk_file_paths = self.data_chunker.chunk_dataframe(
                    df=data, chunk_size=chunk_size, common_file_key=str(self.label)
                )
            elif isinstance(data, list):
                data_chunk_file_paths = self.data_chunker.chunk_list(
                    data=data, chunk_size=chunk_size, common_file_key=str(self.label)
                )
            else:
                raise Exception(
                    "Unsupported data type provided: Use a Python list, DataFrame or .json.gz file"
                )

            for data_chunk_file_index, data_chunk_file_path in enumerate(
                data_chunk_file_paths
            ):
                data_file = open(data_chunk_file_path, "rb")
                sent_data_file = ("data.json.gz", data_file, "application/json")

                if data_chunk_file_index == 0:
                    m = MultipartEncoder(
                        fields={
                            "key": self.key,
                            "inplace": "true" if inplace else "false",
                            "cascade": "true" if cascade else "false",
                            "data": json.dumps(data) if data is not None else "[]",
                            "data_file": sent_data_file,
                        }
                    )
                    result = self.requests.post(
                        f"{self.base_uri}/data-stream/replace",
                        data=m,
                        headers={"Content-Type": m.content_type},
                        stream=False,
                    )
                    self.evaluator.evaluate(response=result)
                    result = result.json()

                else:
                    _result = self._update(None, data_file)
                    assert (
                        _result.get("success") == True
                    ), f"UPDATE_IN_REPLACE_ERROR : SEE : {_result}"

                # Close file
                if data_file is not None:
                    try:
                        data_file.close()
                    except AttributeError:
                        pass

        assert "_key" in result, "ERROR_IN_REPLACE"
        return {"success": True, "upsert_type": "REPLACE", "detail": result}

    def get_data(
        self,
        fields: list = None,
        skip: int = None,
        limit: int = None,
        query: dict = None,
        live: bool = False,
        multithread: bool = True,
        result_type: ResultType = ResultType.PYTHON_LIST,
        storage_path: Union[Path, str] = None,
    ) -> list:
        """
        Retrieve data from the Fusionbase API
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param skip: Pagination parameter to skip rows
        :param limit: Pagination parameter to limit rows
        :param live: Ignore cached results
        :param multithread: Whether multithreading should be used or not (Default is True)
        :param result_type: Defines how the data is returned / stored (Default is a Python list of dictionaries)
        :param storage_path: Path where the data is stored, only for valid result types *_FILES
        :return: The data as a list of dictionaries
        """

        # Check limit parameter boundaries
        if isinstance(limit, int) and (limit < -1 or limit == 0):
            raise ValueError("limit parameter must be > 0 or -1")

        # Check limit parameter boundaries
        if isinstance(skip, int) and skip < 0:
            raise ValueError("skip parameter must be >= 0")

        # Add Fusionbase columns by default to fields
        if isinstance(fields, list) and len(fields) > 0:
            fields.extend(["fb_id", "fb_data_version", "fb_datetime"])
            fields = list(dict.fromkeys(fields))  # Remove duplicates
        else:
            fields = None

        # Make sure pandas is installed for pandas dependened return types
        if result_type in [
            ResultType.CSV_FILES,
            ResultType.FEATHER_FILES,
            ResultType.PARQUET_FILES,
        ]:
            if pd is None:
                raise ModuleNotFoundError(
                    "You must install pandas to use this feature."
                )

        # Create storage path folder if it does not exist
        if result_type in [
            ResultType.JSON_FILES,
            ResultType.CSV_FILES,
            ResultType.PICKLE_FILES,
            ResultType.FEATHER_FILES,
            ResultType.PARQUET_FILES,
        ]:
            if storage_path is None:
                raise FileNotFoundError("STORAGE PATH IS NONE!")

            # Make sure storage path is of type Path
            if isinstance(storage_path, str):
                storage_path = Path(storage_path)

            storage_path = storage_path.joinpath(self.key).joinpath("data")
            storage_path.mkdir(parents=True, exist_ok=True)

        # Get 150 rows to estimate average row size of dataset, factor 1.2 is safety margin
        # TODO Get actual data through fusionbase api
        ONE_AND_A_HALF_GB = 1610612736
        average_row_size_in_bytes = self._estimate_average_row_size()
        max_allowed_limit = math.ceil(
            ONE_AND_A_HALF_GB / (average_row_size_in_bytes * 1.2)
        )

        if isinstance(limit, int) and limit > max_allowed_limit:
            self._log(
                f"[red]Limit can't exceed {max_allowed_limit}. Use multiple requests in batches instead![/red]",
                force=True,
            )
            self._log(
                f"[red]Limit is forcefully set to {max_allowed_limit}[/red]", force=True
            )
            self._log(
                "[yellow]Tip: If you want to get the whole dataset, leave skip and limit empty.[/yellow]",
                force=True,
            )
            self._log("")
            limit = max_allowed_limit

        # Currently a bit hacked, since we have no option to count query results before query execution
        # Result will be incorrect if the query matches more than 5000 entries.
        # TODO: Find a way to efficiently estimate results
        if isinstance(query, dict):
            limit = max_allowed_limit

        self._log(f"[bold blue]Meta Data", rule=True)
        self._log(f"Loading Datastream with key {self.key}")
        self._log(f"{Path(self.tmp_dir)} will be used to cache data partitions")

        if live == False:
            self._log(
                f"[italic bold]live[/italic bold] is [italic bright_red]FALSE[/italic bright_red] - Partitions will be loaded from local cache if possible"
            )
        else:
            self._log(
                f"[italic bold]live[/italic bold] is [italic bright_green]True[/italic bright_green] - All partitions will be pulled from fusionbase"
            )

        # First get only the meta data
        meta_data = self.get_meta_data()
        assert isinstance(
            meta_data, dict
        ), "ERROR COULD NOT RETRIEVE META DATA, TRY LATER"

        # self._log(f"✅ Meta data successfully retrieved")
        self._log(
            f'Name: [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan]'
        )

        # Make sure max batches is always at least 1
        cpu_count = os.cpu_count() if isinstance(os.cpu_count(), int) else 1
        assert isinstance(cpu_count, int)
        max_batches = cpu_count - 1 if cpu_count - 1 > 1 else 1

        # Remove stress from the server to only allow 10 concurrent threads
        if max_batches > 10:
            max_batches = 10

        self._log(
            f'We will use {max_batches if multithread else 1} cores/threads to retrieve the data.{str(" Multithreading is [bold red]OFF[/bold red]") if not multithread else str("")}'
        )

        self._log(
            f'The Datastream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan] has {"{:,}".format(meta_data["meta"]["entry_count"])} rows and {"{:,}".format(meta_data["meta"]["main_property_count"])} columns\n'
        )

        if (
            meta_data["meta"]["entry_count"] > 10_000_000
            and result_type != ResultType.JSON_FILES
        ):
            self._log(
                f"⚠️⚠️⚠️\n The Datastream contains a large number of rows. \n [bold red]Consider downloading it as JSON files first[/bold red] ([italic]stream.as_json_files(...)[/italic]) \n Any in-memory data structure will require a lot of RAM. \n⚠️⚠️⚠️\n"
            )

        # meta_data["meta"]["entry_count"] is calculate on each /get request and should be safe to use
        if (
            skip is not None
            and int(skip) > -1
            and limit is not None
            and int(limit) > -1
        ):
            # skip_limit_batches = [(skip, limit)]
            skip_limit_batches = self._calc_skip_limit_tuples(
                limit=limit,
                max_batches=max_batches,
                initial_skip=skip,
                average_row_size_bytes=average_row_size_in_bytes,
                stream_rows=meta_data["meta"]["entry_count"],
            )

        # Only limit is set without skip
        elif skip is None and limit is not None and limit > -1:
            # skip_limit_batches = [(0, limit)]
            skip_limit_batches = self._calc_skip_limit_tuples(
                limit=limit,
                max_batches=max_batches,
                initial_skip=0,
                average_row_size_bytes=average_row_size_in_bytes,
                stream_rows=meta_data["meta"]["entry_count"],
            )

        # Only skip is set without limit
        elif skip is not None and (limit is None or limit == -1):

            if int(meta_data["meta"]["entry_count"]) - skip < 1:
                raise Exception(
                    f"YOU CAN'T SKIP MORE ENTRIES THAN THE STREAM HAVE ROWS"
                )

            if (
                meta_data is not None
                and "meta" in meta_data
                and "entry_count" in meta_data["meta"]
            ):
                skip_limit_batches = self._calc_skip_limit_tuples(
                    limit=int(meta_data["meta"]["entry_count"]),
                    max_batches=max_batches,
                    initial_skip=skip,
                    average_row_size_bytes=average_row_size_in_bytes,
                    stream_rows=meta_data["meta"]["entry_count"],
                )

        else:
            if (
                meta_data is not None
                and "meta" in meta_data
                and "entry_count" in meta_data["meta"]
            ):
                skip_limit_batches = self._calc_skip_limit_tuples(
                    limit=int(meta_data["meta"]["entry_count"]),
                    max_batches=max_batches,
                    average_row_size_bytes=average_row_size_in_bytes,
                    stream_rows=meta_data["meta"]["entry_count"],
                )
            else:
                # Entry count metadata is missing
                raise Exception(f"DATA STREAM WITH KEY {self.key} HAS NO ENTRY COUNTY.")

        self._log(f"[bold blue]Data", rule=True)

        _tmp_path_set = set()
        # Initialize data result list
        result_datastream_data = list()

        request_semaphore = asyncio.BoundedSemaphore(max_batches)  # TODO: check

        async def __get_data_from_fusionbase(skip_limit, add_to_tmp_path_set=True):

            # Build unique name based on input arguments
            if fields is None:
                _tmp_name = f"fb_stream__{self.base_uri}__{str(self.key)}__{str(limit)}___{str(skip_limit[0])}__{str(skip_limit[0])}"
            else:
                _tmp_name = f"fb_stream__{self.base_uri}__{str(self.key)}__{str(limit)}___{str(skip_limit[0])}__{str(skip_limit[0])}___{'-'.join(fields)}"

            if isinstance(query, dict):
                _tmp_name = f"{_tmp_name}__{json.dumps(query)}"

            _tmp_name = (
                hashlib.sha3_256(_tmp_name.encode("utf-8")).hexdigest()[:12] + ".json"
            )
            _tmp_fpath = PurePath(self.tmp_dir, _tmp_name)

            if add_to_tmp_path_set:
                _tmp_path_set.add(_tmp_fpath)

            # Check if file is already cached
            if Path(f"{_tmp_fpath}.gz").is_file() and not live:
                return _tmp_fpath

            # Build request URL
            request_url = f"{self.base_uri}/data-stream/get/{self.key}?skip={skip_limit[0]}&limit={skip_limit[1]}&compressed_file=true"
            if fields is not None:
                field_query = ""
                for field in fields:
                    field_query = (
                        f"{field_query}&fields={urllib.parse.quote_plus(field)}"
                    )
                field_query = field_query.strip().strip("&").strip()
                request_url = f"{request_url}&{field_query}"

            # Construct query
            if query is not None:
                query_parameters = ""
                for key, val in query.items():
                    query_parameters = f'"{urllib.parse.quote_plus(key)}":"{urllib.parse.quote_plus(val)}",'
                query_parameters = query_parameters.strip().strip(",")
                query_parameters = "query={" + query_parameters + "}"
                request_url = f"{request_url}&{query_parameters}"

            try:
                async with request_semaphore, aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(
                        total=60 * 60
                    ),  # 60*60 => 60 minute timeout
                    headers={"x-api-key": self.auth["api_key"]},
                ) as session:
                    async with session.get(request_url) as resp:
                        assert resp.status == 200
                        data = await resp.read()

                    # Close session after successful download
                    await session.close()

                async with aiofiles.open(f"{_tmp_fpath}.gz", "wb") as outfile:
                    await outfile.write(data)

                return _tmp_fpath
            except Exception as e:
                request_url = f"{self.base_uri}/data-stream/get/{self.key}?skip={skip_limit[0]}&limit={skip_limit[1]}&compressed_file=true"
                self._log(f"Error during download - partition {request_url} failed")

        # Get event loop
        loop = asyncio.get_event_loop()

        try:
            # Check if user has access to more than 10 rows (default)
            tasks = [
                loop.create_task(
                    __get_data_from_fusionbase(skip_limit, add_to_tmp_path_set=False)
                )
                for skip_limit in [(0, 12)]
            ]
            # Check through the files and assert that these contain more than the default number of entries, otherwise user has no access
            for _tmp_file_path in loop.run_until_complete(asyncio.gather(*tasks)):
                try:
                    with gzip.open(f"{_tmp_file_path}.gz", "r") as fin:
                        data = json.loads(fin.read().decode("utf-8"))["data"]
                        if len(data) == 10:
                            # Adjust skip limit if user anyhow has no access to the DataStream
                            skip_limit_batches = [(0, 10)]
                        fin.close()

                except gzip.BadGzipFile as e:
                    continue
                finally:
                    # Remove file
                    Path(f"{_tmp_file_path}.gz").unlink()

            # Get the actual data
            tasks = [
                loop.create_task(__get_data_from_fusionbase(skip_limit))
                for skip_limit in skip_limit_batches
            ]

            if self.log:
                with Progress(
                    SpinnerColumn(),
                    *Progress.get_default_columns(),
                    TimeRemainingColumn(elapsed_when_finished=True),
                    MofNCompleteColumn(),
                ) as progress:

                    downloading_task = progress.add_task(
                        "[bold reverse green] Downloading DataStream ",
                        total=len(tasks),
                    )

                    # A bit hacky
                    # But didn't come up with another method to update the progress in sync method
                    for task_results in tasks:
                        for task_result in loop.run_until_complete(
                            asyncio.gather(*[task_results])
                        ):
                            _tmp_path_set.add(task_result)
                            progress.update(downloading_task, advance=1)
            else:
                # A bit hacky
                # But didn't come up with another method to update the progress in sync method
                for task_results in tasks:
                    for task_result in loop.run_until_complete(
                        asyncio.gather(*[task_results])
                    ):
                        _tmp_path_set.add(task_result)
        except Exception as e:
            self._log("Error during download - final dataset might be inclomplete.")

        # Generator for downloaded data
        def __load__tmp_files():
            data = None
            for _tmp in _tmp_path_set:
                # Unpack gzip and put into df
                try:
                    with gzip.open(f"{_tmp}.gz", "r") as fin:
                        data = json.loads(fin.read().decode("utf-8"))["data"]
                        yield data
                except gzip.BadGzipFile:
                    continue

        # Process downloaded files
        def __process_tmp_files(tmp_path_file):
            try:
                _part_hash = hashlib.sha3_256(
                    str(tmp_path_file).encode("utf-8")
                ).hexdigest()[:12]

                with gzip.open(f"{tmp_path_file}.gz", "r") as fin:
                    data = json.loads(fin.read().decode("utf-8"))["data"]
                    if result_type == ResultType.PYTHON_LIST:
                        return data
                    elif result_type == ResultType.PD_DATAFRAME:
                        return data
                    elif result_type == ResultType.JSON_FILES:
                        with open(
                            PurePath(
                                storage_path,
                                f"ds_{self.key}_part_{_part_hash}_unordered.json",
                            ),
                            "w",
                            encoding="utf-8",
                        ) as fp:
                            # Do not use json.dump since this is not provided if orjson is used
                            # orjson.dumps => bytes  json.dumps => str
                            # Unfortunate but it is like that
                            data_json = json.dumps(data)
                            if isinstance(data_json, bytes):
                                data_json = data_json.decode("utf-8")
                            fp.write(data_json)
                            fp.close()
                    elif result_type == ResultType.CSV_FILES:
                        df = pd.DataFrame(data)
                        df.to_csv(
                            PurePath(
                                storage_path,
                                f"ds_{self.key}_part_{_part_hash}_unordered.csv",
                            ),
                            index=False,
                            quoting=csv.QUOTE_ALL,
                        )
                    elif result_type == ResultType.FEATHER_FILES:
                        df = pd.DataFrame(data)
                        df.to_feather(
                            PurePath(
                                storage_path,
                                f"ds_{self.key}_part_{_part_hash}_unordered.feather",
                            )
                        )
                    elif result_type == ResultType.PARQUET_FILES:
                        df = pd.DataFrame(data)
                        df.to_parquet(
                            PurePath(
                                storage_path,
                                f"ds_{self.key}_part_{_part_hash}_unordered.parquet",
                            ),
                            index=False,
                        )
                    elif result_type == ResultType.PICKLE_FILES:
                        with open(
                            PurePath(
                                storage_path,
                                f"ds_{self.key}_part_{_part_hash}_unordered.pkl",
                            ),
                            "wb",
                        ) as fp:
                            pickle.dump(data, fp)
                            fp.close()

                    return None
            except gzip.BadGzipFile:
                return None

        # Use a single process for Python list and DataFrame
        # Performance gain was not really present, specifically for smaller datasets
        seen_ids = set()
        if (
            result_type == ResultType.PYTHON_LIST
            or result_type == ResultType.PD_DATAFRAME
            and True
        ):
            if self.log == False:
                for i, data in enumerate(__load__tmp_files()):
                    if result_type == ResultType.PYTHON_LIST:
                        result_datastream_data.extend(data)
                    elif result_type == ResultType.PD_DATAFRAME:
                        result_datastream_data.extend(data)
            else:
                with Progress(
                    SpinnerColumn(),
                    *Progress.get_default_columns(),
                    TimeRemainingColumn(elapsed_when_finished=True),
                    MofNCompleteColumn(),
                ) as progress:

                    processing_task = progress.add_task(
                        "[bold reverse green] Processing DataStream  ",
                        total=len(_tmp_path_set),
                        visible=self.log,
                    )

                    for i, data in enumerate(__load__tmp_files()):
                        if result_type == ResultType.PYTHON_LIST:
                            result_datastream_data += data
                        elif result_type == ResultType.PD_DATAFRAME:
                            result_datastream_data += data
                        progress.update(processing_task, advance=1)

        # Use multiprocessing for file generation
        else:
            with PPool(nodes=max_batches) as pool:
                process_file_results = pool.imap(__process_tmp_files, _tmp_path_set)

                if self.log:
                    with Progress(
                        SpinnerColumn(),
                        *Progress.get_default_columns(),
                        TimeRemainingColumn(elapsed_when_finished=True),
                        MofNCompleteColumn(),
                    ) as progress:
                        processing_task = progress.add_task(
                            "[bold reverse green] Processing DataStream  ",
                            total=len(_tmp_path_set),
                            visible=self.log,
                        )

                        for pif in process_file_results:
                            progress.update(processing_task, advance=1)
                else:
                    process_file_results = [pif for pif in process_file_results]

        self._log("[bold blue] Summary", rule=True)
        result = result_datastream_data
        if result_type == ResultType.PD_DATAFRAME:
            result = pd.DataFrame(result_datastream_data)
            self._log(f"\n\n [green bold] Datastream successfully retrieved")
        elif result_type in [
            ResultType.JSON_FILES,
            ResultType.CSV_FILES,
            ResultType.PICKLE_FILES,
            ResultType.FEATHER_FILES,
            ResultType.PARQUET_FILES,
        ]:
            result = None
            self._log(f"\n\n[green bold]Datastream successfully retrieved")
            self._log(f"cd {storage_path.resolve()}")

        return result

    def get_delta_data(
        self,
        version: str,
        fields: list = None,
        live: bool = True,
        result_type: ResultType = ResultType.PYTHON_LIST,
        storage_path: Path = None,
    ) -> list:
        """
        Retrieve data from the Fusionbase API since a specified fb_data_version
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param version: The  fb_data_version starting from from which new data should be downloaded
        :return: The data as a list of dictionaries
        """
        if fields is not None and len(fields) > 0:
            warnings.warn(
                "You set the fields parameter on the delta data method. However, the fields parameter is currently ignored.",
                UserWarning,
                stacklevel=3,
            )

        if fields is None:
            fields = []

        version = str(version)
        VERSION_PATTERN = r"^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$"

        version_matched = re.match(VERSION_PATTERN, version)

        if self.key is None or "":
            raise Exception("A VALID KEY MUST BE SPECIFIED!")

        if (version is None or "") or not bool(version_matched):
            raise Exception("A VALID VERSION MUST BE SPECIFIED!")

        # Add Fusionbase columns by default to fields
        fields.extend(["fb_id", "fb_data_version", "fb_datetime"])
        fields = list(dict.fromkeys(fields))  # Remove duplicates

        # Make sure pandas is installed for pandas dependened return types
        if result_type in [
            ResultType.CSV_FILES,
            ResultType.FEATHER_FILES,
            ResultType.PARQUET_FILES,
        ]:
            if pd is None:
                raise ModuleNotFoundError(
                    "You must install pandas to use this feature."
                )

        # Create storage path folder if it does not exist
        if result_type in [
            ResultType.JSON_FILES,
            ResultType.CSV_FILES,
            ResultType.PICKLE_FILES,
            ResultType.FEATHER_FILES,
            ResultType.PARQUET_FILES,
        ]:
            storage_path = storage_path.joinpath(self.key).joinpath("data")
            storage_path.mkdir(parents=True, exist_ok=True)

        self._log(f"[bold blue]Meta Data", rule=True)
        self._log(f"Loading Datastream with key {self.key}")
        self._log(f"{Path(self.tmp_dir)} will be used to cache data partitions")

        # First get only the meta data
        meta_data = self.get_meta_data()

        # Check if user is trying to get from the latest version
        if version == meta_data.get("data_version"):
            self._log(f"[green bold]\n✅ You are already on the latest version. ✅")
            if result_type == ResultType.PYTHON_LIST:
                return list()
            if result_type == ResultType.PD_DATAFRAME:
                return pd.DataFrame([])

            return None

        assert isinstance(
            meta_data, dict
        ), "ERROR COULD NOT RETRIEVE META DATA, TRY LATER"

        self._log(
            f'Preparing delta data for stream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan] since version: [bold]{version}[/bold]'
        )

        self._log(
            f'The Datastream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan] has {"{:,}".format(meta_data["meta"]["entry_count"])} rows and {"{:,}".format(meta_data["meta"]["main_property_count"])} columns\n'
        )

        self._log(f"[bold blue]Data", rule=True)

        # Build unique name based on input arguments
        _tmp_name = f"fb_stream__delta__{version}___{'-'.join(fields)}"

        _tmp_name = (
            hashlib.sha3_256(_tmp_name.encode("utf-8")).hexdigest()[:12] + ".json"
        )

        _tmp_fpath = PurePath(self.tmp_dir, _tmp_name)

        # Currently it always returns a single result file, however this will change perspectively
        # Therefore we directly set it up as a set
        _tmp_path_set = set()
        _tmp_path_set.add(_tmp_fpath)

        # Initialize data result list
        result_datastream_data = list()

        if Path(f"{_tmp_fpath}.gz").is_file() and live == False:
            # self._log(f"Cache hit! Skip downloading ... ")
            pass

        else:

            request_url = f"{self.base_uri}/data-stream/get/delta/{self.key}/{version}"

            # TODO: Retrieving only specific fields using the delta data is currently not implemented on the server side
            if fields is not None:
                field_query = ""
                for _index, field in enumerate(fields):
                    if _index == 0:
                        # Check if this is the first query parameter
                        if "?" not in request_url:
                            field_query = f"?fields={urllib.parse.quote_plus(field)}"
                        else:
                            field_query = f"&fields={urllib.parse.quote_plus(field)}"
                    else:
                        field_query = (
                            f"{field_query}&fields={urllib.parse.quote_plus(field)}"
                        )
                field_query = field_query.strip().strip("&").strip()
                request_url = f"{request_url}{field_query}"

            r = self.requests.get(request_url, stream=True)
            self.evaluator.evaluate(response=r)

            if (
                r.status_code == 401
                and r.content
                == b'{"detail":[{"loc":"","msg":"You are not authorized to access this resource.","type":"authorization_error.missing"}]}'
            ):
                raise Exception(
                    f"IT SEEMS LIKE YOU DON'T HAVE ACCESS TO DATASTREAM WITH STREAM-ID {self.key}"
                )

            if r.status_code != 200:
                raise Exception(
                    f"SOMETHING WEN'T WRONG WHILE REQUESTING DATASTREAM WITH STREAM-ID {self.key} and DATA AFTER VERSION: {version}, \n RESPONSE CODE WAS : {r.status_code}"
                )

            total_download_size = int(r.headers.get("content-length", 0))

            # Store the file as gzip
            if self.log:

                with gzip.open(f"{_tmp_fpath}.gz", "wb") as fopen, Progress(
                    SpinnerColumn(),
                    *Progress.get_default_columns(),
                    TimeRemainingColumn(elapsed_when_finished=True),
                    DownloadColumn(),
                ) as progress:

                    download_task = progress.add_task(
                        "[bold reverse green] Downloading Delta     ",
                        total=total_download_size,
                    )
                    for data in r.iter_content(chunk_size=1024):
                        size = fopen.write(data)
                        progress.update(download_task, advance=size)
                    fopen.close()

            else:
                with gzip.open(f"{_tmp_fpath}.gz", "wb") as fopen:
                    for data in r.iter_content(chunk_size=1024):
                        size = fopen.write(data)
                    fopen.close()

        # Number of new entries since "version"
        new_entry_count = 0

        def __load__tmp_files():
            data = None
            for _tmp in _tmp_path_set:
                # Unpack gzip and put into df
                try:
                    with gzip.open(f"{_tmp}.gz", "r") as fin:
                        data = json.loads(fin.read().decode("utf-8"))["data"]
                        yield data
                except gzip.BadGzipFile:
                    raise Exception(
                        f"DATA STREAM WITH KEY {self.key} AND VERSION {version}: COMPRESSED FILE: {_tmp_fpath}.gz COULDN'T BE UNPACKED SEE: \n {e}"
                    )

        with Progress(
            SpinnerColumn(),
            *Progress.get_default_columns(),
            TimeRemainingColumn(elapsed_when_finished=True),
            MofNCompleteColumn(),
        ) as progress:

            processing_task = progress.add_task(
                "[bold reverse green] Processing DataStream ",
                total=len(_tmp_path_set),
                visible=self.log,
            )

            for i, data in enumerate(__load__tmp_files()):

                # Count new entries
                new_entry_count += len(data)

                if result_type == ResultType.PYTHON_LIST:
                    result_datastream_data.extend(data)
                elif result_type == ResultType.PD_DATAFRAME:
                    result_datastream_data.extend(data)
                elif result_type == ResultType.JSON_FILES:
                    with open(
                        PurePath(
                            storage_path, f"ds_{self.key}_part_{i}_unordered.json"
                        ),
                        "w",
                        encoding="utf-8",
                    ) as fp:
                        data_json = json.dumps(data)
                        if isinstance(data_json, bytes):
                            data_json = data_json.decode("utf-8")
                        fp.close()
                elif result_type == ResultType.CSV_FILES:
                    df = pd.DataFrame(data)
                    df.to_csv(
                        PurePath(storage_path, f"ds_{self.key}_part_{i}_unordered.csv"),
                        index=False,
                        quoting=csv.QUOTE_ALL,
                    )
                elif result_type == ResultType.FEATHER_FILES:
                    df = pd.DataFrame(data)
                    df.to_feather(
                        PurePath(
                            storage_path, f"ds_{self.key}_part_{i}_unordered.feather"
                        )
                    )
                elif result_type == ResultType.PARQUET_FILES:
                    df = pd.DataFrame(data)
                    df.to_parquet(
                        PurePath(
                            storage_path, f"ds_{self.key}_part_{i}_unordered.parquet"
                        ),
                        index=False,
                    )
                elif result_type == ResultType.PICKLE_FILES:
                    with open(
                        PurePath(storage_path, f"ds_{self.key}_part_{i}_unordered.pkl"),
                        "wb",
                    ) as fp:
                        pickle.dump(data, fp)
                        fp.close()

                progress.update(processing_task, advance=1)

        result = result_datastream_data
        if result_type == ResultType.PD_DATAFRAME:
            result = pd.DataFrame(result_datastream_data)
        elif result_type in [
            ResultType.JSON_FILES,
            ResultType.CSV_FILES,
            ResultType.PICKLE_FILES,
            ResultType.FEATHER_FILES,
            ResultType.PARQUET_FILES,
        ]:
            result = None

        self._log("[bold blue] Summary", rule=True)

        if new_entry_count == 0:
            self._log(
                f"No new entries found, you seem to have the latest version {version}\nThe datastream was last updated at: { meta_data['updated_at']})"
            )

        self._log(f"\n\n [green bold] Datastream successfully retrieved")

        return result

    def get_delta_dataframe(self, version: str, fields: list = None) -> pd.DataFrame:
        """
        Retrieve data from the Fusionbase API since a specified fb_data_version and return it as a valid pandas Dataframe
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param version: The  fb_data_version starting from from which new data should be downloaded
        :return: The data as a pandas Dataframe
        """
        if pd is None:
            raise ModuleNotFoundError("You must install pandas to use this feature.")

        if fields is None:
            fields = []

        data = self.get_delta_data(version=version, fields=fields)

        df = pd.DataFrame(data)
        return df

    def get_dataframe(
        self,
        fields: list = None,
        skip: int = None,
        limit: int = None,
        live: bool = False,
        multithread: bool = True,
    ) -> pd.DataFrame:
        """
        Retrieve data from the Fusionbase API and return it as a valid pandas Dataframe
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param skip: Pagination parameter to skip rows
        :param limit: Pagination parameter to limit rows
        :param live:
        :param multithread: Whether multithreading should be used or not (Default is True)
        :return: The data as a pandas Dataframe
        """
        if pd is None:
            raise ModuleNotFoundError("You must install pandas to use this feature.")

        if fields is None:
            fields = []

        df = self.get_data(
            fields=fields,
            skip=skip,
            limit=limit,
            live=live,
            multithread=multithread,
            result_type=ResultType.PD_DATAFRAME,
        )

        df.drop_duplicates(subset=["fb_id"], inplace=True)
        return df

    def as_dataframe(self, live=False):
        """
        Retrieve data from the Fusionbase API and return it as a valid pandas Dataframe.
        This method is equivalent to `.get_dataframe()` without any parameters set
        :return: The data as a pandas Dataframe
        """
        return self.get_dataframe(live=live)

    def as_list(self, live=False):
        """
        Retrieve data from the Fusionbase API and return it as a Python list.
        This method is equivalent to `.get_data()` without any parameters set
        :return: The data as a Python list
        """
        return self.get_data(live=live)

    def as_json_files(self, storage_path, live=False):
        """
        Retrieve data from the Fusionbase API and stores is as partitioned JSON files at `storage_path`.
        This method is equivalent to `.get_data()` without any parameters set
        :param storage_path: Path where the data is stored
        :return: None
        """
        return self.get_data(
            live=live, result_type=ResultType.JSON_FILES, storage_path=storage_path
        )

    def as_pickle_files(self, storage_path, live=False):
        """
        Retrieve data from the Fusionbase API and stores is as partitioned Pickle files at `storage_path`.
        This method is equivalent to `.get_data()` without any parameters set
        :param storage_path: Path where the data is stored
        :return: None
        """
        return self.get_data(
            live=live, result_type=ResultType.PICKLE_FILES, storage_path=storage_path
        )

    def as_csv_files(self, storage_path, live=False):
        """
        Retrieve data from the Fusionbase API and stores is as partitioned CSV files at `storage_path`.
        This method is equivalent to `.get_data()` without any parameters set
        :param storage_path: Path where the data is stored
        :return: None
        """
        return self.get_data(
            live=live, result_type=ResultType.CSV_FILES, storage_path=storage_path
        )
