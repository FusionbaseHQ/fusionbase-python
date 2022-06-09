from __future__ import annotations

import concurrent.futures
from itertools import islice
import gzip
import hashlib
import json
import math
import os
import platform
import re
import tempfile
import time
import traceback
import urllib.parse
from pathlib import Path, PurePath
from typing import IO, Union

import numpy as np
import pandas as pd
import requests
from requests_toolbelt import MultipartEncoder
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from tqdm import tqdm

from fusionbase.exceptions.DataStreamNotExistsError import DataStreamNotExistsError
from fusionbase.exceptions.ResponseEvaluator import ResponseEvaluator


class DataStream:

    def __init__(self, auth: dict, connection: dict, config: dict = None, log: bool = False) -> None:
        """
       Used to initialise a new DataStream Object
       :param auth: the standard authentication object to authenticate yourself towards the fusionbase API
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

        self.auth = auth
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.requests = requests.Session()
        self.requests.headers.update({'x-api-key': self.auth["api_key"]})
        self.log = log
        self.console = Console()
        self.evaluator = ResponseEvaluator()

        if "cache_dir" in config:
            self.tmp_dir = PurePath(Path(config["cache_dir"]))
        else:
            self.tmp_dir = PurePath(Path(
                "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()), 'fusionbase')
        # Ensure that tmp/cache directory exists
        Path(self.tmp_dir).mkdir(parents=True, exist_ok=True)

    def _log(self, message: str, force=False) -> None:
        if not self.log and not force:
            return None
        else:
            self.console.log(message)

    @staticmethod
    def _is_gzipped_file(file_obj) -> bool:
        with gzip.open(file_obj, 'r') as fh:
            try:
                fh.read(1)
                fh.close()
                return True
            except gzip.BadGzipFile:
                return False

    @staticmethod
    def _calc_skip_limit_tuples(limit: int, max_batches: int = 20, initial_skip=0):

        def chunks(it, size):
            it = iter(it)
            return iter(lambda: tuple(islice(it, size)), ())

        part_duration = math.floor(limit / max_batches)
        skip_limits = [(i * part_duration + initial_skip, (i + 1) * part_duration - i * part_duration)
                       for i in range(max_batches)]
        if (skip_limits[-1][0] + skip_limits[-1][1]) < limit:
            skip_limits.append((skip_limits[-1][0] + skip_limits[-1][1], limit-(skip_limits[-1][0] + skip_limits[-1][1]) + 1))
        # Remove (0,0) tuple values
        skip_limits = list(filter(lambda x: x > (0, 0), skip_limits))

        # Remove tuples that contain higher starting numbers the the actual data stream size
        skip_limits = list(filter(lambda x: x[0] <= limit, skip_limits))

        # Chunks are bigger than 150k entries, i.e. they exceed the hard limit
        # Make chunks into smaller batches of 100k each
        if skip_limits[0][1] >= 150000:
            # 102161 is just a random prime number, could by any number < 150k
            skip_limit_chunks = list(chunks(range(0, limit+1), 102161))
            skip_limits = [(chunk[0]+initial_skip, chunk[-1]+1 - chunk[0])
                       for i, chunk in enumerate(skip_limit_chunks)]

        # Remove tuples that contain higher starting numbers the the actual data stream size
        # It is indended to filter it twice
        skip_limits = list(filter(lambda x: x[0] <= limit, skip_limits))

        return skip_limits

    def pretty_meta_data(self, key: Union[str, int]) -> None:
        """
        Retrieves the metadata from a Stream by giving a Service specific key and prints it nicely to console
        :param key: The key of the Stream either as a string or integer value
        """
        meta_data = self.get_meta_data(key=key)
        table = Table(title=meta_data["name"]["en"])

        table.add_column("Property", justify="right",
                         style="magenta", no_wrap=True)
        table.add_column("Value", style="cyan")

        def __add_row(table, label, data, key):
            if key in data:
                table.add_row(label, str(data[key]))
            return table

        table = __add_row(table, "Key", meta_data, "_key")
        table = __add_row(table, "Unique Label", meta_data, "unique_label")
        table = __add_row(table, "# Entries", meta_data["meta"], "entry_count")
        table = __add_row(table, "# Props",
                          meta_data["meta"], "main_property_count")
        table = __add_row(table, "Data Version", meta_data, "data_version")
        table = __add_row(table, "Data Update", meta_data, "data_updated_at")
        table = __add_row(table, "Store Version", meta_data, "store_version")
        table = __add_row(table, "Source", meta_data["source"], "label")
        table = __add_row(table, "Source Key", meta_data["source"], "_key")

        print("\n" * 2)
        self._log(table, True)
        print("\n" * 2)

    def get_meta_data(self, key: Union[str, int]) -> Union[None, dict]:
        """
       Retrieves the metadata from a Stream by giving a Stream specific key
       :param key: The key of the Stream either as a string or integer value
       :return: The metadata for the given service as a python dictionary
       """
        r = self.requests.get(
            f"{self.base_uri}/data-stream/get/{key}/meta")
        self.evaluator.evaluate(r)
        meta = r.json()
        return meta

    def get_meta_data_by_label(self, label: str) -> dict:
        """
        Retrieves the metadata from a Stream by providing the unique label of the Datastream
        :param label: The unique label of the Datastream
        :return: the metadata as a python dictionary
        """
        # 1. Get ID by the label
        r = self.requests.get(f"{self.base_uri}/data-stream/get/label/{label}")
        self.evaluator.evaluate(response=r)
        data = r.json()

        # 2. Get the data stream
        if data is None or r.status_code != 200 or "_key" not in data:
            self._log(
                f"Datastream with unique label [bold]{label}[/bold] does not exist")
            return None

        data_stream_meta_data = self.get_meta_data(data["_key"])
        return data_stream_meta_data

    def get_schema_by_label(self, label: str) -> dict:
        """
        Returns the schema of a datastream by providing the unique label
        :param label: The unique label of the datastream
        :return: The schema of the datastream as a dict
        """
        meta_data = self.get_meta_data_by_label(label)
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
            f'{self.base_uri}/data-stream/meta/update', json=meta_data)
        self.evaluator.evaluate(response=r)

        self._log(r.text)
        return r.status_code

    def set_source(self, data_stream_key: Union[int, str], source_key: Union[int, str], stream_specific: dict) -> bool:
        """
        Used to set the source of a provided Datastream
        :param stream_specific:
        :param data_stream_key: The key of the Datastream
        :param source_key: The key of the Datasource
        :return:
        """
        assert "uri" in stream_specific, "STREAM_SPECIFIC_URI_IS_REQUIRED"
        update_dict = {
            "data_stream_key": data_stream_key,
            "source_key": source_key,
            "stream_specific_uri": stream_specific["uri"]
        }
        r = self.requests.patch(
            f'{self.base_uri}/data-stream/meta/set-source', json=update_dict)

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
            "✅ [green]Source of the stream has been successfully updated.[/green]")
        return True

    def _create(self, unique_label: str, name: dict, description: Union[dict, set], scope: str, source: str,
               data,
               data_file: IO = None,
               provision: str = "MARKETPLACE") -> dict:
        """
        Used to create a new Datastream
        :param unique_label: The unique label of the datastream
        :param name: The name of the Datastream as a dict
        :param description: The Description of the datastream as a dict
        :param scope: The Scope of the stream either "PUBLIC" or "PRIVATE"
        :param source: The Datastream source
        :param data: The data provided as a json or a list of dictionaries
        :param data_file: You can also provide the data as a gzipped file
        :param provision: ["MARKETPLACE", "PRIVATE"]
        :return: The result dict returned by the Fusionbase API
        """
        unique_label = unique_label.strip()

        assert len(unique_label) > 0, "UNIQUE_LABEL_REQUIRED"
        assert isinstance(name, dict), "NAME_MUST_BE_A_DICT"
        assert "en" in name.keys() and len(
            name["en"]) > 0, "NAME_EN_MUST_BE_SET"
        assert isinstance(description, dict), "DESCRIPTION_MUST_BE_A_DICT"
        assert "en" in description.keys() and len(
            description["en"]) > 0, "DESCRIPTION_EN_MUST_BE_SET"
        assert scope in [
            "PUBLIC", "PRIVATE"], "SCOPE_MUST_BE_PUBLIC_OR_PRIVATE"

        assert isinstance(
            data, list) or data is None, "DATA_MUST_BE_LIST_OF_DICTS"
        assert data is not None or data_file is not None, "DATA_MUST_BE_LIST_OF_DICTS_OR_FILE"

        assert provision in ["MARKETPLACE",
                             "PRIVATE"], "INCORRECT_PROVISION_TYPE"

        data_stream_definition = {
            "unique_label": unique_label,
            "name": name,
            "description": description,
            "provision": provision,
            "scope": scope,
            "source": source,
            "data": data
        }

        if data_file is not None:
            # assert self._is_gzipped_file(data_file), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
            data_file = ("data.json.gz", data_file, "application/json")

        m = MultipartEncoder(
            fields={'data_stream_definition': json.dumps(data_stream_definition),
                    "data_file": data_file}
        )

        result = self.requests.post(
            f"{self.base_uri}/data-stream/new", data=m, headers={'Content-Type': m.content_type}, stream=False)

        self.evaluator.evaluate(response=result)
        result = result.json()

        if "detail" in result and "error" in result["detail"]:
            return {
                "success": False,
                **result
            }

        assert "_key" in result, "ERROR_CREATE"

        return {
            "success": True,
            "detail": result
        }

    def _update(self, key: Union[str, int], data: list[dict], data_file: IO = None):

        if data_file is not None:
            # assert self._is_gzipped_file(data_file), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
            data_file = ("data.json.gz", data_file, "application/json")

        m = MultipartEncoder(
            fields={"key": key,
                    "data": json.dumps(data) if data is not None else "[]",
                    "data_file": data_file}
        )

        result = self.requests.post(
            f"{self.base_uri}/data-stream/add/data", data=m, headers={'Content-Type': m.content_type},
            stream=False)  # files={"data_file": data_file}, data=update_definition

        self.evaluator.evaluate(response=result)
        result = result.json()

        # Check if update was successfull
        if "detail" in result:
            for d in result["detail"]:
                # Currently only check if data stream key exists
                if d["type"] == 'data_warning.empty':
                    return {
                        "success": False,
                        "detail": "DATA_STREAM_DOES_NOT_EXIST"
                    }
        assert "_key" in result, "ERROR_UPDATE"

        return {
            "success": True,
            "detail": result
        }

    def update(self, unique_label: str = None, key: Union[str, int] = None, data: list[dict] = None,
               data_file_path: IO = None) -> dict:
        """
        Used to update a Datastream
        :param unique_label: The unique label of the datastream
        :param key: The key of the Datatsream you want to update either as an integer or string
        :param data: The data provided as a json or a list of dictionaries
        :param data_file_path: You can also provide the data as a gzipped file
        """
        assert (
            unique_label is None and key is None) == False, "NO_KEY_OR_UNIQUE_LABEL_GIVEN"

        stream_meta = dict()
        if key is None:
            stream_meta = self.get_meta_data_by_label(unique_label)
        else:
            stream_meta["_key"] = key

        data_file = None
        # Check if file is used as data
        if isinstance(data_file_path, str):
            # print(data_file_path)
            assert self._is_gzipped_file(
                data_file_path), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
            data_file = open(data_file_path, "rb")
            # Set data to None to only use the file
            data = None

        result = self._update(stream_meta["_key"], data, data_file)

        # Close file
        if data_file is not None:
            try:
                data_file.close()
            except AttributeError:
                pass
        return result

    def update_create(self, unique_label: str, name, description, scope, source, data: list[dict] = None,
                      data_file_path: IO = None,
                      provision: str = "MARKETPLACE",
                      chunk: bool = False,
                      chunk_size: int = None) -> dict:
        """
        Main method to invoke the update or creation of a new Datastream
        :param unique_label: The unique label of the datastream
        :param name: The name of the Datastream as a dict
        :param description: The Description of the datastream as a dict
        :param scope: The Scope of the stream either "PUBLIC" or "PRIVATE"
        :param source: The Datastream source
        :param data: The data provided as a json or a list of dictionaries
        :param data_file_path: You can also provide the data as a gzipped file
        :param provision: ["MARKETPLACE", "PRIVATE"]
        :param chunk: Whether you want to upload the data in junks or not default is False
        :param chunk_size: The size of the chunks during the upload
        :return:
        """

        start_time = time.time()
        data_chunks = []
        data_chunk_files = []

        if data is not None and data_file_path is not None:
            self._log(
                "[red]WARNING:[/red] YOU PROVIDED DATA IN MEMORY AND VIA FILE. TAKE ONLY MEMORY NOW")

        # Check if data is provided via file path
        if data is None and isinstance(data_file_path, str):
            assert self._is_gzipped_file(
                data_file_path), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
            data_chunk_files.append(data_file_path)
        else:
            data_chunks = [data]

            # Chunk the data
        if chunk:
            # Read only if chunks are required
            if isinstance(data_file_path, str):
                data = pd.read_json(data_file_path)
                # Remove the full file from the chunk files
                data_chunk_files = []
            if chunk_size is None or isinstance(chunk_size, int) == False:
                chunk_size = math.ceil(len(data) / 1_000_000)
            self._log(
                f"Split dataset with {len(data)} rows into {1 if chunk_size is None else chunk_size} chunks.")
            data_chunks = np.array_split(data, chunk_size)

        # Build files for upload
        for chunk_index, data_chunk in enumerate(data_chunks):
            chunk_file_path = str(
                PurePath(self.tmp_dir, f"__tmp_df_upload_{chunk_index}.json.gz"))
            # Check if data is provided as dataframe
            if isinstance(data_chunk, pd.core.frame.DataFrame):
                data_chunk.to_json(chunk_file_path, orient="records")
            elif isinstance(data_chunk, np.ndarray):
                data_chunk = list(data_chunk)
                with gzip.open(chunk_file_path, 'wt', encoding='UTF-8') as zipfile:
                    json.dump(data_chunk, zipfile)
            else:
                raise Exception(
                    "INCORRECT DATA TYPE PROVIDED, EITHER LIST OF DICTS, DATAFRAME OR .JSON.GZ FILE!!")

            data_chunk_files.append(chunk_file_path)

        result = None
        upsert_type = None

        for data_chunk_file_index, data_chunk_file in enumerate(data_chunk_files):
            
            try:
                stream_meta = self.get_meta_data_by_label(unique_label)
            except DataStreamNotExistsError as e:
                stream_meta = None

            if isinstance(data_chunk_file, str):
                # print(data_file_path)
                assert self._is_gzipped_file(
                    data_chunk_file), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
                data_file = open(data_chunk_file, "rb")
                # Set data to None to only use the file
                data = None

                # Datasetream does not exist create a new one
                if stream_meta is None or not isinstance(stream_meta, dict) or "_key" not in stream_meta:
                    result = self._create(unique_label, name,
                                         description, scope, source, data, data_file, provision)
                    upsert_type = "CREATE"
                    result["upsert_type"] = upsert_type
                else:
                    result = self._update(stream_meta["_key"], data, data_file)

                    if upsert_type != "CREATE":
                        upsert_type = "UPDATE"

                    result["upsert_type"] = upsert_type
                    # Close file
                    if data_file is not None:
                        try:
                            data_file.close()
                        except AttributeError:
                            pass

                self._log(
                    f"Push chunk {data_chunk_file_index + 1} of {1 if chunk_size is None else chunk_size} chunks.")

                if not result["success"]:
                    self._log(
                        f"[red]ERROR: CHUNK {data_chunk_file_index} FAILED -- REST IS STILL GOING.[/red]")

        self._log(f"All chunks done.")
        self._log(f"Execution time :: {time.time() - start_time}")
        return result

    def replace(self, unique_label: str, cascade: bool = True, inplace: bool = False, data: list[dict] = None,
                data_file_path: IO = None,
                sanity_check: bool = True) -> dict:
        """
        Used to replace the data of a datastream
        :param unique_label: The unique label of the datastream
        :param cascade: If you want to cascade the data (Default is True)
        :param inplace: If the replacing should happen inplace (Default is False)
        :param data: The data provided as a json or a list of dictionaries
        :param data_file_path: You can also provide the data as a gzipped file
        :param sanity_check:
        :return: The Success dictionary response returned by the Fusionbase API
        """

        assert isinstance(cascade, bool) and isinstance(
            inplace, bool), "CASCADE_AND_INPLACE_MUST_BE_BOOLEAN"

        # Sanity check, this method can be destructive
        if sanity_check:
            self.console.print(" " * 80, style="red on white")
            self.console.print(
                "YOU ARE ABOUT TO REPLACE A DATA STREAM.", style="blink bold red underline")
            self.console.print(
                "IF CASCADE == TRUE AND OR INPLACE == TRUE THERE IS NO WAY BACK!", style="blink bold red underline")
            self.console.print(
                "EVEN IN CASE BOTH ARE FALSE THERE IS NO IMPLEMENTED WAY BACK", style="blink bold red underline")
            self.console.print("DO YOU KNOW WHAT YOU ARE DOING?",
                               style="blink bold red underline")
            print("\n")
            sanity_check = Prompt.ask(
                "Do you want to proceed? Enter [italic]yes[/italic]", default="no")
            self.console.print(" " * 80, style="red on white")

            if sanity_check != 'yes':
                self.console.print("Replace aborted.")
                return {
                    "success": False,
                    "upsert_type": "REPLACE",
                    "detail": "USER_ABORT"
                }
        else:
            self.console.print(
                f"YOU ARE ABOUT TO REPLACE DATA STREAM '{unique_label}'  -- God bless you and good luck.",
                style="blink bold red underline")

        stream_meta = self.get_meta_data_by_label(unique_label)

        data_file = None
        # Check if file is used as data
        if isinstance(data_file_path, str):
            # print(data_file_path)
            assert self._is_gzipped_file(
                data_file_path), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
            data_file = open(data_file_path, "rb")
            # Set data to None to only use the file
            data = None

        # Datasetream does not exist create a new one
        if stream_meta is None or not isinstance(stream_meta, dict) or "_key" not in stream_meta:
            # Close file
            if data_file is not None:
                try:
                    data_file.close()
                except AttributeError:
                    pass
            raise Exception("DATA_STREAM_DOES_NOT_EXIST")
        else:
            if data_file is not None:
                # assert self._is_gzipped_file(data_file), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
                data_file = ("data.json.gz", data_file, "application/json")

            m = MultipartEncoder(
                fields={"key": stream_meta["_key"],
                        "inplace": "true" if inplace else "false",
                        "cascade": "true" if cascade else "false",
                        "data": json.dumps(data) if data is not None else "[]",
                        "data_file": data_file}
            )

            result = self.requests.post(
                f"{self.base_uri}/data-stream/replace", data=m, headers={'Content-Type': m.content_type},
                stream=False)  # files={"data_file": data_file}, data=update_definition

            # assert result.status_code == 200, "COULD_NOT_REPLACE_DATA_STREAM_STATUS_CODE_NE_200"
            self.evaluator.evaluate(response=result)
            result = result.json()

            # Close file
            if data_file is not None:
                try:
                    data_file.close()
                except AttributeError:
                    pass

            # Check if update was successfull
            if "detail" in result:
                for d in result["detail"]:
                    # Currently only check if data stream key exists
                    if d["type"] == 'data_warning.empty':
                        return {
                            "success": False,
                            "upsert_type": "REPLACE",
                            "detail": "DATA_STREAM_DOES_NOT_EXIST"
                        }
            assert "_key" in result, "ERROR_UPDATE"

            return {
                "success": True,
                "upsert_type": "REPLACE",
                "detail": result
            }

    def get_data(self, key: Union[str, int], fields: list = None, skip: int = None, limit: int = None,
                 live: bool = False, multithread: bool = True) -> list:
        """
        Retrieve data from the Fusionbase API
        :param key: The key of the datastream provided as an integer or string
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param skip: Pagination parameter to skip rows
        :param limit: Pagination parameter to limit rows
        :param live:
        :param multithread: Whether multithreading should be used or not (Default is True)
        :return: The data as a list of dictionaries
        """

        # Add Fusionbase columns by default to fields
        if isinstance(fields, list) and len(fields) > 0:
            fields.extend(["fb_id", "fb_data_version", "fb_datetime"])
            fields = list(dict.fromkeys(fields))  # Remove duplicates
        else:
            fields = None


        if isinstance(limit, int) and limit > 150000:
            self._log("[red]Limit can't exceed 150.000. Use multiple requests in batches instead![/red]", force=True)
            self._log("[red]Limit is forcefully set to 150.000[/red]", force=True)
            self._log("[yellow]Tip: If you want to get the whole dataset, leave skip and limit empty.[/yellow]", force=True)
            self._log("")
            limit = 150000

        self._log(f"Getting meta data for Datastream with key {key}")

        # First get only the meta data
        meta_data = self.get_meta_data(key=key)
        self._log(f'✅ Meta data successfully retrieved')
        self._log(
            f'Preparing data for stream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan]')

        # Make sure max batches is always at least 1
        max_batches = (os.cpu_count() - 1) if os.cpu_count() > 1 else 1

        self._log(
            f'We will use {max_batches if multithread else 1} cores/threads to retrieve the data.{str(" Multithreading is [bold red]OFF[/bold red]") if not multithread else str("")}')

        # meta_data["meta"]["entry_count"] is calculate on each /get request and should be safe to use
        if skip is not None and int(skip) > -1 and limit is not None and int(limit) > -1:
            skip_limit_batches = [(skip, limit)]
        
        # Only limit is set without skip
        elif skip is None and limit is not None:
            skip_limit_batches = [(0, limit)]
        
        # Only skip is set without limit
        elif skip is not None and (limit is None or limit == -1):

            if int(meta_data["meta"]["entry_count"])-skip < 1:
                raise Exception(
                    f"YOU CAN'T SKIP MORE ENTRIES THAN THE STREAM HAVE ROWS")                

            if meta_data is not None and "meta" in meta_data and "entry_count" in meta_data["meta"]:
                skip_limit_batches = self._calc_skip_limit_tuples(
                        limit=int(meta_data["meta"]["entry_count"]), max_batches=max_batches, initial_skip=skip)

        else:
            if meta_data is not None and "meta" in meta_data and "entry_count" in meta_data["meta"]:
                if int(meta_data["meta"]["entry_count"]) / max_batches < 500000:
                    skip_limit_batches = self._calc_skip_limit_tuples(
                        limit=int(meta_data["meta"]["entry_count"]), max_batches=max_batches)
                else:
                    skip_limit_batches = self._calc_skip_limit_tuples(limit=int(meta_data["meta"]["entry_count"]),
                                                                      max_batches=math.ceil(
                                                                          (int(meta_data["meta"][
                                                                              "entry_count"]) * max_batches) / 8000000))
            else:
                # Entry count metadata is missing
                raise Exception(
                    f"DATA STREAM WITH KEY {key} HAS NO ENTRY COUNTY.")

        self._log(
            f'The Datastream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan] has {meta_data["meta"]["entry_count"]} rows and {meta_data["meta"]["main_property_count"]} columns\n')

        _tmp_path_set = set()
        result_datastream_data = list()

        def __get_data_from_fusionbase(self, skip_limit, chunk):

            self._log(
                f'Getting the data form {str(skip_limit[0])} to {str(skip_limit[0] + skip_limit[1])} now -> This is part {str(chunk)}')

            # Build unique name based on input arguments
            if fields is None:
                _tmp_name = f"fb_stream__{str(key)}__{str(limit)}___{str(skip_limit[0])}__{str(skip_limit[0])}"
            else:
                _tmp_name = f"fb_stream__{str(key)}__{str(limit)}___{str(skip_limit[0])}__{str(skip_limit[0])}___{'-'.join(fields)}"

            _tmp_name = hashlib.sha3_256(_tmp_name.encode(
                "utf-8")).hexdigest()[:12] + '.json'
            _tmp_fpath = PurePath(self.tmp_dir, _tmp_name)
            _tmp_path_set.add(_tmp_fpath)

            if Path(f"{_tmp_fpath}.gz").is_file() and not live:
                self._log(f'Cache hit! Skip downloading part {str(chunk)}')
                return

            # Build request URL
            request_url = f"{self.base_uri}/data-stream/get/{key}?skip={skip_limit[0]}&limit={skip_limit[1]}&compressed_file=true"

            if fields is not None:
                field_query = ""
                for field in fields:
                    field_query = f"{field_query}&fields={urllib.parse.quote_plus(field)}"
                field_query = field_query.strip().strip('&').strip()
                request_url = f"{request_url}&{field_query}"

            r = self.requests.get(
                request_url,
                stream=True)  # logic! if the limit is increased, it loads n the first time, 2xn the second and so on

            total_download_size = int(r.headers.get('content-length', 0))

            # Store the file as gzip
            if self.log:
                with open(f"{_tmp_fpath}.gz", "wb") as fopen, tqdm(
                        desc=f"Downloading part {chunk}",
                        total=total_download_size,
                        unit='iB',
                        unit_scale=True,
                        unit_divisor=1024,
                ) as bar:
                    for data in r.iter_content(chunk_size=1024):
                        size = fopen.write(data)
                        bar.update(size)
                    fopen.close()
            else:
                with open(f"{_tmp_fpath}.gz", "wb") as fopen:
                    for data in r.iter_content(chunk_size=1024):
                        size = fopen.write(data)
                    fopen.close()

        # Parallelize download
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() - 1 if multithread else 1) as executor:
            data_part = {executor.submit(__get_data_from_fusionbase, self, skip_limit, chunk): (
                skip_limit, chunk) for chunk, skip_limit in enumerate(skip_limit_batches)}
            for future in concurrent.futures.as_completed(data_part):
                url = data_part[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                    print(traceback.format_exc())
                else:
                    pass

        self._log(f'\n✅ Data successfully retrieved.')
        self._log(f'Building the final result object...')

        # Load the downloaded files into a data frame
        for _tmp in _tmp_path_set:
            # Unpack gzip and put into df
            try:
                with gzip.open(f"{_tmp}.gz", 'r') as fin:
                    data = json.loads(fin.read().decode('utf-8'))["data"]
                    result_datastream_data.extend(data)
            except gzip.BadGzipFile as e:
                continue

        self._log(f'✅  Done.')

        return result_datastream_data


    def get_delta_data(self, key: Union[str, int], version: str, fields: list = None, live: bool = True) -> list:
        """
        Retrieve data from the Fusionbase API since a specified fb_data_version
        :param key: The key of the datastream provided as an integer or string
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param version: The  fb_data_version starting from from which new data should be downloaded
        :return: The data as a list of dictionaries
        """
        if fields is None:
            fields = []

        version = str(version)
        VERSION_PATTERN = r'^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$'

        version_matched = re.match(VERSION_PATTERN, version)

        if key is None or '':
            raise Exception('A VALID KEY MUST BE SPECIFIED!')

        if (version is None or '') or not bool(version_matched):
            raise Exception('A VALID VERSION MUST BE SPECIFIED!')

        # Add Fusionbase columns by default to fields
        fields.extend(["fb_id", "fb_data_version", "fb_datetime"])
        fields = list(dict.fromkeys(fields))  # Remove duplicates

        self._log(f"Getting meta data for Datastream with key {key}")

        # First get only the meta data
        meta_data = self.get_meta_data(key=key)
        self._log(f'✅ Meta data successfully retrieved')
        self._log(
            f'Preparing delta data for stream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan] since version: [bold]{version}[/bold]')

        self._log(
            f'The Datastream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan] has {meta_data["meta"]["entry_count"]} rows and {meta_data["meta"]["main_property_count"]} columns\n')

        self._log(
            f'Getting the delta data for stream [bold underline cyan]{meta_data["name"]["en"]}[/bold underline cyan] since version: [bold]{version}[/bold]')

        # Build unique name based on input arguments
        _tmp_name = f"fb_stream__delta__{version}___{'-'.join(fields)}"

        _tmp_name = hashlib.sha3_256(_tmp_name.encode(
            "utf-8")
        ).hexdigest()[:12] + '.json'

        _tmp_fpath = PurePath(self.tmp_dir, _tmp_name)

        if Path(f"{_tmp_fpath}.gz").is_file() and live == False:
            self._log(f'Cache hit! Skip downloading ... ')

        else:
            r = self.requests.get(
                f"{self.base_uri}/data-stream/get/delta/{key}/{version}", stream=True)

            self.evaluator.evaluate(response=r)

            if r.status_code == 401 and r.content == b'{"detail":[{"loc":"","msg":"You are not authorized to access this resource.","type":"authorization_error.missing"}]}':
                raise Exception(
                    f"IT SEEMS LIKE YOU DON'T HAVE ACCESS TO DATASTREAM WITH STREAM-ID {key}")

            if r.status_code != 200:
                raise Exception(
                    f"SOMETHING WEN'T WRONG WHILE REQUESTING DATASTREAM WITH STREAM-ID {key} and DATA AFTER VERSION: {version}, \n RESPONSE CODE WAS : {r.status_code}")

            total_download_size = int(r.headers.get('content-length', 0))

            # Store the file as gzip
            if self.log:
                with gzip.open(f"{_tmp_fpath}.gz", "wb") as fopen, tqdm(
                    desc=f"Downloading DELTA",
                    total=total_download_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for data in r.iter_content(chunk_size=1024):
                        size = fopen.write(data)
                        bar.update(size)
                    fopen.close()
            else:
                with gzip.open(f"{_tmp_fpath}.gz", "wb") as fopen:
                    for data in r.iter_content(chunk_size=1024):
                        size = fopen.write(data)
                    fopen.close()

        self._log(f'\n✅ Data successfully retrieved.')
        self._log(f'Building the final result object...')

        # Load the downloaded file into a data frame
        # Unpack gzip and put into df
        try:
            with gzip.open(f"{_tmp_fpath}.gz", 'r') as fin:
                result_datastream_data = json.loads(
                    fin.read().decode('utf-8'))["data"]

        except gzip.BadGzipFile as e:
            raise Exception(
                f"DATA STREAM WITH KEY {key} AND VERSION {version}: COMPRESSED FILE: {_tmp_fpath}.gz COULDN'T BE UNPACKED SEE: \n {e}")

        if len(result_datastream_data) == 0:
            self.console.print(
                f"NO NEW ENTRIES FOUND. EITHER YOUR VERSION IS UP TO DATE OR YOUR SPECIFIED VERSION: {version} DOESN'T EXIST. \n PLEASE RECHECK! \n (note the datastream was last updated at: { meta_data['updated_at']})", style="bold yellow")

        self._log(f'✅  Done')

        return result_datastream_data

    def get_delta_dataframe(self, key:Union[str, int], version: str, fields: list=None) -> pd.DataFrame:
        """
        Retrieve data from the Fusionbase API since a specified fb_data_version and return it as a valid pandas Dataframe
        :param key: The key of the datastream provided as an integer or string
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param version: The  fb_data_version starting from from which new data should be downloaded
        :return: The data as a pandas Dataframe
        """
        if fields is None:
            fields = []

        data = self.get_delta_data(key=key, version=version, fields=fields)

        df = pd.DataFrame(data)
        return df

    def get_dataframe(self, key: Union[str, int], fields: list = None, skip: int = None, limit: int = None,
                      live: bool = False,
                      multithread: bool = True) -> pd.DataFrame:
        """
        Retrieve data from the Fusionbase API and return it as a valid pandas Dataframe
        :param key: The key of the datastream provided as an integer or string
        :param fields: The fields or columns of the data you want to retrieve (Projection)
        :param skip: Pagination parameter to skip rows
        :param limit: Pagination parameter to limit rows
        :param live:
        :param multithread: Whether multithreading should be used or not (Default is True)
        :return: The data as a pandas Dataframe
        """
        if fields is None:
            fields = []

        data = self.get_data(key=key, fields=fields, skip=skip,
                             limit=limit, live=live, multithread=multithread)
        df = pd.DataFrame(data)
        #df.drop_duplicates(subset=["fb_id"], inplace=True)
        return df
