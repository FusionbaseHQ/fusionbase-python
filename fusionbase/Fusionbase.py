from __future__ import annotations

import gzip
import json
import math
import platform
import tempfile
import time
from pathlib import Path, PurePath
from typing import IO, Union
import requests
from requests_toolbelt import MultipartEncoder
from rich.console import Console

try:
    import numpy as np
except ImportError as e:
    np = None

try:
    import pandas as pd
except ImportError as e:
    pd = None


from fusionbase.DataService import DataService
from fusionbase.DataStream import DataStream
from fusionbase.exceptions.DataStreamNotExistsError import \
    DataStreamNotExistsError
from fusionbase.exceptions.ResponseEvaluator import ResponseEvaluator


class Fusionbase:

    def __init__(self, auth: dict, connection: dict = {"base_uri": "https://api.fusionbase.com/api/v1"}, log: bool = False, config: dict = None) -> None:
        """
        Used to initialise a new Fusionbase Object to further access streams and services
        :param key: The key of the service either as a string or integer value
        :param auth: the standard authentication object to authenticate yourself towards the fusionbase API
        Example:
        auth = {"api_key": " ***** Hidden credentials *****"}

        :param connection: the standard authentication object used to verify e.g which uri should be used
        Example:
        connection={"base_uri": "https://api.fusionbase.com/api/v1"}

        :param log: Whether the the output of any given operation should be logged to console
        """

        if config is None:
            config = {}

        self.auth = auth
        self.config = config
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.__log = log
        self.requests = requests.Session()
        self.requests.headers.update({'x-api-key': self.auth["api_key"]})
        self.__log = log
        self.console = Console()
        self.evaluator = ResponseEvaluator()

        if "cache_dir" in config:
            self.tmp_dir = PurePath(Path(config["cache_dir"]))
        else:
            self.tmp_dir = PurePath(Path(
                "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()), 'fusionbase')
        # Ensure that tmp/cache directory exists
        Path(self.tmp_dir).mkdir(parents=True, exist_ok=True)

    @property
    def log(self):
        return self.__log

    @log.setter
    def log(self, log: bool):
        self.__log = log

    @staticmethod
    def _is_gzipped_file(file_obj) -> bool:
        with gzip.open(file_obj, 'r') as fh:
            try:
                fh.read(1)
                fh.close()
                return True
            except gzip.BadGzipFile:
                return False

    def _log(self, message: str, force=False) -> None:
        if not self.log and not force:
            return None
        else:
            self.console.log(message)

    def get_datastream(self, key: Union[str, int] = None, label: str = None) -> DataStream:
        """Factory Method used to create a DataStream object

        Args:
            key (Union[str, int]): the key of the datastream

        Returns:
            DataStream: Returns an instance of the requested datastream by key
        """
        return DataStream(key=key, label=label, auth=self.auth, connection=self.connection, log=self.log, config=self.config)

    def get_dataservice(self, key: Union[str, int], cache:bool=False) -> DataService:
        """Factory Method used to create a DataService object

        Args:
            key (Union[str, int]): the key of the dataservice
            cache (bool): whether caching should be enabled or not

        Returns:
            DataService: Returns an instance of the requested dataservice by key
        """
        return DataService(key=key, auth=self.auth, connection=self.connection, log=self.log, cache=cache)

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

    def create_stream(self, unique_label: str, name, description, scope, source, data: list[dict] = None,
                      data_file_path: IO = None,
                      provision: str = "MARKETPLACE",
                      chunk: bool = False,
                      chunk_size: int = None) -> DataStream:
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

        if pd is None:
            raise ModuleNotFoundError('You must install pandas to use this feature.')

        if np is None:
            raise ModuleNotFoundError('You must install numpy to use this feature.')

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
            # data_stream = DataStream(auth=self.auth, label=unique_label, connection=self.connection, config=self.config, log=self.log)
            # stream_meta = data_stream._get_meta_data_by_label()
            # # stream_meta = self._get_meta_data_by_label(unique_label)

            if isinstance(data_chunk_file, str):
            # print(data_file_path)
                assert self._is_gzipped_file(
                    data_chunk_file), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"
                data_file = open(data_chunk_file, "rb")
                # Set data to None to only use the file
                data = None

                result = self._create(unique_label, name,
                                      description, scope, source, data, data_file, provision)
                upsert_type = "CREATE"
                result["upsert_type"] = upsert_type

                self._log(
                    f"Push chunk {data_chunk_file_index + 1} of {1 if chunk_size is None else chunk_size} chunks.")

                if not result["success"]:
                    self._log(
                        f"[red]ERROR: CHUNK {data_chunk_file_index} FAILED -- REST IS STILL GOING.[/red]")

        self._log(f"All chunks done.")
        self._log(f"Execution time :: {time.time() - start_time}")
        return DataStream(auth=self.auth, key=result['detail']['_key'],label=unique_label, connection=self.connection, config=self.config, log=self.log)

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

        if pd is None:
            raise ModuleNotFoundError('You must install pandas to use this feature.')

        if np is None:
            raise ModuleNotFoundError('You must install numpy to use this feature.')

        try:
            data_stream = self.get_datastream(label=unique_label)
            data_stream.update(unique_label=unique_label,
                               data=data, data_file_path=data_file_path)
        except DataStreamNotExistsError:
            data_stream = self.create_stream(unique_label=unique_label, name=name, description=description, scope=scope, data=data,
                                             source=source, data_file_path=data_file_path, provision=provision, chunk=chunk, chunk_size=chunk_size)

        return data_stream
