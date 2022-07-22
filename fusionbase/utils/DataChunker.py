from pathlib import Path
from pathlib import PurePath
import platform
import tempfile
import gzip
import glob
import os
from concurrent.futures import ThreadPoolExecutor

try:
    import orjson as json
except ImportError as e:
    import json
try:
    import numpy as np
except ImportError as e:
    np = None
try:
    import pandas as pd
except ImportError as e:
    pd = None


class DataChunker:
    def __init__(self, config: dict) -> None:
        if "cache_dir" in config:
            self.tmp_dir = PurePath(Path(config["cache_dir"]))
        else:
            self.tmp_dir = PurePath(
                Path(
                    "/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()
                ),
                "fusionbase",
            )

        # Ensures that tmp/cache directory exists
        self.tmp_dir = Path(self.tmp_dir).joinpath("update_create")
        Path(self.tmp_dir).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _is_gzipped_file(file_obj) -> bool:
        """
        Checks if `file_obj` is a gezipped file
        :param file_obj: Path of the file that should be checked if it is gzipped
        """
        with gzip.open(file_obj, "r") as fh:
            try:
                fh.read(1)
                fh.close()
                return True
            except gzip.BadGzipFile:
                return False

    def _load_gzip_json(self, file_path):
        """
        Load a gzipped json file into a Python object
        :param file_obj: Path of the file that should be checked if it is gzipped
        """
        with gzip.open(file_path, "rt", encoding="UTF-8") as zipfile:
            data = json.load(zipfile)
        return data

    def _store_data_chunks(self, data: list, chunk_size: int, common_file_key: str):
        """
        Chunks a Python list into chunks of size `chunk_size` and returns a list of the file paths
        :param data: Python list of dictionaries that represent the dataset which will be splitted
        :param chunk_size: Size of the individual chunks
        :param common_file_key: A common file key that will be added to the individual chunk file names
        """
        if np is None:
            raise ModuleNotFoundError("You must install numpy to use this feature.")

        data_chunk_file_paths = list()

        # Chunk size can't be larger than the number of data items
        if chunk_size > len(data):
            chunk_size = len(data)

        # Chunk memory loaded data
        data_chunks = np.array_split(data, chunk_size)

        # Build files for upload
        for chunk_index, data_chunk in enumerate(data_chunks):
            chunk_file_path = str(
                PurePath(
                    self.tmp_dir,
                    f"__tmp_df_upload_{chunk_index}__{common_file_key}.json.gz",
                )
            )

            data_chunk = list(data_chunk)
            with gzip.open(chunk_file_path, "wt", encoding="utf-8") as zipfile:
                json_str = json.dumps(data_chunk)
                try:
                    json_str = json_str.decode("utf-8")
                except (UnicodeDecodeError, AttributeError):
                    pass
                zipfile.write(json_str)

            data_chunk_file_paths.append(chunk_file_path)

        return data_chunk_file_paths

    def chunk_file(self, data_file_path, chunk_size: int, common_file_key: str):
        """
        Chunks a gzipped json file into chunks of size `chunk_size` and returns a list of the file paths
        :param data_file_path: Gzipped json files that represent the dataset which will be splitted
        :param chunk_size: Size of the individual chunks
        :param common_file_key: A common file key that will be added to the individual chunk file names
        """

        assert self._is_gzipped_file(
            data_file_path
        ), "ONLY_GZIPPED_DATA_FILES_ARE_SUPPORTED"

        # Load data into memory
        data = self._load_gzip_json(file_path=data_file_path)

        return self._store_data_chunks(
            data=data, chunk_size=chunk_size, common_file_key=common_file_key
        )

    def chunk_dataframe(self, df, chunk_size: int, common_file_key: str):
        """
        Chunks a DataFrame into chunks of size `chunk_size` and returns a list of the file paths
        :param df: DataFrame that represent the dataset which will be splitted
        :param chunk_size: Size of the individual chunks
        :param common_file_key: A common file key that will be added to the individual chunk file names
        """
        if pd is None:
            raise ModuleNotFoundError("You must install pandas to use this feature.")

        data = df.to_dict(orient="records")
        return self._store_data_chunks(
            data=data, chunk_size=chunk_size, common_file_key=common_file_key
        )

    def chunk_list(self, data: list, chunk_size: int, common_file_key: str):
        """
        Chunks a Python list into chunks of size `chunk_size` and returns a list of the file paths
        :param data: Python list of dictionaries that represent the dataset which will be splitted
        :param chunk_size: Size of the individual chunks
        :param common_file_key: A common file key that will be added to the individual chunk file names
        """
        return self._store_data_chunks(
            data=data, chunk_size=chunk_size, common_file_key=common_file_key
        )

    def load_chunks(self, common_file_key: str, as_dataframe=False):
        """
        Loads a chunked dataset from disk        
        :param common_file_key: A common file key that will be added to the individual chunk file names
        :param as_dataframe: Flag of the data should be returned as DataFrame or Python list
        """

        chunk_files = glob.glob(
            str(self.tmp_dir) + os.sep + f"__tmp_df_upload_*__{common_file_key}.json.gz"
        )

        # Chunks do not exists
        if len(chunk_files) == 0:
            return None

        data = list()

        workers = os.cpu_count() - 1
        if workers < 0:
            workers = 1

        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = executor.map(self._load_gzip_json, chunk_files)

        for r in results:
            data.extend(r)

        if as_dataframe:
            if pd is None:
                raise ModuleNotFoundError(
                    "You must install pandas to use this feature."
                )
            return pd.DataFrame(data)

        return data
