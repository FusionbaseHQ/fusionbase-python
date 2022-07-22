from enum import Enum


class ResultType(Enum):
    PYTHON_LIST = 0
    PD_DATAFRAME = 1
    JSON_FILES = 2
    PICKLE_FILES = 3
    FEATHER_FILES = 4
    PARQUET_FILES = 5
    CSV_FILES = 6
