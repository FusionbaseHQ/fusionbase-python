# Import Fusionbase
from fusionbase import Fusionbase
from pathlib import Path
from fusionbase.constants.ResultType import ResultType
import os


# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"}, log=True)


data_stream_key = "28654971"
data_stream = fusionbase.get_datastream(key=data_stream_key)

# Retrieve the whole datastream as partitioned JSON files
# Both method calls are equivalent
data_stream.get_data(result_type=ResultType.JSON_FILES, storage_path=Path("./data/"))
data_stream.as_json_files(storage_path=Path("./data/"))

# Retrieve the whole datastream as partitioned CSV files
# Both method calls are equivalent
data_stream.get_data(result_type=ResultType.CSV_FILES, storage_path=Path("./data/"))
data_stream.as_csv_files(storage_path=Path("./data/"))

# Retrieve the whole datastream as partitioned Pickle files
# Both method calls are equivalent
data_stream.get_data(result_type=ResultType.PICKLE_FILES, storage_path=Path("./data/"))
data_stream.as_pickle_files(storage_path=Path("./data/"))

# Retrieve the whole datastream as dataframe
# Both method calls are equivalent
df = data_stream.get_data(result_type=ResultType.PD_DATAFRAME)
df = data_stream.as_dataframe()