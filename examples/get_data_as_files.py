# Import Fusionbase
# TODO: REMOVE, ONLY FOR DEV
import sys
sys.path.append("../")


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

# Get the whole dataset JSON files
data = data_stream.get_data(result_type=ResultType.JSON_FILES, storage_path=Path("./data/"))

print(data)