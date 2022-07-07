# Import Fusionbase
from fusionbase import Fusionbase
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})


data_stream_key = "28654971"
data_stream = fusionbase.get_datastream(key=data_stream_key)

# Get data since a specific version
version = '76d17547-cac6-4aaf-be16-bda597d3496f'
data = data_stream.get_delta_data(version=version)
print(data)