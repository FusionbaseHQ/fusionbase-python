# Import Fusionbase
from fusionbase.DataStream import DataStream

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
data_stream = DataStream(auth={"api_key": "*** SECRET CREDENTIALS ***"},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

data_stream_key = 28654971
version = '76d17547-cac6-4aaf-be16-bda597d3496f'

# This will pull all the data from the fusionbase datastream with key: '28654971' since fb_data_version: '76d17547-cac6-4aaf-be16-bda597d3496f'
data = data_stream.get_delta_data(key=data_stream_key, version=version)
print(data)