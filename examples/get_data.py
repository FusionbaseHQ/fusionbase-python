# Import Fusionbase
from fusionbase.DataStream import DataStream

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
data_stream = DataStream(auth={"api_key": "*** SECRET CREDENTIALS ***"},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

# Put your data stream key here
data_stream_key = 28654971
data = data_stream.get_data(key=data_stream_key)

print(data)