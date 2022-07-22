# Import Fusionbase
from fusionbase import Fusionbase
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})


data_stream_key = "28654971"
data_stream = fusionbase.get_datastream(key=data_stream_key)

# Print a nice table containing the meta data of the stream
data_stream.pretty_meta_data()