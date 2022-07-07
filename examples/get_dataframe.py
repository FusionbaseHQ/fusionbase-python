# Import Fusionbase
from fusionbase import Fusionbase
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
# log=True enables live logging
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"}, log=True)


data_stream_key = "28654971"
data_stream = fusionbase.get_datastream(key=data_stream_key)

# Get the whole dataset as pandas dataframe
# multithread = False only leverages one core to retrieve the dataset | True is the default value
# live = True ignores the local cache | False is the default value
df = data_stream.get_dataframe(multithread=False, live=True)
print(df)


# Get only specific columns from the dataset
df = data_stream.get_dataframe(fields=["country_iso_alpha3", "geo"])
print(df.columns)


# Get only a specific window from the dataset
# The following sample skips the first 10 rows (skip) and then selects the following 10 (limit)
df = data_stream.get_dataframe(fields=["country_iso_alpha3", "geo"], skip=10, limit=10)
print(df)