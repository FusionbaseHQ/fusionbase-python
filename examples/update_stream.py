# Import Fusionbase
from fusionbase.DataStream import DataStream

# Import pandas
import pandas as pd

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
data_stream = DataStream(auth={"api_key": "*** SECRET CREDENTIALS ***"},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

unique_label = 'MUNICH_OKTOBERFEST_BEER_CONSUMPTION'

# Get the datastream by it's unique label
data_stream_meta = data_stream.get_meta_data_by_label(label=unique_label)

# Load the data for which you want to create a datastream
# Currenty, only lists of dictionaries are supported, i.e., orient="records"
update_beer_consum_dataset = pd.read_csv(
 "data/oktoberfest_beer.csv").to_dict(orient="records")

# Update the data.
# It will automatically deduplicate the data and only add new entries.
result = data_stream.update(key=data_stream_meta["_key"], data=update_beer_consum_dataset)

# On success, this will return the id and the key of the newly created data stream
print(result)