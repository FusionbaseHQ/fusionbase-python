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
 "data/oktoberfest_beer__NEW.csv").to_dict(orient="records")

#
# WARNING! DANGEROUS OPERATION
#
# Replace the whole datastream data
# cascade will delete all attribute definitions and re-create them based on the new data if set to "True", otherwise it will just add the new ones if there are any.
# inplace will wipe all data and replace it with the new dataset. If it is set to false, the old version will still be available and the replace will basically act as a base dataset update.
result = data_stream.replace(key=data_stream_meta["_key"], cascade=True, inplace=True, data=update_beer_consum_dataset)

# On success, this will return the id and the key of the newly created data stream
print(result)