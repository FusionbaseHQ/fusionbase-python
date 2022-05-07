# Import Fusionbase
from fusionbase.DataStream import DataStream

#Import pandas
import pandas as pd

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
data_stream = DataStream(auth={"api_key": "*** SECRET CREDENTIALS ***"},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

# Load the data for which you want to create a datastream
# Currenty, only lists of dictionaries are supported, i.e., orient="records"
beer_consum_dataset = pd.read_csv(
 "data/oktoberfest_beer.csv").to_dict(orient="records")

# Unique label for the dataset
unique_label = 'MUNICH_OKTOBERFEST_BEER_CONSUMPTION'

# The source must exist!
# Use data_source.get_all() to list them all
source ={
 "_id": "data_sources/1007428",
 "stream_specific": {
   "uri": "https://www.opengov-muenchen.de/dataset/oktoberfest"
 }
}

# Creates the dataset
# Data entries are deduplicated by default
result = data_stream.update_create(unique_label=unique_label, name={"en": "Munich Oktoberfest Beer Consumption"}, description={
                         "en": "This is a cool description"}, scope="PUBLIC", source=source, data=beer_consum_dataset)

# On success, this will return the id and the key of the newly created data stream
print(result)