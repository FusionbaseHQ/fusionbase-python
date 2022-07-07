# Import Fusionbase
from fusionbase import Fusionbase
import pandas as pd
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

# Load the data for which you want to create a datastream
# Currenty, only lists of dictionaries are supported, i.e., orient="records"
beer_consum_dataset_df = pd.read_csv(
 "data/oktoberfest_beer.csv")

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


# Create a new data stream
data_stream = fusionbase.create_stream(unique_label=unique_label, name={"en": "Munich Oktoberfest Beer Consumption"}, description={
                         "en": "This is a cool description"}, scope="PUBLIC", source=source, data=beer_consum_dataset_df)

# Print new data stream instance
print(data_stream)