# Import Fusionbase
from fusionbase import Fusionbase
import pandas as pd
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})

unique_label = 'MUNICH_OKTOBERFEST_BEER_CONSUMPTION'
data_stream_editable = fusionbase.get_datastream(label=unique_label)

# Load the data for which you want to create a datastream
# Currenty, only lists of dictionaries are supported, i.e., orient="records"
update_beer_consum_dataset = pd.read_csv(
 "data/oktoberfest_beer.csv").to_dict(orient="records")

# Update the data.
# It will automatically deduplicate the data and only add new entries.
result = data_stream_editable.update(data=update_beer_consum_dataset)