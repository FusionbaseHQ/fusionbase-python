# Import Fusionbase
from fusionbase import Fusionbase
import os
import pandas as pd

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})


unique_label = 'MUNICH_OKTOBERFEST_BEER_CONSUMPTION'
data_stream_editable = fusionbase.get_datastream(label=unique_label)

# Load the data for which you want to create a datastream
# Currenty, only lists of dictionaries are supported, i.e., orient="records"
update_beer_consum_dataset = pd.read_csv(
 "data/oktoberfest_beer__NEW.csv").to_dict(orient="records")

# Replace the data of the given data stream
# This is only for some data streams with special user privileges possible
data_stream_editable.replace(data=update_beer_consum_dataset)