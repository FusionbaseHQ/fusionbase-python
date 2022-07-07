# Import Fusionbase
from fusionbase import Fusionbase
import pandas as pd
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"}, log=True)

data_service_key = "23622632"
data_service = fusionbase.get_dataservice(key=data_service_key)


data = [{"address": "Agnes-Pockels-Bogen 1, 80992 München"}, {"address": "Maasweg 5, 80805 München"}]
df = pd.DataFrame(data)

# The callback always requires at least 2 parameter inputs:
# The first on is the series/row of the dataframe
# The second one is the API response from Fusionbase
def callback(series, api_result):
    series["house_number"] = api_result[0]["house_number"]
    series["city"] = api_result[0]["city"]
    return series

enriched_df = data_service.apply(df, [("address_string", "address")], callback=callback)