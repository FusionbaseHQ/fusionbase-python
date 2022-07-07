# Import Fusionbase
from fusionbase import Fusionbase
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})


data_service_key = "23622632"
data_service = fusionbase.get_dataservice(key=data_service_key)

# Get data since a specific version as pandas dataframe
payload = [
    {
        "name": "address_string", # THIS IS THE NAME OF THE INPUT VALUE
        "value": "Agnes-Pockels-Bogen 1, 80992 München" # THE VALUE FOR THE INPUT
    }
]

result = data_service.invoke(parameters=payload)
print(result)


# OR, the following is equivalent
result = data_service.invoke(address_string="Agnes-Pockels-Bogen 1, 80992 München")
print(result)