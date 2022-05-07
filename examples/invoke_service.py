# Import Fusionbase
from fusionbase.DataService import DataService

# Create a new dataservice
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
data_service = DataService(auth={"api_key": "*** SECRET CREDENTIALS ***"},
                      connection={"base_uri": "https://api.fusionbase.com/api/v1"})


data_service_key = 23622632

# Retrieves the request definition (such as required parameters) from a Service by giving a Service specific key and prints it nicely to console
payload = [
    {
        "name": "address_string", # THIS IS THE NAME OF THE INPUT VALUE
        "value": "Agnes-Pockels-Bogen 1, 80992 München" # THE VALUE FOR THE INPUT
    }
]

result = data_service.invoke(key=data_service_key, parameters=payload)
print(result)