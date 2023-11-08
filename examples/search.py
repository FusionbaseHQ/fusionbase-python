# Import Fusionbase
from fusionbase import Fusionbase
import pandas as pd
import os

# Create a new datastream
# Provide your API Key and the Fusionbase API URI (usually: https://api.fusionbase.com/api/v1)
fusionbase = Fusionbase(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                      connection={"base_uri": "http://localhost:8000/api/v2"})



if __name__ == '__main__':
    from rich import print

    location = fusionbase.get_location("Würzburg")    
    #print(location)
    answer = location.answer("Anzahl der Einwohner")
    print(answer)
