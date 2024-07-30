import requests
import pandas as pd

def get_long_lat(postcode_list):
    url = "https://api.postcodes.io/postcodes"
    params = {
    	"postcodes": postcode_list
    }
    response = requests.post(url, json = params)
    return response.json()

# Then we need to format it so we only get the bits we're interested in.  
def extract_long_lat(data):
    extracted_data = []
    for item in data['result']:
        if 'result' in item and item['result']:
            extracted_data.append({
                'postcode': item['result'].get('postcode'),
                'longitude': item['result'].get('longitude'),
                'latitude': item['result'].get('latitude')
            })
    return pd.DataFrame(extracted_data)