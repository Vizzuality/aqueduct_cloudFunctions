from multiprocessing import Pool, TimeoutError
import json
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import logging
import pickle
from flask import escape


with open("./privatekey.json") as keyfile:
    GEOPY = json.loads(keyfile.read())

ALLOWED_EXTENSIONS = set(['csv', 'xlsx'])

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

def serialize_response(data):

    return json.dumps({'rows': data.to_dict(orient='record')}, indent=2)

def serialize_error(message):
    return json.dumps({'error': message})

def read_functions(extension):
    dic = {
        'csv': pd.read_csv,
        'xlsx': pd.read_excel
    }

    return dic[extension]

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_google_results(address):
    """
    Get geocode results from Google Maps Geocoding API.
    
    Note, that in the case of multiple google geocode reuslts, this function returns details of the FIRST result.
    
    @param address: String address as accurate as possible. For Example "18 Grafton Street, Dublin, Ireland"
    @param api_key: String API key if present from google. 
                    If supplied, requests will use your allowance from the Google API. If not, you
                    will be limited to the free usage of 2500 requests per day.
    @param return_full_response: Boolean to indicate if you'd like to return the full response from google. This
                    is useful if you'd like additional location details for storage or parsing later.
    """
    # Set up your Geocoding url
    logging.info("[GOOGLE URL]: init")
    params = {
    "address":address,
    "key":GEOPY.get('AQUEDUCT_GOOGLE_PLACES_PRIVATE_KEY')
    }
    
    # Ping google for the reuslts:
    try:
        with requests.Session() as s:
            s.mount('https://',HTTPAdapter(max_retries=Retry(2,backoff_factor=0.001)))
            r = s.get(url=GEOCODE_URL, params=params, timeout=15)
        
        if r.status_code == requests.codes.ok:
            # Results will be in JSON format - convert to dict using requests functionality
            results = r.json()
            # if there's no results or an error, return empty results.
            if len(results['results']) == 0:
                output = {
                    "formatted_address" : None,
                    "latitude": None,
                    "longitude": None,
                    "matched": False
                }
            else:    
                answer = results['results'][0]
                output = {
                    "formatted_address" : answer.get('formatted_address'),
                    "latitude": answer.get('geometry').get('location').get('lat'),
                    "longitude": answer.get('geometry').get('location').get('lng'),
                    "matched":True
                }
        else:
            logging.error(f"[GEOCODER: Get google place]: {r.text}")
            logging.error(f"[GEOCODER- GOOGLE URL]: {r.status_code}")
            output = {
                "formatted_address" : None,
                "latitude": None,
                "longitude": None,
                "matched": False
            }
            
        # Append some other details:    
        output['input_string'] = address
        output['number_of_results'] = len(results['results'])
        output['status'] = results.get('status')
        
        return output
    except Exception as e:
        raise e

def get_latlonrow(x):
    index, row = x
    logging.info(f"{index}")
    if pd.notna(row['address']) or (row['address'] in ('', ' ')):
        address = get_google_results(row['address'])
        return address["formatted_address"], address["latitude"], address["longitude"], address["matched"], address["status"]
    else:
        return None, None, None, False, None

def geocoding(data):
    try:
        data.columns = map(str.lower, data.columns)
        #logging.debug(f'[GeoCode Service] Geo-encoding columns: {data.columns}')
        if 'address' in data.columns:
            logging.debug(f'[GeoCode Service] "address" present in "data.columns":')
            data1 = pd.DataFrame(0.0, index=list(range(0, len(data))), columns=list(['matched address', 'lat', 'lon', 'match', 'geocode_status']))
            data = pd.concat([data, data1], axis=1)
            with Pool(processes=8) as p:
                logging.info(f'[GeoCode Service] geocoding init:')
                #output = p.map_async(get_latlonrow, data.iterrows())
                #output.wait()
                #data[['matched address', 'lat', 'lon', 'match', 'geocode_status']] = output.get()
                data[['matched address', 'lat', 'lon', 'match', 'geocode_status']] = p.map(get_latlonrow, data.iterrows())
                logging.info(f'[GeoCode Service] geocoding end')
                #data.fillna(value=None, inplace=True)
            
        else:
            return (serialize_error(f'Address column missing'), 500, headers)
    except Exception as e:
        raise e
    return data

def geocoder(request):
    # For more information about CORS and CORS preflight requests, see
    # https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request
    # for more information.
    # Set CORS headers for the main request
    request.get_json()
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json'
    }
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers.update({
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        })
        return ('', 204, headers)
    logging.info(f'[GeoCode Service] init')

    try:
       if request.method == 'POST':
           logging.info(f'[GeoCode Service]: File keys detected: {list(request.files.keys())}')
           if 'file' not in request.files:
                return (serialize_error(f'No file provided'), 500, headers)

           file = request.files['file']

           extension = file.filename.rsplit('.', 1)[1].lower()

           if file and allowed_file(file.filename):
               data = read_functions(extension)(request.files.get('file'))
               logging.info(f'[GeoCode Service] Data loaded: {data.columns}')
               data.rename(columns={'Unnamed: 0': 'row'}, inplace=True)
               data.dropna(axis=1, how='all', inplace=True)
               logging.info(f'[GeoCode Service] Data loaded; columns cleaned: {data.columns}')

               if not {'row', 'Row'}.issubset(data.columns):
                   data.insert(loc=0, column='row', value=range(1, 1 + len(data)))

               if len(data) == 0:
                   return (serialize_error(f'The file is empty'), 500, headers)
                   
               if len(data) > 1000:
                   return (serialize_error(f'Row number should be less or equal to 1000'), 500, headers)
           
               return (serialize_response(geocoding(data)), 200, headers)
           
           else:
               return (serialize_error(f'{extension} is not an allowed file extension'), 500, headers)       
   
    except Exception as e:
       return (serialize_error(f'{e}'), 500, headers)
