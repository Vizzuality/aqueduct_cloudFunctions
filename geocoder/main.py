from multiprocessing import Pool, TimeoutError

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd

import json
import logging


with open("./privatekey.json") as keyfile:
	GEOPY = json.loads(keyfile.read())

ALLOWED_EXTENSIONS = set(['csv', 'xlsx'])

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

def serialize_response(data):
	return json.dumps({'rows': data}, indent=2, ensure_ascii=False)

def serialize_error(message):
	return json.dumps({'error': message})

def read_functions(extension):
	dic = {
		'csv': pd.read_csv,
		'xlsx': pd.read_excel
	}
	return dic[extension]

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
			s.mount('https://',HTTPAdapter(max_retries=Retry(2, backoff_factor=0.001)))
			r = s.get(url=GEOCODE_URL, params=params, timeout=15)
		
		if r.status_code == requests.codes.ok:
			# Results will be in JSON format - convert to dict using requests functionality
			results = r.json()
			# if there's no results or an error, return empty results.
			if len(results['results']) == 0:
				output = {
					"matched_address" : None,
					"lat": None,
					"lon": None,
					"match": False
				}
			else:    
				answer = results['results'][0]
				output = {
					"matched_address" : answer.get('formatted_address'),
					"lat": answer.get('geometry').get('location').get('lat'),
					"lon": answer.get('geometry').get('location').get('lng'),
					"match":True
				}
		else:
			logging.error(f"[GEOCODER: Get google place]: {r.text}")
			logging.error(f"[GEOCODER- GOOGLE URL]: {r.status_code}")
			output = {
				"matched_address" : None,
				"lat": None,
				"lon": None,
				"match": False
			}
			
		# Append some other details:    
		output['address'] = address
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
		address["row"] = row['row']
		return address
	else:
		return  {
				"matched_address" : None,
				"lat": None,
				"lon": None,
				"match": False,
				"address": None,
				"number_of_results": None,
				"status":"Address value not available",
				"row": row['row']
			}

def geocoding(data):
	try:
		logging.info(f'[GeoCode Service] geocoding init:')
		
		with Pool(processes=16) as p:
			output = p.map_async(get_latlonrow, data.iterrows())
			output.wait()
		
		logging.info('[GeoCode Service] geocoding end')

		return output.get()			
	except Exception as e:
		raise e

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

	try:
		logging.info(f'[GeoCode Service] init')
		if request.method == 'POST':

			if 'file' not in request.files:
				return (serialize_error(f'No file provided'), 500, headers)

			
			file = request.files['file']
			extension = file.filename.rsplit('.')[-1].lower()

			if extension in ALLOWED_EXTENSIONS:
				data = read_functions(extension)(request.files.get('file'))
				data.columns = data.columns.str.lower()
				
				logging.info(f'[GeoCode Service] Data loaded: {data.columns}')

				data.rename(columns={'Unnamed: 0': 'row'}, inplace=True)
				
				if 'row' not in data.columns:
					data.insert(loc=0, column='row', value=range(1, 1 + len(data)))
					
				
				data.dropna(axis=1, how='all', inplace=True)
				
				#data.fillna(value=None, method=None, inplace=True)

				logging.info(f'[GeoCode Service] Data loaded; columns cleaned: {data.columns}')
				if 'location_name' not in data.columns:
					data.insert(loc=0, column='location_name', value=range(1, 1 + len(data)))

				logging.info(f'[GeoCode Service] Data loaded; columns cleaned: {data.columns}')

				if 'address' not in data.columns:
					return (serialize_error(f'Address column missing'), 500, headers)

				if len(data) == 0:
					return (serialize_error(f'The file is empty'), 500, headers)

				if len(data) > 1000:
					return (serialize_error(f'Row number should be less or equal to 1000'), 500, headers)

				
				return (serialize_response(geocoding(data)), 200, headers)

			else:
				return (serialize_error(f'{extension} is not an allowed file extension'), 500, headers)
		else:
			return (serialize_error(f'request method ({request.method}) not allowed'), 500, headers)       

	except Exception as e:
		return (serialize_error(f'{e}'), 500, headers)
