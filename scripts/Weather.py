####################################################################################
# Weather.py
# @ Author Ryan Herrin
#
# Program to obtain hourly weather data per day and transform it into a CSV file 
# to later be entered into a database. 
#
# Will be written as a class so other programs can call it too. 
####################################################################################
'''
The intention is to have a website that has the past hourly data of a certain city
and download the data. The national weather government site has an API where you 
can download weather data in a JSON format. 
'''

import os
import requests
import datetime


# Use logging function
enable_screen_log = True
# Define Global locations 
global_weather_write_loc = '/projects/ADSB-Flight-Freq-Tracker/data/adsb_processed_data/'

# Determine full path based on if the system is Windows or running on the Linux(pi)
if os.name == "nt":
	# If Windows
	win_prefix = "Z:"
	global_weather_write_loc = win_prefix + global_weather_write_loc 
else:
	# If Linux(Pi)
	pi_prefix = "/home/pi/Documents"
	global_weather_write_loc = pi_prefix + global_weather_write_loc


class Weather:
	"""Class to retrieve weather data from the National Weather Website.
	With the option to parse the data and write to a csv file that can be exported 
	to a database. 
	
	API Notes:
		- Stations are 4 letters and can be found on the weather.gov site
		- All date and times use ISO-8601 format.
		- Outputs Zulu time 
		- Military time 
	"""
	def __init__(self):
		#self.weather_url = weather_url
		self.csv_write_loc = global_weather_write_loc
		self.api_root_url = "https://api.weather.gov/"

	def _weather_log(self, print_data):
		""" Custom logger """
		if enable_screen_log:
			print_data = str(print_data) #Strong Typecast to string 
			print("[Weather] > {}".format(print_data))
		else:
			pass
	
	def _generate_api_call(self, station, date=None, start_date=None, end_date=None,
						   limit=None):
		"""Generates an API call using the provided data and the url.
		This generator exclusively uses the [GET] /stations/{stationId/observations} web
		service from the weather.gov API documentation.
		"""
		if date != None and limit == 1:
			return(self.api_root_url + 
				   "stations/{}".format(station) + 
				   "/observations?start={}T00%3A00%3A00-06%3A00&".format(date) +
				   "limit=1")
		
	def _execute_api_call(self, api_url):
		"""Internal Function that runs the API call and returns the JASON information
		if the request was successfull"""
		# Print out API url for informational purposes 
		self._weather_log("API Call: {}".format(api_url))
		self._weather_log("Attempting to retrieve data...")
		
		# Attempt to get the data
		try:
			call_data = requests.get(api_url)
			
			# Check to see if the data transfer was successfull. This doesn't mean
			# the data received doesn't have an error, but getting a response was good.
			if call_data.status_code == requests.codes.ok:
				self._weather_log("Success...")
				
			# Raise exception if not successfull
			call_data.raise_for_status()
			
			return(call_data.text)
				
		except Exception as err:
			self._weather_log(str(err))
	
	
	def get_current_weather(self, station):
		"""Returns the current weather for the closest posted hour. 
		@Parameters:
			- station : str
				Name of the station to get the weather from.
		"""
		# Generate current day and format it 
		curr_date = datetime.datetime.now().strftime("%Y-%m-%d")
		# Get API URL
		api_call_url = self._generate_api_call(station, date=curr_date, limit=1)
		
		return(self._execute_api_call(api_call_url))
		
	
	def get_daily_weather(self, station, date):
		"""Returns the weather for the specified date at the station.
		@Parameters:
			- station : str
				Name of the station to get the weather from. 
			- date : str
				A specific day formatted YYYY-MM-DD
		"""
		pass
	
	def _write_raw_to_file(self):
		"""This function is ONLY for debugging. 
		It writes the captured data to a local JSON file. This is so we can create 
		methods to parse the JSON data either in a way to make it work or to make 
		it more efficient.
		"""
		pass


if __name__ == "__main__":
	curr_weather = Weather().get_current_weather("KFTW")





















