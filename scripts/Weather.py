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
import csv
import json
import calendar
import requests
import datetime
from datetime import date


# Use logging function
enable_screen_log = True
# Define Global locations
global_weather_write_loc = '/projects/ADSB-Flight-Freq-Tracker/data/weather_data/'

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

	def _generate_api_call(self, station, req_date=None, start_date=None, end_date=None,
						   limit=None, fun_call=None):
		"""Generates an API call using the provided data and the url.
		This generator exclusively uses the [GET] /stations/{stationId/observations} web
		service from the weather.gov API documentation.
		"""
		# Current Weather by latest post
		if fun_call == 1:
			return(self.api_root_url +
				   "stations/{}".format(station) +
				   "/observations?start={}T00%3A00%3A00-06%3A00&".format(req_date) +
				   "limit=1")

		# Get the hourly weather data by chosen day. If the day is the current day then
		# the api will only return up to the latest data
		if fun_call == 2:
			return(self.api_root_url +
				  "stations/{}".format(station) +
				  "/observations?start={}".format(req_date) +
				  "T00%3A00%3A00-05%3A00&" +
				  "end={}T23%3A59%3A00-05%3A00".format(req_date)
				  )

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

	def tranform_json_to_list(self, json_wthr_data, req_date=None):
		"""Takes the JSON data that the API generates and returns the list version
		of the data returning only the data we want

		# Each list is structured as follows:
			# [0] Date YYYY-MM-DD
			# [1] Time "In 24 Hr and to the nearest hour", Chicago -05:00
			# [2] Day of the week
			# [3] Barometric Pressure, measured in Pa
			# [4] Temp, in Celcius
			# [5] Wind speed, in kph
			# [6] Wind Direction, using the headings
			# [7] Was it raining. Yes or No
		"""
		# Transform the JSON in a workable list of dictionaries
		lst_of_dict = json.loads(json_wthr_data)["features"]

		master_lst = []

		def format_timestamp(timestamp, req_date=None):
			"""In-function process to parse the time stamp"""
			# Split the timestamp
			tmp_ts = timestamp.split("T")
			tmp_date = tmp_ts[0]
			tmp_time = tmp_ts[1]

			# Get rid of the extra time zone info at the end
			tmp_time = tmp_time.split("+")[0]

			# Format time for Central Chicago time zone. -05:00
			# Split into H M S
			cnt_tz = tmp_time.split(":")
			# Convert to integer
			cnt_tz[0] = int(cnt_tz[0])
			cnt_tz[1] = int(cnt_tz[1])
			cnt_tz[2] = int(cnt_tz[2])

			# Round to the past hour
			cnt_tz[1] = '00'; cnt_tz[2] = '00'

			# Subtract the 5 hours to account for time offset
			if cnt_tz[0] - 5 < 0:
				cnt_tz[0] = (24-(5-cnt_tz[0]))
			else:
				cnt_tz[0] = cnt_tz[0] - 5

			cnt_tz[0] = str(cnt_tz[0])

			# Add a 0 to the front of the hour if it's only a single digit
			if len(cnt_tz[0]) == 1:
				cnt_tz[0] = "0" + cnt_tz[0]
			# Now bring it all together to form the final string
			fnl_cnt_tz = cnt_tz[0] + ":" + cnt_tz[1] + ":" + cnt_tz[2]

			if req_date == None:
				return(tmp_date, fnl_cnt_tz)
			else:
				return(req_date, fnl_cnt_tz)

		def check_for_NoneType(lst):
			"""Checks for NoneTypes and replaces with NA's if found"""
			n_lst = lst
			for indx in range(len(n_lst)):
				if str(n_lst[indx]) == 'None':
					n_lst[indx] = "NA"

			return(n_lst)

		def was_it_raining(prp_dict):
			"""Check percipitation value. If it did rain return Yes, otherwise return
			No.
			"""
			try:
				# If there is a value other than None, then there was rain
				precip = prp_dict["precipitationLastHour"]["value"]
			except Exception as err:
				self._weather_log(str(err))

			if str(precip) == "None":
				return("No")
			else:
				return("Yes")

		# Loop through the data_dict and extract the data we want
		for day_indx in range(len(lst_of_dict)):
			tmp_lst = [] # List to hold the daily values

			# Extract the properties dictionary to use as our base
			indx_prp = lst_of_dict[day_indx]["properties"]

			# Time stamp
			if req_date != None:
				indx_day, indx_time = format_timestamp(indx_prp["timestamp"],
										   req_date=req_date)
			else:
				indx_day, indx_time = format_timestamp(indx_prp["timestamp"])

			# Append all the hourly values of the day to the tmp_lst
			tmp_lst.append(indx_day) # [0] Date YYYY-MM-DD
			tmp_lst.append(indx_time) # [1] Time
			tmp_lst.append(self.get_day_of_the_week(indx_day)) # [2] Day of the week
			tmp_lst.append(indx_prp["barometricPressure"]["value"]) # [3] Bar Pressure
			tmp_lst.append(indx_prp["temperature"]["value"]) # [4] Temp
			tmp_lst.append(indx_prp["windSpeed"]["value"]) # [5] Wind speed
			tmp_lst.append(indx_prp["windDirection"]["value"]) # [6] Wind Direction
			tmp_lst.append(was_it_raining(indx_prp)) # [7] Was it raining

			# Replace Nonetypes with NA's
			tmp_lst = check_for_NoneType(tmp_lst)

			# Append the day (tmp_lst) to the master
			master_lst.append(tmp_lst)

		return(master_lst, lst_of_dict)

	def get_day_of_the_week(self, prp_day):
			"""Get the date and return which day of the week it is"""
			# Parse the date to turn it into Y M D with no leading zeros
			date_lst = prp_day.split("-")

			date_year = int(date_lst[0])
			date_month = int(date_lst[1])
			date_day = int(date_lst[2])

			# Create a date object
			date_obj = date(date_year, date_month, date_day)

			return(calendar.day_name[date_obj.weekday()])

	def _convert_pa_to_hg(self, pa):
		"""From pascal to inch in mercury"""
		try:
			hg = round((float(pa) / 3386), 2)
			return(hg)
		except Exception as err:
			err = str(err)
			return("NA")

	def _convert_c_to_f(self, c_temp):
		"""Convert Celcius to farenheit"""
		try:
			f_temp = round(((float(c_temp) * (9/5)) + 32), 2)
			return(f_temp)
		except Exception as err:
			err = str(err)
			return("NA")

	def _convert_kph_to_mph(self, kph):
		"""Convert from Kph and returns Mph"""
		try:
			mph = round((float(kph) / 1.609), 2)
			return(mph)
		except Exception as err:
			err = str(err)
			return("NA")

	def _headings_to_ordinal(self, wind_direction):
		"""Convert numerical heading into ordinal wind directions.
		N, NE, E, SE, S, SW, W, NW
		"""
		if wind_direction == "NA": # Skip if there was no value
			return("NA")

		wind_dir = int(wind_direction)

		# Convert the headings into directions
		if wind_dir > 337.5:
			return("N")
		if wind_dir > 0 and wind_dir <= 22.5:
			return("N")
		if wind_dir > 22.5 and wind_dir <= 67.5:
			return("NE")
		if wind_dir > 67.5 and wind_dir <= 112.5:
			return("E")
		if wind_dir > 112.5 and wind_dir <= 157.5:
			return("SE")
		if wind_dir > 157.5 and wind_dir <= 202.5:
			return("S")
		if wind_dir > 202.5 and wind_dir <= 247.5:
			return("SW")
		if wind_dir > 247.5 and wind_dir <= 292.5:
			return("W")
		if wind_dir > 292.5 and wind_dir <= 337.5:
			return("NW")

	def convert_to_merica(self, lst_of_wthr):
		"""Takes in a list of already generated weather data and converts units into
		merica units. C -> F, Kph -> Mph. And change the wind direction from compass
		rose messurements to base NEWS directions.
		"""
		# Create copy of the list
		modded_list = lst_of_wthr

		# Iterate through the list of records
		for hour_indx in range(len(modded_list)):
			# Convert the pressure from pa to hg
			modded_list[hour_indx][3] = self._convert_pa_to_hg(modded_list[hour_indx][3])
			# Convert temp from C to F
			modded_list[hour_indx][4] = self._convert_c_to_f(modded_list[hour_indx][4])
			# Convert wind KPH into MPH
			modded_list[hour_indx][5] = self._convert_kph_to_mph(modded_list[hour_indx][5])
			# Transform the headings into ordinal directions
			modded_list[hour_indx][6] = self._headings_to_ordinal(modded_list[hour_indx][6])

		return(modded_list)

	def get_current_weather(self, station):
		"""Returns the current weather for the closest posted hour.
		@Parameters:
			- station : str
				Name of the station to get the weather from.
		"""
		# Generate current day and format it
		curr_date = datetime.datetime.now().strftime("%Y-%m-%d")
		# Get API URL
		api_call_url = self._generate_api_call(station, req_date=curr_date, limit=1,
										 fun_call=1)

		return(self._execute_api_call(api_call_url))

	def get_daily_weather(self, station, req_date):
		"""Returns the weather for the specified date at the station as a list.
		@Parameters:
			- station : str
				Name of the station to get the weather from.
			- date : str
				A specific day formatted YYYY-MM-DD
		"""
		# Set variables to different names
		le_station = station
		le_date = req_date

		# Pass Information to api generator
		api_call_url = self._generate_api_call(le_station, le_date, fun_call=2)
		# Pass URL and retrieve JSON data
		raw_weather_json = self._execute_api_call(api_call_url)

		# Transform the JSON data into python dictionaries
		wthr_data_lst, dbg_list = self.tranform_json_to_list(raw_weather_json,
													   req_date=le_date)

		return(wthr_data_lst, dbg_list)

	def _write_raw_to_file(self):
		"""This function is ONLY for debugging.
		It writes the captured data to a local JSON file. This is so we can create
		methods to parse the JSON data either in a way to make it work or to make
		it more efficient.
		"""
		raise NotImplementedError()

	def write_daily_to_csv(self, wthr_data, output_location, header=True):
		"""Write weather out to a CSV file"""
		file_date = wthr_data[0][0]
		# Define location to write CSV to
		csv_write_loc = output_location + "{}_Weather_log.csv".format(file_date)

		# Header for CSV if True
		weather_header = ["Date", "Time", "Weekday", "BarometricPressure", "Temp",
					      "WindSpeed", "WindDirection", "Raining"]

		self._weather_log("Writing out weather data to CSV...")

		try:
			with open(csv_write_loc, 'w', newline='') as csv_out:
				data_writer = csv.writer(csv_out, delimiter=',')
				if header:
					data_writer.writerow(weather_header)
				for row in wthr_data:
					data_writer.writerow(row)

		except Exception as err:
			self._weather_log("Could not write out to CSV...")
			self._weather_log(str(err))

		self._weather_log("CSV successfully created...")

	def _format_wunder_time(self, wunder_time):
		"""Format the time for wunderground into 24hr time
		Already accounts for Chicago time zone.
		"""
		# Split the time from AM/PM
		split_time = wunder_time.split(' ')
		curr_hour = int(split_time[0].split(":")[0])
		meridiem = split_time[1] # AM or PM

		# Adding or subtracting 12 depending on AM or PM
		meridiem_coef = 0
		if meridiem == "PM":
			meridiem_coef = 12

		# If it is 12AM then set the hour to 0
		if curr_hour == 12 and meridiem == "AM":
			curr_hour = 0
		# Otherwise set the hour normally
		else:
			curr_hour = curr_hour + meridiem_coef

		# Add leading zero if it's a single digit
		if len(str(curr_hour)) == 1:
			curr_hour = "0" + str(curr_hour)

		return(str(curr_hour) + ":00:00")

	def _format_wunder_baro(self, baro_p):
		"""Strip the string so only the pressure value is left"""
		if baro_p != "NA" or baro_p != "None":
			new_bar = baro_p.split(' ')[0][:5]
			return(new_bar)
		else:
			return("NA")

	def _format_wunder_number(self, w_value):
		"""Format numbers from wunder. Strips all but the number"""
		if w_value != "NA" or w_value != "None":
			temp_numeric = ""
			for num in w_value:
				if num.isdigit():
					temp_numeric = temp_numeric + num
			return(temp_numeric)
		else:
			return("NA")

	def _format_wunder_wind_direction(self, w_direction):
		"""Wunder uses a 16 point system where we only use an 8. Convert over to
		our way of directions"""
		# Makes sure it's not Null first
		if w_direction == "NA" or w_direction == "None":
			return("NA")

		direction = w_direction

		# Go through and make case like statements for the wind direction
		if direction == "NNW" or direction == "NNE":
			direction = "N"
		if direction == "ENE" or direction == "ESE":
			direction = "E"
		if direction == "SSE" or direction == "SSW":
			direction = "S"
		if direction == "WSW" or direction == "WNW":
			direction = "W"

		return(direction)

	def _format_wunder_rain(self, w_rain):
		"""Detects if it was raining that hour or not"""
		# In short if it's not equal to Null or the 0.0 in value then there is a good
		# chance it was raining
		w_rain=str(w_rain)
		if w_rain == "NA" or w_rain == "None":
			return("No")
		if w_rain[:3] == "0.0":
			return("No")
		else:
			return("Yes")

	def wunderground_convert(self, csv_location):
		"""Sometimes the weather gov api does not have past data to retrieve. I found
		this site wunderground that keeps historical hourly data. This function takes
		the hourly data from that site and creates a list to match the output that we
		would expect from the weather gov api output.
		Note:
			- The name of wonderground csv HAS TO BE in the YYYY-MM-DD.csv format
			- The data copied from the hourly section was copy and pasted in excel
			- This will return a list that can be used with the write_daily_to_csv f(x)
		"""
		wunder_data = [] # Hold the data from the csv file

		# Grab the date from the file name. This is why it's important to name it right
		file_date = csv_location.split("/")[-1][:-4]

		# Get the day of the week
		day_of_week = self.get_day_of_the_week(file_date)

		# Read in the CSV data
		try:
			with open(csv_location, newline='') as csvfile:
				# CSV reader that seperated by commas
				wunder_reader = csv.reader(csvfile, delimiter=',')
				# Add Rows to raw data variable
				for row in wunder_reader:
					# Okay, let's do some organizing to match the imported data
					# to our expected output list from the weather gov api
					tmp_row = []

					# [0] Date
					tmp_row.append(file_date)
					# [1] Time
					tmp_row.append(self._format_wunder_time(row[0]))
					# [2] Weekday
					tmp_row.append(day_of_week)
					# [3] BarometricPressure
					tmp_row.append(self._format_wunder_baro(row[7]))
					# [4] Temp
					tmp_row.append(self._format_wunder_number(row[1]))
					# [5] Wind Speed
					tmp_row.append(self._format_wunder_number(row[5]))
					# [6] Wind Direction
					tmp_row.append(self._format_wunder_wind_direction(row[4]))
					# [7] Was it raining
					tmp_row.append(self._format_wunder_rain(row[8]))

					wunder_data.append(tmp_row)

				csvfile.close()

		except Exception as err:
			self._weather_log("Could not read-in wunder data.\n"+str(err))

		return(wunder_data)


#if __name__ == "__main__":
	'''
	daily_weather, data_dict = Weather().get_daily_weather("KFTW", "2022-07-22")
	daily_weather = Weather().convert_to_merica(daily_weather)
	# Write out to weather location
	Weather().write_daily_to_csv(daily_weather, global_weather_write_loc)
	'''
	'''
	# Bulk update
	wunder_dir = "Z:/Projects/ADSB-Flight-Freq-Tracker/data/.wunderground_wthr/"
	list_of_files = os.listdir(wunder_dir)

	for file in list_of_files:
		w_path = wunder_dir + file
		wunder_data = Weather().wunderground_convert(w_path)
		Weather().write_daily_to_csv(wunder_data, global_weather_write_loc)
	'''
	'''
	wunder_path = "Z:/Projects/ADSB-Flight-Freq-Tracker/data/.wunderground_wthr/2022-06-20.csv"
	wunder_data = Weather().wunderground_convert(wunder_path)
	Weather().write_daily_to_csv(wunder_data, global_weather_write_loc)
	'''

























