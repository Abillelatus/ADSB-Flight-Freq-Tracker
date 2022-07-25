################################################################################
# Combine.py 
# @author: Ryan Herrin
#
# Takes the processed flight data and creates one CSV file per day and merges 
# the weather data with it.
#
# This is not written as a class and should only used as a script 
################################################################################

import os
import csv


def get_p_flt_data_by_day(flt_data_loc, day):
	"""Get all csv files from specified day and load it into one master list that
	will be returned. (Flight Data)
	"""
	# Find all csv files for the specified day
	file_lst = os.listdir(flt_data_loc)
	csv_lst = []
	
	for file in file_lst:
		if day in file:
			csv_lst.append(file)
			
	# Extracted data
	p_flt_data = []
			
	# Read in the chosen csv files and create one large list for the day
	for csv_file in csv_lst:
		try:
			with open(flt_data_loc + '/' + csv_file, newline='') as csvfile:
				# CSV reader that seperated by commas
				flt_reader = csv.reader(csvfile, delimiter=',')
				flt_reader.__next__() # Skip the header 
				# Add Rows to raw data variable 
				for row in flt_reader:
					p_flt_data.append(row)
					
				csvfile.close()
	
		except Exception as err:
			print("Could not open CSV file: ")
			print(str(err))	
	
	return(p_flt_data)

def get_weather_data_by_day(wthr_data_loc, day):
	"""Get all csv files from specified day and load it into one master list that
	will be returned. (Weather)
	"""
	# Format date to match file names
	day_name = day.split("_")
	day_name = day_name[0]+"-"+day_name[1]+"-"+day_name[2] 
	
	# Find all csv files for the specified day
	file_lst = os.listdir(wthr_data_loc)
	csv_lst = []
	
	for file in file_lst:
		if day_name in file:
			csv_lst.append(file)
			
	# Extracted data
	wthr_data = []
	
	# Read in the chosen csv files and create one large list for the day
	for csv_file in csv_lst:
		try:
			with open(wthr_data_loc + '/' + csv_file, newline='') as csvfile:
				# CSV reader that seperated by commas
				day_reader = csv.reader(csvfile, delimiter=',')
				day_reader.__next__() # Skip the header 
				# Add Rows to raw data variable 
				for row in day_reader:
					wthr_data.append(row)
					
				csvfile.close()
	
		except Exception as err:
			print("Could not open CSV file: ")
			print(str(err))	
	
	return(wthr_data)
	
def get_active_days(flt_data_path):
	"""Parse through the directory and find all days that have data"""
	file_lst = os.listdir(flt_data_path)
	# Create a set of dates to store dates and eliminate duplicates 
	date_set = set()
	
	for file in file_lst:
		date_set.add(file[:10])
		
	# Organize by date 
	date_set = list(date_set)
		
	return(date_set)
	
def combine_flt_and_wthr(flt_data, wthr_data):
	"""Combine the flight data and weather data to create one large all inclusive 
	dataset (terms and conditions may apply).
	This function is going to take us for a ride so hold on. 
	"""
	# Okay lets do this....
	combined_list = []
	
	# For structual integrity make sure the num of rows read in from the flight data
	# is the same number of rows created and returned
	flt_data_len = len(flt_data)

	# Iterate through each row of the flight data 
	for flight_row in flt_data:
		# Iterate through each row of weather
		for weather_row in wthr_data:
			if flight_row[8] == weather_row[1]:
				tmp_lst = [
					flight_row[0], 	# HexCode
					flight_row[1], 	# Date 
					flight_row[2], 	# Time
					flight_row[3], 	# FlightNumber
					flight_row[4], 	# Alt
					flight_row[5],	# GroundSpeed
					flight_row[6],	# Squawk
					flight_row[7], 	# Airline
					weather_row[2], # Weekday
					weather_row[3], # BarometricPressure
					weather_row[4], # Temp
					weather_row[5], # WindSpeed
					weather_row[6], # WindDirection
					weather_row[7], # Raining
					]
				combined_list.append(tmp_lst)
				
	# Check to make sure the same number that went in is the same going out
	if len(combined_list) == flt_data_len:
		return(combined_list)
	else:
		print("Integrity Error. List Mismatch detected...")
		
def write_combined_to_csv(comb_data, write_to_location, day, header):
	"""Write the combined data to a csv file"""
	# Create file name 
	csv_file_name = write_to_location + '/' + day + '_full.csv'
	
	print("Creating CSV file: {}".format(csv_file_name))
	
	# Write out the data to a csv 
	try:
		with open(csv_file_name, 'w', newline='') as csv_outfile:
			data_writer = csv.writer(csv_outfile, delimiter=',')
			# write the header to the file 
			data_writer.writerow(header)
			# Now write out the combined data
			for row in comb_data:
				data_writer.writerow(row)
				
		print("CSV Created...")
			
	except Exception as err:
		print(str(err))
	
######## Entry #########
if __name__ == "__main__":
	flt_data_loc = "Z:/Projects/ADSB-Flight-Freq-Tracker/data/adsb_processed_data"
	wthr_data_loc = "Z:/Projects/ADSB-Flight-Freq-Tracker/data/weather_data"
	output_loc = "Z:/Projects/ADSB-Flight-Freq-Tracker/data/combined_data"
	
	full_header = [
		"HexCode", "Date", "Time", "FlightNumber", "Alt", "GroundSpeed",
		"Squawk", "Airline", "Weekday", "BarometricPressure", "Temp", 
		"WindSpeed", "WindDirection", "Raining"
		]
	
	# Get a list of active days 
	active_days = get_active_days(flt_data_loc) 
	
	for s_date in range(len(active_days)):
		# Grab the a day from the active days list and create a combined list of that 
		# data from that particular day 
		flt_day_data = get_p_flt_data_by_day(flt_data_loc, active_days[s_date])
		# Now grab the weather from that day 
		wthr_day_data = get_weather_data_by_day(wthr_data_loc, active_days[s_date])
		# Let's combine the weather and flight data now
		combined_data = combine_flt_and_wthr(flt_day_data, wthr_day_data)

		try:
			# Write it out to a csv file 
			write_combined_to_csv(combined_data, output_loc, active_days[s_date], full_header)
		except Exception as err:
			print("Could not write to CSV...\n"+str(err))
