################################################################################
# TenNinty.py 
# @author: Ryan Herrin
#
# Mulit-Class script for processing captured ADSB data and trasnforming it into
# a custom tailored CSV file. 
################################################################################

''' 
Columns to capture and the description 
Col[python index], Desc:
------------------
[4], Aircraft Hexidecimal code (Intended to use as primary key)
[6], Date Message generated 
[7], Time Message generated 
[10], Callsign/Flight Number
[11], Altitude
[12], GroundSpeed
[17], Squawk code
'''

import os
import csv
import shutil
import datetime


class TenNinty_Parser:
	'''Class that takes in data generated from a dump1090 aplication and modifies
	it and returns a custom csv file'''
	def __init__(self, csv_dump_loc):
		self.focused_columns = [4, 6, 7, 10, 11, 12, 17]
		self.csv_dump_loc = csv_dump_loc
		self.TenNinty_Raw = self._read_dumpfile()
		self.dump_data = []
		self.unique_hex = [] # Keep a track of unique hex values for appending new data
		self.parsed_file_name = self._get_file_name()

	def _logger(self, x):
		''' Logger function for custom output '''
		out_x = str(x)
		print(out_x)
		
	def _get_file_name(self):
		file_name = self.csv_dump_loc.split("/")[-1]
		file_name = file_name.split('_')
		# 2, 3, 4, 5
		return(file_name[2]+'_'+file_name[3]+'_'+file_name[4]+'_'+file_name[5]) 

	def get_parsed_data(self, path_to_write, use_header=False, to_csv=False):
		''' Return parsed data. '''
		self.parse_file()

		if use_header:
			self._add_header()

		# Write to CSV is to_csv is True 
		if to_csv:
			self.write_to_csv(path_to_write)

		return(self.dump_data)

	def get_raw_data(self):
		''' For whatever reason you can retrieve the raw input '''
		return(self.TenNinty_Raw)

	def pretty_print(self):
		''' Better readability if running from the command line'''
		for row in self.dump_data:
			print()

	def _read_dumpfile(self):
		''' Read in the 1090 dump file and return a parsed csv file with unique values '''
		# Array to hold the raw data 
		tmp_arry = []
		try:
			with open(self.csv_dump_loc, newline='') as csvfile:
				# CSV reader that seperated by commas
				TenNinty_Reader = csv.reader(csvfile, delimiter=',')
				# Add Rows to raw data variable 
				for row in TenNinty_Reader:
					# If the header or putty log info is included and needs to skip
					if "=~" in row[0]:
						pass
					elif row[4] == '000000':
						pass
					else:
						tmp_arry.append(row)

				csvfile.close()
			
			return(tmp_arry)

		except Exception as err:
			self._logger("Could not open CSV file: ")
			self._logger(str(err))

	def _add_callsign(self):
		'''Add a column to the data that identifies the aircrafts callsign if it has
		one. Some aircraft Have an 'N' number that signifies it's a private aircraft. 
		Others start with at least two letters and then numbers. That's the callsign 
		and the flight number'''
		# index 3 in dump_data is where the Flight Number is 
		for flight in range(len(self.dump_data)):
			flight_num = str(self.dump_data[flight][3])
			
			# Create a way to seperate the Callsign from the number if it is not "N"
			# or empty 
			callsign_isalpha_len = 0
			callsign = ''
			if len(flight_num) > 1:
			
				for letter in flight_num:
					if letter.isalpha():
						callsign_isalpha_len +=1
						callsign = callsign + letter
						
					else:
						break
			
			# If it's an empty string then go ahead and say it's unknown
			if flight_num == '' or flight_num == 'NA':
				self.dump_data[flight].append('NA')
			# If the first 3 are letters, it's commercial and the Callsign is added
			elif callsign_isalpha_len > 1:
				self.dump_data[flight].append(callsign)
			# If the first 2 are letters, it's rare and should be addressed, but is added
			#elif flight_num[:2].isalpha():
				#self.dump_data[flight].append(flight_num[:2])
			# If the first char is "N", then it's 99% a private aircraft
			elif flight_num[0] == "N":
				self.dump_data[flight].append("Private")
			else:
				# If all else incase to keep the 8 row integrity
				self.dump_data[flight].append('NA')

	def _add_header(self):
		'''Add header to beginning of array'''
		return(self.dump_data.insert(0, ['HexCode', 'Date', 'Time', 'FlightNumber', 
			'Alt', 'GroundSpeed', 'Squawk', 'Airline']))
    
	def _format_date(self, date):
		"""Format date to conform with MySQL standards"""
		return(date.replace('/', '-'))
	
	def _format_time(self, time):
		"""Format time to only keep H, M, S."""
		new_time = time.split('.')
		return(str(new_time[0]))
	
	def _not_null(self, data):
		"""For data fields that need to have NA instead of Null"""
		if data == '':
			return("NA")
		else:
			return(data)
	
	def parse_file(self):
		''' Parse the data that was read in. '''
		for row in self.TenNinty_Raw:
			# Check every row for the unique Hex
			# If the Hex isn't in the dump_data array then add it 
			if row[4] not in self.unique_hex:
					self.dump_data.append([
                        row[4],     	            # [0] HexCode
                        self._format_date(row[6]),  # [1] Data
                        self._format_time(row[7]),  # [2] Time
                        self._not_null(row[10]),    # [3] Flight / #
                        row[11],                    # [4] Altitude
                        row[12],                    # [5] Ground Speed
                        row[17]                     # [6] Squawk
                        ])
					self.unique_hex.append(row[4])
			# Else, if it is, then check if any of the values are updated or
			# Contain data thats missing 
			else:
				data_index = int()
				# Find index of Hex value that represents each aircraft 
				for pos in range(len(self.dump_data)):
					if row[4] == self.dump_data[pos][0]:
						data_index = pos
						
				# Update/or ignore the index entry with new information
				for val in row:
					if row[6] != '' and self.dump_data[data_index][1] == '':
						self.dump_data[data_index][1] = row[6]
					if row[7] != '' and self.dump_data[data_index][2] == '':
						self.dump_data[data_index][2] = row[7]
					if row[10] != '' and self.dump_data[data_index][3] == 'NA':
						self.dump_data[data_index][3] = str(row[10]).strip()
					if row[11] != '' and self.dump_data[data_index][4] == '':
						self.dump_data[data_index][4] = row[11]
					if row[12] != '' and self.dump_data[data_index][5] == '':
						self.dump_data[data_index][5] = row[12]
					if row[17] != '' and self.dump_data[data_index][6] == '':
						self.dump_data[data_index][6] = row[17]

		# Add the callsign row 
		self._add_callsign()

	def write_to_csv(self, write_path):
		''' Writes self.dump to a CSV file that can be used to upload to a DB '''
		# Define location to write CSV to
		csv_write_loc = write_path + "{}_log.csv".format(self.parsed_file_name)
		#print(csv_write_loc)

		with open(csv_write_loc, 'w', newline='') as csv_out:
			data_writer = csv.writer(csv_out, delimiter=',')
			for row in self.dump_data:
				data_writer.writerow(row)
				
	def get_closest_hour(self):
		'''OPTIONAL. Get the time and find the nearest hour. Past the 30 min mark rounds up, 
		under 30 min will round down. This is to provide a values to match up with the weather 
		data which only calculates weather every hour.'''
		pass
		
class SnapShot:
	''' This class is to be run with a cron job to take snap shots of the live log 
	file and write the data to a seperate file. Then clears the live feed file so 
	it doesn't become overwhelmingly large. Seriously. Dis boy gets big real quick.
	The plan is to have this run once an hour to keep the data size down. ''' 
	def __init__(self, live_feed_loc, copyto_loc=''):
		# Location of the live feed file 
		self.live_feed_loc = live_feed_loc
		self.copyto_loc = copyto_loc
		self.new_raw_path = ''
		self.curr_time = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")

	def snap_dat_feed(self):
		''' Capture the data currently in the file. Basically Copies it'''
		try:
			# Perform the copy
			shutil.copyfile(self.live_feed_loc, self.copyto_loc)
			
			# Parse and get root dir of where to store the copied file. AKA: remove the last 
			# part of the file path
			raw_data_dir = ''
			for path in self.copyto_loc.split('/')[:-1]:
				raw_data_dir = raw_data_dir + path + '/'
			
			# Rename file to avoid name conflicts in the same dir 
			self.new_raw_path = str(raw_data_dir + "live_raw_{}".format(self.curr_time))
			os.rename(self.copyto_loc, self.new_raw_path)
		
			# If the write was complete then we can erase the contents of the live stream
			try:
				open(self.live_feed_loc, 'w').close()
				
			except Exception as err:
				# Well erasing the data didn't go as planned. Fudge.
				print('Could not erase live feed file data...\n{}'.format(str(err)))
				
		except Exception as err:
			print("Could not copy live feed file...\n{}".format(str(err)))
			
	def get_raw_path(self):
		'''Returns path of the copied raw data location''' 
		return(str(self.new_raw_path))
	
# Function to re-run all raw data in the adsb_raw_data dir and create new 
# processed data. 
def _bulk_update(target_dir):
	'''This function is only meant to be run from command line. It will
	reproccess the raw data and generate new CSV files. Used mainly when
	the code to create the parsed CSV files is made. force_bulk_update 
	must be set to TRUE.'''
	# Get list of all files in the raw data directory 
	raw_data_list = os.listdir(target_dir)
	
	# Go through all the files in the directory 
	for raw_file in raw_data_list:
		if raw_file != ".init":
			raw_file_path = target_dir + raw_file
			
			# Run the process on each file not names .init
			TenNinty_Parser(raw_file_path).get_parsed_data(
				global_csv_write_loc, use_header=True, to_csv=True
				)
			
	return(True)


#++++++++++++++++++++++++
# #### ENTRY #### 
#++++++++++++++++++++++++

# Production mode. What it run autonomously. Set to False for de-bugging/develop
production_mode = False

force_bulk_update = True # To trigger the bulk update function. Can only be ran
						 # if production mode is set to False.  

# Define Global locations 
global_csv_write_loc = '/projects/ADSB-Flight-Freq-Tracker/data/adsb_processed_data/'
global_live_feed_loc = '/projects/ADSB-Flight-Freq-Tracker/data/30003_LiveFeed.csv'
global_raw_copyto_loc = '/projects/ADSB-Flight-Freq-Tracker/data/adsb_raw_data/30003_LiveFeed.csv'

# Determine full path based on if the system is Windows or running on the Linux(pi)
if os.name == "nt":
	# If Windows
	win_prefix = "Z:"
	global_csv_write_loc = win_prefix + global_csv_write_loc
	global_live_feed_loc = win_prefix + global_live_feed_loc
	global_raw_copyto_loc = win_prefix + global_raw_copyto_loc 
	
else:
	# If Linux(Pi)
	pi_prefix = "/home/pi/Documents"
	global_csv_write_loc = pi_prefix + global_csv_write_loc
	global_live_feed_loc = pi_prefix + global_live_feed_loc
	global_raw_copyto_loc = pi_prefix + global_raw_copyto_loc 

'''
if __name__ =="__main__":
	# Main entry for running in production mode		
	if production_mode:
		# Create copy of the live feed data 
		snapshot = SnapShot(global_live_feed_loc, global_raw_copyto_loc)
		snapshot.snap_dat_feed()
	
		# Process the captured CSV data using the TenNinty_parser 
		process_data = TenNinty_Parser(snapshot.get_raw_path())
	
		# Write it out to a csv 
		process_data.get_parsed_data(global_csv_write_loc, use_header=True, to_csv=True)
	
	if production_mode == False:
		"""Only for debugging and developement"""
	
		# Process the captured CSV data using the TenNinty_parser 
		raw_data_path = 'Z:/projects/ADSB-Flight-Freq-Tracker/data/adsb_raw_data/'
		
		#TenNinty_Parser(raw_data_path).get_parsed_data(global_csv_write_loc, 
		#											   use_header=True, to_csv=True)
	    
		# Manually reproduce the parsed data files from the raw data
		if force_bulk_update:
			_bulk_update(raw_data_path)
'''




























