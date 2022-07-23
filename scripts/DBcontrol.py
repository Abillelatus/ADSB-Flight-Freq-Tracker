# -*- coding: utf-8 -*-
################################################################################
# DBcontrol.py 
# @author: Ryan Herrin
#
# Used for creating functions that interacts with a chose DB.
# This could be Queries, insert statements, or anything else 
################################################################################

import os
import csv
import mysql.connector


verbose = True # Screen logging

class MySQLdb:
	"""Class to interact with the MySQL database"""
	def __init__(self, server, username, pw, database):
		self.SERVER = server
		self.USERNAME = username
		self.PW = pw
		self.DATABASE = database
		# Create connection 
		self.connection = mysql.connector.connect(
			host=self.SERVER, user=self.USERNAME, password=self.PW,
			database=self.DATABASE)
		
	def _db_logger(self, print_data):
		""" Custom logger """
		if verbose:
			print_data = str(print_data) #Strong Typecast to string 
			print("[DB] > {}".format(print_data))
		else:
			pass
		
	def _execute_statement(self, statement):
		"""Performs the statement execution for each funtion"""
		num_inserted = 0
		try:
			# Create a new cursor instance and execute 
			db_cursor = self.connection.cursor()
			db_cursor.execute(statement)
			# Get results 
			#results = db_cursor.fetchall()
			
			num_inserted +=1
			
			#self._db_logger(statement)
			
		except Exception as err:
			#self._db_logger(statement)
			print(str(err))
			
		finally:
			self._commit()
			db_cursor.close() # Close out the cursor
			 
	def upload_flight_csv(self, csv_location):
		"""Upload the data of the parsed flight data into the DB"""
		# SQL insert statement 
		sql_cmd = "INSERT INTO flight_data VALUES"
		
		csv_data = []
		# Read in CSV Data 
		try:
			with open(csv_location, newline='') as csvfile:
				csv_reader = csv.reader(csvfile, delimiter=',')
				next(csv_reader)
				for row in csv_reader:
					csv_data.append(row)
					
				csvfile.close()
			
		except Exception as err:
			self._db_logger(str(err))
			
		for line in csv_data:
			self._execute_statement(sql_cmd + str(tuple(line)))
		
	def _commit(self):
		self.connection.commit()
		
	def close(self):
		"""Close the MySQL DB connection"""
		self._db_logger("Closing SQL Connection...")
		self.connection.close()

def my_sql_bulk_upload(dir_path, mysql_connection):
	"""Insert all the values from all the CSV files in a specified location"""
	# Get list of all files in the raw data directory 
	raw_data_list = os.listdir(dir_path)
	
	# Go through all the files in the directory 
	for raw_file in raw_data_list:
		if raw_file != ".init":
			raw_file_path = dir_path + raw_file
			
			# Upload all the data
			mysql_connection.upload_flight_csv(raw_file_path)
			
	return(True)	

'''
if __name__ == "__main__":
	csv_dir_loc = "Z:/Projects/ADSB-Flight-Freq-Tracker/data/adsb_processed_data/"
	
	# MySQL Testing 
	pw = open("Z:/Projects/Cred_Manager/MySql_pw.txt", "r").readline()
	try:
		sql_conn = MySQLdb("localhost", "root", pw, "adsb_flight_db")
		# Bulk update 
		my_sql_bulk_upload(csv_dir_loc, sql_conn)
		#sql_test.upload_flight_csv(tmp_csv_location)
		
	except Exception as err:
		print(str(err))
		
	finally:
		sql_conn.close()
'''










