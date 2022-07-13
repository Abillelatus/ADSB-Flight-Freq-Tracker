# -*- coding: utf-8 -*-
################################################################################
# DBcontrol.py 
# @author: Ryan Herrin
#
# Used for creating functions that interacts with a chose DB.
# This could be Queries, insert statements, or anything else 
################################################################################

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
		try:
			# Create a new cursor instance and execute 
			db_cursor = self.connection.cursor()
			db_cursor.execute(statement)
			# Get results 
			results = db_cursor.fetchall()
			return(results)
			
		except Exception as err:
			print(str(err))
			
		finally:
			db_cursor.close() # Close out the cursor
			 
			
	def upload_flight_csv(self, csv_location):
		"""Upload the data of the parsed flight data into the DB"""
		
		
		
	def close(self):
		"""Close the MySQL DB connection"""
		self.connection.close()



if __name__ == "__main__":
	tmp_csv_location = "Z:/Projects/ADSB-Flight-Freq-Tracker/data/adsb_processed_data/2022_06_20_232430_log.csv"
	
	# MySQL Testing 
	











