# -*- coding: utf-8 -*-
################################################################################
# DStaxAstraControl.py
# @author(s): Ryan Herrin, Akib Hossain
#
# Used for creating functions that interacts with the Data Stax Astra DB
################################################################################

import os
import json
#import cassandra
from json import JSONDecodeError
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class DataStaxAstra:
	""" Class that connects to a Astra DB and can run commands"""
	def __init__(self):
		self.keyspace = str()
		self.zip_location = str()
		self.client_id = str()
		self.client_secret = str()

	def set_secure_zip_location(self, zip_location):
		"""Set path for the secure zip file"""
		# Check to see if path exists and set path if it does
		if os.path.exists(zip_location):
			self.zip_location = zip_location
		else:
			print("Secure Zip path does not exist...")

	def set_json_credintials(self, cred_json):
		"""Set credintials from JSON file provided by Astra"""
		cred_data = str()
		# Read in json file
		with open(cred_json, 'r') as json_infile:
			raw_data = json_infile.read()
			try:
				cred_data = json.loads(raw_data)
			except JSONDecodeError as err:
				print(str(err))
				print("Could not load in JSON data... Make sure there are ','" +
					  " delimeters...")

			json_infile.close() # Close file handler connection

		# Assing values
		self.client_id = cred_data["clientID"]
		self.client_secret = cred_data["clientSecret"]

	def set_keyspace(self, user_keyspace):
		"""Assign Keyspace"""
		self.keyspace = str(user_keyspace)

	def create_session(self):
		"""Attempt to create and return a session"""
		# Verify all credintials have been set
		if self.zip_location == '':
			print("Error: Zip location has not been set...")
			return(0)

		if self.client_id == '':
			print("Error: Credintials have not been set...")
			return(0)

		if self.keyspace == '':
			print("Error: Keyspace has not been set...")
			return(0)

		# Create cloud configuration
		cloud_config = {'secure_connect_bundle':self.zip_location}
		# Set credintials
		auth_provider = PlainTextAuthProvider(self.client_id, self.client_secret)
		cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

		# attempt to create the session
		try:
			session = cluster.connect(self.keyspace)
			return(session)

		except Exception as err:
			print(str(err))

	def run_custom_query(self, query_string):
		"""Run a custom made query string. Returns results.
		@Params:
			- query_string : str()
		"""
		raise NotImplementedError()
		
	def execute_qry(self, session, query):
		"""Execute the query on our own terms. Takes in a session, and query, and
		returns the results. This allows us to also format if wanted"""
		try:
			return(session.execute(query))
		except Exception as err:
			print(str(err))
		
	############ Pre-Defined Queries ###########
	# These are predifined queries that can be run once a session has been created.
	# You will need to pass in the session and table name
	def run_qry_test(self, session, table):
		query = ("SELECT * FROM {}.{} LIMIT 5;".format(self.keyspace, table))
		results = self.execute_qry(session, query)
		return(results)

	def run_qry_total_rows(self, session, table):
		query = ("SELECT COUNT(*) FROM {}.{}".format(self.keyspace, table))
		results = self.execute_qry(session, query)
		return(results.all()[0][0])
	
	def run_qry_flight_per_weekday(self, session, table):
		days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
		        "Saturday", "Sunday"]
		result_dict = dict()
		for day in days:
			query_day = ("SELECT COUNT(*) FROM {}.{} ".format(self.keyspace, table) +
			            "WHERE \"Weekday\" in ('{}') ".format(day) +
				        "ALLOW FILTERING;")
			result_dict[day] = self.execute_qry(session, query_day).all()[0][0]
		return(result_dict)
	
	def run_qry_flights_by_wind_direction(self, session, table):
		w_drctn = ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "CALM", "VAR"]
		result_dict = dict()
		for drctn in w_drctn:
			query = (
				"SELECT COUNT(*) FROM {}.{} ".format(self.keyspace, table) +
				"WHERE \"WindDirection\" in ('{}') ".format(drctn) +
				"ALLOW FILTERING;"
				)
			result_dict[drctn] = self.execute_qry(session, query).all()[0][0]
		return(result_dict)
	
	def run_qry_popular_callsign(self, session, table, callsigns):
		callsigns = callsigns
		result_dict = dict()
		for cs in callsigns:
			query = (
				"SELECT COUNT(*) FROM {}.{} ".format(self.keyspace, table) +
				"WHERE \"Airline\" in ('{}') ".format(cs) +
				"ALLOW FILTERING;"
				)
			result_dict[cs] = self.execute_qry(session, query).all()[0][0]
		return(result_dict)
	############################################
















