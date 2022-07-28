# -*- coding: utf-8 -*-
################################################################################
# Demo.py
# @author(s): Ryan Herrin, Akib Hossain
#
# Used to showcase the DB projects
################################################################################

import matplotlib.pyplot as plt
from TenNinty import Callsigns
from DStaxAstraControl import DataStaxAstra


# User defined locations and data
CREDINTIAL_LOCATION = "Z:/Projects/Cred_Manager/ADSB-Flight-Data-token.json"
SECURE_ZIP_LOCATION = "Z:/Projects/Cred_Manager/secure-connect-adsb-flight-data.zip"
KEYSPACE_NAME = 'flights'
TABLE = "f_data"
'''
CREDINTIAL_LOCATION = "Z:/Projects/Cred_Manager/Flight-token.json"
SECURE_ZIP_LOCATION = "Z:/Projects/Cred_Manager/secure-connect-flight.zip"
KEYSPACE_NAME = 'n_full'
TABLE = "f_data"
'''

# Grab full list of callsigns 
callsigns = Callsigns().get_callsigns('../data/callsign_data.csv')

# Create DB object and set credintials
db_conn = DataStaxAstra()
db_conn.set_secure_zip_location(SECURE_ZIP_LOCATION)
db_conn.set_json_credintials(CREDINTIAL_LOCATION)
db_conn.set_keyspace(KEYSPACE_NAME)

# Create session and run queries
try:
	# Initialize the session
	db_session = db_conn.create_session()

	##### All further session calls go under here #####
	# Grab the total number of rows
	total_rows = db_conn.run_qry_total_rows(db_session, TABLE)
	
	# Get number of flights per weekday
	flts_per_wkdy = db_conn.run_qry_flight_per_weekday(db_session, TABLE)
	
	# Get number of flights depending on wind direction
	flts_per_wind = db_conn.run_qry_flights_by_wind_direction(db_session, TABLE)
	
	# Find the most popular airline 
	popular_airline = db_conn.run_qry_popular_callsign(
		db_session, TABLE, callsigns)

except Exception as err:
	print(str(err))

finally:
	# Always close out the session when done
	print("Closing Session Connection...")
	db_session.shutdown()

# Create Charts 
plt.bar(*zip(*flts_per_wkdy.items()))	
plt.show()
	
	
	

