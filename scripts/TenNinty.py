################################################################################
# TenNinty.py 
# @author: Ryan Herrin
#
# Mulit-Class script  
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

import csv 


class TenNinty_Parser:
    '''Class that takes in data generated from a dump1090 aplication and modifies
    it and returns a custom csv file'''
    def __init__(self, csv_dump_loc):
        self.focused_columns = [4, 6, 7, 10, 11, 12, 17]
        self.csv_dump_loc = csv_dump_loc
        self.TenNinty_Raw = self._read_dumpfile()
        self.dump_data = []
        self.unique_hex = [] # Keep a track of unique hex values for appending new data

    def _logger(self, x):
        ''' Logger function for custom output '''
        out_x = str(x)
        print(out_x)

    def get_parsed_data(self, use_header=False):
        ''' Return parsed data. '''
        self.parse_file()
        if use_header:
            self._add_header()

        return(self.dump_data)

    def get_raw_data(self):
        ''' For whatever reason you can retrieve the raw input '''
        return(self.TenNinty_Raw)

    def pretty_print(self):
        ''' Better readability if running from the command line'''
        #TODO
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
                    if row[4] == '000000':
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
            # If it's an empty string then go ahead and say it's unknown
            if flight_num == '':
                self.dump_data[flight].append('NA')
            # If the first 3 are letters, it's commercial and the Callsign is added
            elif flight_num[:3].isalpha():
                self.dump_data[flight].append(flight_num[:3])
            # If the first 2 are letters, it's rare and should be addressed, but is added
            elif flight_num[:2].isalpha():
                self.dump_data[flight].append(flight_num[:2])
            # If the first char is "N", then it's 99% a private aircraft
            elif flight_num[0] == "N":
                self.dump_data[flight].append("Private")

    def _add_header(self):
        '''Add header to beginning of array'''
        return(self.dump_data.insert(0, ['HexCode', 'Date', 'Time', 'FlightNumber', 
            'Alt', 'GroundSpeed', 'Squawk', 'Airline']))
    
    def parse_file(self):
        ''' Parse the data that was read in. '''
        for row in self.TenNinty_Raw:
            # Check every row for the unique Hex
            # If the Hex isn't in the dump_data array then add it 
            if row[4] not in self.unique_hex:
                    self.dump_data.append([row[4], row[6], row[7], row[10],
                                           row[11], row[12], row[17]])
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
                    if row[6] != '' and self.dump_data[pos][1] == '':
                        self.dump_data[pos][1] = row[6]
                    if row[7] != '' and self.dump_data[pos][2] == '':
                        self.dump_data[pos][2] = row[7]
                    if row[10] != '' and self.dump_data[pos][3] == '':
                        self.dump_data[pos][3] = str(row[10]).strip()
                    if row[11] != '' and self.dump_data[pos][4] == '':
                        self.dump_data[pos][4] = row[11]
                    if row[12] != '' and self.dump_data[pos][5] == '':
                        self.dump_data[pos][5] = row[12]
                    if row[17] != '' and self.dump_data[pos][6] == '':
                        self.dump_data[pos][6] = row[17]

        # Add the callsign row 
        self._add_callsign()
        

if __name__ == "__main__":
    csv_location = "Z:\\Projects\\ADSB_Flight_Track\\Data\\Sample_Data\\30003_Sample_Data.csv"
    process_data = TenNinty_Parser(csv_location)
    file_data = process_data.get_parsed_data(use_header=True)

    for i in file_data:
        print(i)










