USE adsb_flight_db;

CREATE TABLE IF NOT EXISTS adsb_flight_db.flight_data (
	hex_code VARCHAR(20) NOT NULL,
    flight_day DATE NOT NULL,
    flight_time TIME NOT NULL,
    flight_number VARCHAR(20) NOT NULL,
    flight_alt INT,
    flight_grnd_spd INT,
    flight_squawk VARCHAR(10),
    flight_airline VARCHAR(20),
    PRIMARY KEY (hex_code, flight_number)
);

CREATE TABLE IF NOT EXISTS adsb_flight_db.callsign (
	airline_callsign VARCHAR(10) NOT NULL,
    owning_company VARCHAR(45) NOT NULL,
    PRIMARY KEY (airline_callsign)
);


