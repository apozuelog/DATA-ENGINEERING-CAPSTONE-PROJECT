CREATE TABLE IF NOT EXISTS bikes.bikes_table (
	bike_id int not NULL,
	CONSTRAINT bike_pkey PRIMARY KEY (bike_id)
);

CREATE TABLE IF NOT EXISTS bikes.time_table (
	start_time timestamp not NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE IF NOT EXISTS bikes.stations_table (
	station_id int not NULL,
	station_name varchar(256),
	station_lat DOUBLE PRECISION,
	station_lon DOUBLE PRECISION,
	CONSTRAINT station_pkey PRIMARY KEY (station_id)
);

CREATE TABLE IF NOT EXISTS bikes.users_table (
	user_type varchar(256),
	birth_year int,
	gender varchar(256),
	share varchar(256)
);

CREATE TABLE IF NOT EXISTS bikes.staging_bike_trips (
    duration_sec int,
    start_time timestamp,
    end_time timestamp,
    start_station_id int,
    start_station_name varchar (256),
    start_station_latitude DOUBLE PRECISION,
    start_station_longitude DOUBLE PRECISION,
    end_station_id int,
    end_station_name varchar(256),
    end_station_latitude DOUBLE PRECISION,
    end_station_longitude DOUBLE PRECISION,
    bike_id int,
    user_type varchar(256),
    member_birth_year int,
    member_gender varchar(20),
    bike_share_for_all_trip varchar(10)
);

CREATE TABLE IF NOT EXISTS bikes.bike_trips_table (
	trip_id bigint,
	start_time timestamp,
	end_time timestamp,
	duration_sec int,
	start_station_id int,
	end_station_id int,
	bike_id int,
	user_type varchar(256),
	member_birth_year int,
	member_gender varchar(256),
	CONSTRAINT trip_pkey PRIMARY KEY (trip_id),
	foreign key(start_time) references bikes.time_table(start_time),
	foreign key(start_station_id) references bikes.stations_table(station_id),
	foreign key(end_station_id) references bikes.stations_table(station_id),
	foreign key(bike_id) references bikes.bikes_table(bike_id)
);

