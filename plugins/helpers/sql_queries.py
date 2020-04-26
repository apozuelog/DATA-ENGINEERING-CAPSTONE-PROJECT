class SqlQueries:
    bike_trips_table_insert = ("""
        SELECT trip_id, start_time::timestamp, end_time::timestamp, replace(duration_sec,'""',0)::integer, 
                replace(start_station_id,'""',0)::integer, replace(end_station_id,'""',0)::integer, 
                replace(bike_id,'""',0)::integer, trim(user_type,'"'), 
                replace(member_birth_year,'""', 0)::integer, trim(member_gender,'"')
        FROM bikes.staging_trips
        WHERE start_station_id <> 'NULL' OR end_station_id <> 'NULL'
    """)

    bikes_table_insert = ("""
        SELECT bike_id::integer
        FROM bikes.staging_trips
    """)

    stations_table_insert = ("""
        SELECT distinct start_station_id::integer, trim(start_station_name,'"'), start_station_latitude, start_station_longitude
        from bikes.staging_trips 
        where start_station_id <> 'NULL'
        UNION
        SELECT distinct end_station_id::integer, trim(end_station_name,'"'), end_station_latitude, end_station_longitude
        from bikes.staging_trips
        where end_station_id <> 'NULL'
    """)

    time_table_insert = ("""
        SELECT distinct start_time::timestamp, extract(hour from (start_time::timestamp)), extract(day from (start_time::timestamp)), 
                extract(week from (start_time::timestamp)), extract(month from (start_time::timestamp)), 
                extract(year from (start_time::timestamp)), extract(dayofweek from (start_time::timestamp))
        FROM bikes.staging_trips
    """)
    
    users_table_insert = ("""
        SELECT trim(user_type,'"'), replace(member_birth_year,'""',0)::integer, trim(member_gender,'"'), trim(bike_share_for_all_trip,'"')
        FROM bikes.staging_trips
    """)
    
    