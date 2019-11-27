class SqlQueries:
    songplay_table_insert = """
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events) events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE events.page = 'NextSong'
        """

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
   
    songplays_null = ("""
        SELECT count(*)
        FROM songplays
        WHERE level IS NULL OR 
        location IS NULL OR
        playid IS NULL OR
        sessionid IS NULL OR
        start_time IS NULL OR
        user_agent IS NULL OR
        userid IS NULL
       """)
    
    songs_null = ("""
        SELECT count(*)
        FROM songs
        WHERE artistid IS NULL OR
        duration IS NULL OR 
        songid IS NULL OR
        title IS NULL OR
        year IS NULL 
       """)
    
    artists_null = ("""
        SELECT count(*)
        FROM artists
        WHERE artistid IS NULL OR
        lattitude IS NULL OR 
        location IS NULL OR
        longitude IS NULL OR
        name IS NULL 
       """)
    
    users_null = ("""
        SELECT count(*)
        FROM users
        WHERE first_name IS NULL OR
        gender IS NULL OR 
        last_name IS NULL OR
        level IS NULL OR
        userid IS NULL
       """)
    
    time_null = ("""
        SELECT count(*)
        FROM time
        WHERE day IS NULL OR
        hour IS NULL OR 
        month IS NULL OR
        start_time IS NULL OR
        week IS NULL OR
        weekday IS NULL OR
        year IS NULL 
       """)    
    
    dq_checks = [
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0}
        ]    
    