class SqlQueries:
    songplays_table_insert = ("""
        SELECT
                md5(events.sessionId || events.start_time) songplay_id,
                events.start_time, 
                events.userId, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionId, 
                events.location, 
                events.userAgent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staged_events
            WHERE page='NextSong') events
            LEFT JOIN staged_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    users_table_insert = ("""
        SELECT distinct userId, firstName, lastName, gender, level
        FROM staged_events
        WHERE page='NextSong'
    """)

    songs_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staged_songs
    """)

    artists_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staged_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)