import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS factSongPlays;"
user_table_drop = "DROP TABLE IF EXISTS dimUsers;"
song_table_drop = "DROP TABLE IF EXISTS dimSongs;"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists;"
time_table_drop = "DROP TABLE IF EXISTS dimTime;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist TEXT,
        auth TEXT,
        first_name TEXT,
        gender TEXT,
        item_in_session INTEGER,
        last_name TEXT,
        length NUMERIC,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration NUMERIC,
        session_id INTEGER,
        song TEXT,
        status INTEGER,
        ts BIGINT,
        user_agent TEXT,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id TEXT,
        artist_name TEXT,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location TEXT,
        song_id TEXT,
        title TEXT,
        duration NUMERIC,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factSongPlays (
        songplay_id INTEGER IDENTITY(0,1), 
        start_time TIMESTAMP, 
        user_id TEXT, 
        level TEXT, 
        song_id TEXT, 
        artist_id TEXT, 
        session_id INT, 
        location TEXT, 
        user_agent TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimUsers (
        user_id TEXT, 
        first_name TEXT, 
        last_name TEXT, 
        gender TEXT, 
        level TEXT
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimSongs (
        song_id TEXT, 
        title TEXT, 
        artist_id TEXT, 
        year INT, 
        duration DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimArtists (
        artist_id TEXT, 
        name TEXT, 
        location TEXT, 
        latitude DECIMAL, 
        longitude DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimTime (
        start_time TIMESTAMP, 
        hour INT, 
        day INT, 
        week INT,
        month INT, 
        year INT, 
        weekday INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from 's3://udacity-dend/log-data'
    credentials 'aws_iam_role={}'
    compupdate off
    region 'us-west-2'
    JSON 'auto' truncatecolumns;
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    copy staging_songs 
    from 's3://udacity-dend/song-data'
    credentials 'aws_iam_role={}'
    compupdate off
    region 'us-west-2'
    JSON 'auto' truncatecolumns;
""").format(config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO factSongPlays (start_time, user_id,
        level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
    FROM staging_events AS se 
    INNER JOIN staging_songs AS ss
    ON (se.artist = ss.artist_name)
    AND (se.song = ss.title)
    AND (se.length = ss.duration)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO dimUsers (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.user_id, se.first_name, se.last_name, se.gender, se.level
    FROM staging_events AS se
    WHERE page='NextSong';
""")

song_table_insert = ("""
    INSERT INTO dimSongs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO dimArtists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT a.start_time, 
    EXTRACT(HOUR FROM a.start_time), EXTRACT(DAY FROM a.start_time), 
    EXTRACT(WEEK FROM a.start_time), EXTRACT(MONTH FROM a.start_time), 
    EXTRACT(YEAR FROM a.start_time), EXTRACT(WEEKDAY FROM a.start_time)
    FROM factSongPlays AS a;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
