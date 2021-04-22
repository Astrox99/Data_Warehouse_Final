import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP ALL TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS sonplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
    artist          varchar,
    auth            varchar,
    firstName       varchar,
    gender          varchar,
    itemInSession   int,
    lastName        varchar,
    length          float,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    numeric,
    sessionId       int,
    song            varchar,
    status          int,
    ts              timestamp,
    userAgent       varchar,
    userId          int

)
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           int,
    artist_id           varchar,
    artist_latitude     numeric,
    artist_longitude    numeric,
    artist_location     varchar,
    artist_name         varchar,
    song_id             varchar,
    title               varchar,
    duration            float,
    year                int
) 
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     int identity(0,1)   PRIMARY KEY,
    start_time      timestamp,
    user_id         int                 NOT NULL,                 
    level           varchar,
    song_id         varchar             NOT NULL,
    artist_id       varchar             NOT NULL SORTKEY DISTKEY,
    session_id      int                 NOT NULL,
    location        varchar,
    user_agent      varchar
)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
    user_id         int                 SORTKEY PRIMARY KEY,
    first_name      varchar             NOT NULL,
    last_name       varchar             NOT NULL,
    gender          varchar,
    level           varchar
)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
    song_id         varchar             SORTKEY PRIMARY KEY,
    title           varchar             NOT NULL,
    artist_id       varchar             NOT NULL,
    year            int,
    duration        numeric    
)
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
    artist_id       varchar             SORTKEY DISTKEY PRIMARY KEY,
    name            varchar             NOT NULL,
    location        varchar,
    latitude        numeric,
    longitude       numeric
)
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
    start_time      timestamp           SORTKEY PRIMARY KEY,
    hour            int,
    day             int,
    week            int,
    month           int,
    year            int,
    weekday         int
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {bucket}
    iam_role {iam_role}
    json {json_path}
    timeformat as 'epochmillisecs'
    region 'us-west-2';

""").format(bucket = config['S3']['LOG_DATA'], iam_role = config['IAM_ROLE']['ARN'], json_path = config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {bucket}
    iam_role {iam_role}
    json 'auto'
    region 'us-west-2';

""").format(bucket = config['S3']['SONG_DATA'], iam_role = config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" 
    INSERT  INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT se.ts  AS start_time, 
            se.userId       AS user_id, 
            se.level        AS level, 
            ss.song_id      AS song_id, 
            ss.artist_id    AS artist_id, 
            se.sessionId    AS session_id, 
            se.location     AS location, 
            se.userAgent    AS user_agent
    FROM    staging_events se
    JOIN    staging_songs ss
    ON   (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT  INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT se.userId  AS user_id,
            se.firstName        AS first_name,
            se.lastName         AS last_name,
            se.gender           AS gender,
            se.level            AS level
    FROM    staging_events se
    WHERE   user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT  INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT ss.song_id AS song_id,
            ss.title            AS title,
            ss.artist_id        AS artist_id,
            ss.year             AS year,
            ss.duration         AS duration
    FROM    staging_songs ss
""")

artist_table_insert = ("""
    INSERT  INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT ss.artist_id   AS artist_id,
            ss.artist_name          AS name,
            ss.artist_location      AS location,
            ss.artist_latitude      AS latitude,
            ss.artist_longitude     AS longitude
    FROM    staging_songs ss
""")

time_table_insert = ("""
    INSERT  INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT start_time                 AS start_time,
            EXTRACT(HOUR FROM start_time)       AS hour,
            EXTRACT(DAY FROM start_time)        AS day,
            EXTRACT(WEEK FROM start_time)       AS week,
            EXTRACT(MONTH FROM start_time)      AS month,
            EXTRACT(YEAR FROM start_time)       AS year,
            EXTRACT(WEEKDAY FROM start_time)    AS weekday
    FROM    songplays
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create] 
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]