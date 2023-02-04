import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
HOST = config.get('CLUSTER', 'HOST')
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = config.get('CLUSTER', 'DB_PORT')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table
(eventId INT IDENTITY(0, 1) PRIMARY KEY,
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender CHAR(1),
itemInSession SMALLINT,
lastName VARCHAR,
length DECIMAL,
level CHAR(10),
location VARCHAR,
method CHAR(10),
page VARCHAR,
registration VARCHAR,
sessionId INTEGER NOT NULL,
song VARCHAR,
status INTEGER,
ts INT8 NOT NULL,
userAgent VARCHAR,
userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table
(num_songs INTEGER,
artist_id VARCHAR NOT NULL,
artist_lattitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR PRIMARY KEY,
title VARCHAR NOT NULL,
duration DECIMAL NOT NULL,
year SMALLINT
)
diststyle even
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table
(songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
start_time_stamp TIMESTAMP NOT NULL,
user_id INTEGER NOT NULL,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table
(user_id INTEGER PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender CHAR(1),
level CHAR(10)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table
(song_id VARCHAR PRIMARY KEY,
title VARCHAR NOT NULL,
artist_id VARCHAR,
year INTEGER,
duration DECIMAL NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table
(artist_id VARCHAR PRIMARY KEY,
name VARCHAR NOT NULL,
location VARCHAR,
lattitude double precision,
longitude double precision
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table
(start_time_stamp INT8 PRIMARY KEY,
start_time TIMESTAMP,
hour SMALLINT,
day SMALLINT,
week SMALLINT,
month SMALLINT,
year SMALLINT,
weekday SMALLINT
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events_table from {}
iam_role {}
json {}
;
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs_table from {}
iam_role  {}
json 'auto'
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES


songplay_table_insert = ("""
INSERT INTO songplay_table
(start_time_stamp, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
t.start_time,
se.userId,
se.level,
ss.song_id,
ss.artist_id,
se.sessionId,
se.location,
se.userAgent
FROM staging_events_table se
JOIN time_table t
ON se.ts = t.start_time_stamp
JOIN staging_songs_table ss
ON (se.song = ss.title) AND (se.artist = ss.artist_name)
""")


user_table_insert = ("""
INSERT INTO user_table
(user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM staging_events_table
WHERE userId is not null;
""")
user_table_distinct = ("""
BEGIN;
CREATE TABLE temp1 AS SELECT DISTINCT * FROM user_table;
ALTER TABLE user_table RENAME TO temp2;
ALTER TABLE temp1 RENAME TO user_table;
DROP TABLE temp2;
COMMIT;
""")

song_table_insert = ("""
INSERT INTO song_table
(song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs_table
""")

artist_table_insert = ("""
INSERT INTO artist_table
(artist_id, name, location, lattitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_lattitude, artist_longitude
FROM staging_songs_table
""")

artist_table_distinct = (
"""
BEGIN;
CREATE TABLE temp1 AS SELECT DISTINCT * FROM artist_table;
ALTER TABLE artist_table RENAME TO temp2;
ALTER TABLE temp1 RENAME TO artist_table;
DROP TABLE temp2;
COMMIT;
"""
)

time_table_insert = ("""
INSERT INTO time_table
(start_time_stamp, start_time, hour, day, week, month, year, weekday)
(SELECT
ts,
TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
extract (hour from start_time),
extract (day from start_time),
extract (week from start_time),
extract (month from start_time),
extract (year from start_time),
extract (dow from start_time)
FROM staging_events_table
WHERE page = 'NextSong')
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id
FROM song_table s
LEFT JOIN artist_table a ON s.artist_id = a.artist_id
WHERE s.title = %s
AND a.name = %s
AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, songplay_table_insert, user_table_insert, user_table_distinct, song_table_insert, artist_table_insert, artist_table_distinct]
