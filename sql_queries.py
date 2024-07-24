import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES
print("Dropping facts and dimension tables if already created")

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

print("Creating table staging_events")

staging_events_table_create= ("""
CREATE TABLE staging_events(
artist VARCHAR, 
aut VARCHAR, 
firstName VARCHAR, 
gender VARCHAR, 
ItemInSession INT, 
lastName VARCHAR, 
length FLOAT,  
level VARCHAR, 
location TEXT, 
method VARCHAR, 
page VARCHAR, 
registration VARCHAR, 
sessionId INTEGER,
song VARCHAR,
status INTEGER, 
ts BIGINT,  
userAgent VARCHAR,  
userId INTEGER);""")

print("Created table staging_events")


print("Creating table staging_songs")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
song_id VARCHAR,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
duration FLOAT,
num_songs INT,
title VARCHAR,
year INT);""")

print("Created table staging_songs")


print("Creating table songplays")

songplay_table_create = ("""
CREATE TABLE songplays(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL sortkey distkey,
user_id INTEGER NOT NULL,
level VARCHAR,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR);""")

print("Created table songplays")


print("Creating table users")

user_table_create = ("""
CREATE TABLE users(
user_id VARCHAR PRIMARY KEY NOT NULL,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR);""")

print("Created table users")


print("Creating table songs")

song_table_create = ("""
CREATE TABLE songs(
song_id VARCHAR(500) PRIMARY KEY NOT NULL,
title VARCHAR(500) NOT NULL,
artist_id VARCHAR(500) NOT NULL,
year INTEGER,
duration FLOAT NOT NULL);""")

print("Created table songs")


print("Creating table artists")

artist_table_create = ("""
CREATE TABLE artists(
artist_id VARCHAR PRIMARY KEY NOT NULL,
name VARCHAR,
location VARCHAR,
latitude DECIMAL,
longitude DECIMAL);""")

print("Created table artists")


print("Creating table time")

time_table_create = ("""
CREATE TABLE time(
start_time TIMESTAMP PRIMARY KEY,
hour INTEGER, 
day  INTEGER, 
week INTEGER, 
month INTEGER, 
year INTEGER,
weekday INTEGER);""")

print("Created table time")


# STAGING TABLES
print("Creating staging tables")

print("Creating staging events copy from bucket")

staging_events_copy = ("""
copy staging_events from {bucket}
credentials 'aws_iam_role={role}'
region 'us-west-2'
format as JSON {path}
timeformat as 'epochmillisecs'""").format(bucket=LOG_DATA, role=ARN, path=LOG_JSONPATH)

print("Created staging events copy from bucket")

print("Creating staging songs copy from bucket")

staging_songs_copy = ("""
copy staging_songs from {bucket}
credentials 'aws_iam_role={role}'
region 'us-west-2'
format  as JSON 'auto'
TRUNCATECOLUMNS 
BLANKSASNULL 
EMPTYASNULL""").format(bucket=SONG_DATA, role=ARN)

print("Created staging songs copy from bucket")

# FINAL TABLES
print("Inserting data into created tables")


print("Inserting data into songplay table")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location, se.useragent
FROM staging_events se
JOIN staging_songs ss  
ON ss.artist_name  = se.artist
AND  se.page = 'NextSong'
AND  se.song  = ss.title;""")

print("Inserted data into songplay table")


print("Inserting data into users table")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT se.userid, se.firstname, se.lastname, se.gender, se.level
FROM staging_events se
JOIN(SELECT userid, MAX(ts) as max_time_stamp
FROM staging_events
WHERE page = 'NextSong'
GROUP BY userid) se2
ON se.userid = se2.userid
AND se.ts = se2.max_time_stamp
WHERE se.page = 'NextSong';""")

print("Inserted data into users table")


print("Inserting data into songplay table")

song_table_insert = ("""
INSERT INTO songs(song_id,title, artist_id, year, duration) 
SELECT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
FROM staging_songs ss;""")

print("Inserting data into songplay table")


print("Inserting data into artist table")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude,longitude) 
SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
FROM staging_songs ss;""")

print("Inserted data into artist table")


print("Inserting data into time table")

time_table_insert = ("""
INSERT INTO time 
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
EXTRACT (hour FROM start_time),
EXTRACT (day FROM start_time),
EXTRACT (week FROM start_time),
EXTRACT (month FROM start_time),
EXTRACT (year FROM start_time),
EXTRACT (weekday FROM start_time)
FROM staging_events
WHERE ts IS NOT NULL;""")

print("Inserted data into time table")

# QUERY LISTS
print("Executing create table queries")
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

print("Executing drop table queries")
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

print("Executing copy table queries")
copy_table_queries = [staging_events_copy, staging_songs_copy]

print("Executing insert data queries")
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]