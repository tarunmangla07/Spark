# Introduction
The user base of a music streaming startup called as ``sparkify`` has grown tremendously and due to which they want their processes and data on to cloud.
The entire data resides in Amazon S3  in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The company wants to build an ETL pipeline that extracts the S3 data, processes them with Spark and then loads the data back to S3 into a set of dimension tables to easily get insights about the songs their users are listening to.

## Data Sets
* Log Data - staging table to store log data 
    * Columns - artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId.

* Song Data - staging table to store song data
    * Columns -  num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year.

# Star Schema
In the star schema, we have a ``Fact Table `` called as ``songplays`` and various ``dimension Tables`` such as ``users``, ``songs``, ``àrtists`` and ``time``

## Fact Table
* songplays -  records in log data associated with song plays
    * Columns - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Table
* users - users in the app
    * Columns - user_id, first_name, last_name, gender, level
* songs - songs in music database
    * Columns - song_id, title, artist_id, year, duration
* artists - artists in music database
    * Columns - artist_id, name, location, latitude, longitude
* time - timestamp of records in songplays broken down into specific units
    * Columns - start_time, hour, day, week, month, year, weekday
   
# ETL pipeline 

The ETL pipeline consists loads the data stored in S3 processes them with Spark and then loads the data back to S3 into a set of Fact and dimension tables as parquet files.

# How to get Started
Please follow the below steps in order to get the project running:
* Make sure the project consists of the following files:
    * ``dl.cfg`` - configuration file containing all the aws related information.
    * ``etl.py`` - Script which loads the data from S3 and then processes with Spark and loads the data to Fact and dimension tables as parquet files.
* when all the files are in place, now we are ready to run the project as:
    * First insert AWS key and secret access key in ``dl.cfg``
    * Now Run ``etl.py`` script to loads the data stored in S3 processes them with Spark and then loads the data back to S3 into a set of Fact and dimension tables as parquet files.