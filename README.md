# Project: Data Lake with Spark
## Introduction
In this project, we are using spark to implemented a data lake. The data lake implementation consist of three steps as listed below.
1.Load unstructured logs and songs data sets from s3
2.processed the data andthen put the processed data to spark tables
3.write back the spark tables in s3 back.

## Technologies
Python, Spark SQL, Spark DataFrame, AWS, Data Lake, ETL pipeline, JSON.

## Design

### Database Schema
Since we have a simple queries, the star schema has been choosen. This schema is the simplest styl of data mart schema.
Five Tables have been designed, one Fact Table and four Dimension Tables.
![Database ER Diagramm](pictures/DB.PNG)

#### Fact Table
1. **songplays**
+ columns: songplay_id, user_id, level, song_id, artist_id, session_id, location, user_agent, start_time, month,year

#### Dimension Tables
1. **users** - users in the app
+ columns: user_id, first_name, last_name, gender, level

2. **songs** - songs in music database
+ columns: song_id, title, artist_id, year, duration

3. **artists** - artists in music database
+ columns: artist_id, name, location, latitude, longitude

4. **time** - timestamps of records in songplays broken down into specific units
+ columns: start_time, hour, day, week, month, year, weekday

### ETL Pipeline
Spark-Pyton has been used to read the songs and logs data set from S3, to transforme the read data, and store the processed data in Spark tables.
And finaly write the tables back in S3

## Implementation

### Input/Output Data
+ The input data (log and song data) are located in AWS S3 
+ The processed Data have been writen back to the AWS S3 

### File: etl.py
This file contains python fuctions for processing songs data and logs data. By running this file songs and logs data will be read from the song_data and log_data located in S3 (see input data), processed and store in S3.

### File: dl.cfg
This file contains AWS Access Credentials.

## Run the application
Before running the files in items (1) and (2) make sure that the redshift (cluster) database is available.
The parameters to set the aws resources and clients are in the file dwl.cfg available
1. offen a Terminal
3. Run the file *etl.py* : in the Terminal write *python etl.py*
