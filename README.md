# Data_Modelling_PostgreSQL
 

## A- Dataset:
In this study, the Million Song Data Set data set was used. (A collection of free audio features and metadata for one million contemporary popular music tracks.) http://millionsongdataset.com/ 

## B- Project Scope: 

 

    - Apply data modeling with Postgres and create an ETL pipeline using Python, 

  

    - Defining fact and dimension tables for a star chart for a particular analytical focus, 

 

    - Writing an ETL pipeline that transfers data from files in two local directories to these tables in Postgres using Python and SQL. 

 
 
## C- Schema 
### Schema for Song Play Analysis 

Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables. You can use the Test_Read_df.ipnyb notebook for understanding the dataset values.

#### 1- Fact Table 

    songplays - records in log data associated with song plays i.e. records with page NextSong 

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 

#### 2- Dimension Tables 

   a- users - users in the app 

        user_id, first_name, last_name, gender, level 

   b- songs - songs in music database 

        song_id, title, artist_id, year, duration 

   c- artists - artists in music database 

        artist_id, name, location, latitude, longitude 

   d- time - timestamps of records in songplays broken down into specific units 

        start_time, hour, day, week, month, year, weekday 

## D- Project Steps 

Below are steps you can follow to complete the project: 

### Create Tables 

   - Write CREATE statements in sql_queries.py to create each table in the folder 1-Sql_queries_and_create_tables 

 

   - Write DROP statements in sql_queries.py to drop each table if it exists.Use sql_queries.py in the in the folder 1-Sql_queries_and_create_tables under Scirpt folder 

 

   - Run create_tables.py to create your database and tables. Use create_tables.py in in the folder 1-Sql_queries_and_create_tables under Scirpt folder 

 

   - Run test.ipynb to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook. Use test.ipnyb in the folder 3-Test under Script folder 

 

### Build ETL Processes 

Follow instructions in the etl.ipynb in the 2-ETL folder notebook to develop ETL processes for each table. At the end of each table section, or at the end of the notebook, run test.ipynb ( which is in the 3-Test folder)to confirm that records were successfully inserted into each table. Remember to rerun create_tables.py to reset your tables before each time you run this notebook. 

### Build ETL Pipeline 

Use what you've completed in etl.ipynb to complete etl.py ( which is in 2-ETL folder), where you'll process the entire datasets. Remember to run create_tables.py before running etl.py to reset your tables. Run test.ipynb to confirm your records were successfully inserted into each table. 

 
 

 
 

  

 