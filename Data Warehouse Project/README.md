# Background 
It is important for the startup, and indeed all businesses, to understand their consumer beahaviour in order to target products and services to the right audience as well as drive product improvement. Therefore, Sparkify needs to be able to analyse its user data and derive actinable insights. The most vital data include knowing what songs, artists and genres are in high demand, the demographics of its users as well as how to use this data to effectively grow their subscriber base.
# Project Objectives
This database helps the analytics team to achieve three main goals. Firstly, it allows them to access the trove of data that has hithertho been unaccesible. Secondly, given the design schema and type of database(relational) implemented, it allows them to do a wide range of analytical queries that can help current and future questions. Lastly, this database will also help them improve the efficiecy in which they are able to respond to their user behaviours which may provide a competitve advantage to Sparkify.
# Project Dataset
The project directory contains a data folder where the Sparkify's streaming data log and song data is located in json format. The 'sql_queries.py' file contains Postgres queries for creating and inserting data into tables. It also contains a query for selecting songs from the song table. The 'create_tables.py' file contains python script for executing the sql queries using the psycopg2 wrapper. The 'etl.ipynb' was used to develop an ETL process for the data while the 'etl.py' was used to load the entire dataset. The 'test.ipynb' contains sql code for testing the output of the code written to ascertain that the code actually does whatits supposed to do.
To run the python script, always ensure that the kernel is restarted when switching from file to file in order not to duplicate connection to the database.
# Database Schema
Database schema employed is the Star Schema with a fact Table (SongPlay) and four dimension tables (Users, Songs, Artists and Time). The ETL pipeline entails reading datasets in json format containing information about the played song (Song dataset) and an event long for user streaming (Log dataset). The star schema was chosen due to the ralatively straighforward nature of the available data as well as the simplicity involved in the design.
# Example Query
The following code can be used to display the most active listeners on the Sparkify platform and help tailor loyalty schemes/marketing materials to such customers.

    %%sql
    select user_id, count(user_id) as n, level
    FROM songplays
    group by user_id, level
    order by n desc
    LIMIT 10;


