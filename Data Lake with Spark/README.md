# Background
This project aims to help Sparkify, a music steaming platform to utilise its streaming data for analytics purposes. Therefore, records obtained from user activities are extracted from a data lake- in this case an S3 storage- tranformed and loaded into another S3 bucket as parquet files. Parquet files are columnar storage which enables optimal storage and retrieval of data. This can then be used by Sparkify to analyse its user data and derive actionable insights. The most vital data, among others, include knowing what songs, artists and genres are in high demand, the demographics of its users as well as how to use this data to effectively grow their subscriber base.

# Project Objectives
This database helps the analytics team to achieve three main goals. Firstly, it allows them to access the trove of data that has hithertho been unaccesible. Secondly, given the design schema and type of database(relational) implemented, it allows them to do a wide range of analytical queries that can help current and future questions. Lastly, this database will also help them improve the efficiecy in which they are able to respond to their user behaviours which may provide a competitve advantage to Sparkify.

# Project Datasets and Files
The `dl.cfg` file contains AWS key Id and secret access key. This data should not be shared online for security purposes and hence has been removed from the uploaded file. This is used to authenticate one's identity on the Amazon AWS. The `etl.py` file contains the script used for executing the spark job. The script will import the needed dependencies and libraries, create tables with the required columns,read data from the S3 bucket (udacity-dend) and save it as parquet files in another bucket(kokobucket). The `data` folder contains a sample of the song and streaming log data that can be used for running sample/preliminary queries. 

# ETL Process
Sparkify's dataset are located in the "udacity-dend" AWS S3 bucket which contains its streaming log and song data, both in json format. Another S3 bucket, kokobucket, is created to save the tables as parquet files. Records are copied from udacity-dend bucket and cleaned and then stored as parquet tables in kokbucket. This tables can then be queried to answer analytics questions

# Database Schema
Database schema employed is the Star Schema with a fact Table (SongPlay) and four dimension tables (Users, Song, Artist and Time). The ETL pipeline entails reading datasets in json format containing information about the played song (Song dataset) and an event long for user streaming (Log dataset). The star schema was chosen due to the ralatively straighforward nature of the available data as well as the simplicity involved in the design.
Songplay tabele consists of start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, month and year columns .
Users table contains user_id, first_name, last_name, gender and level columns.
Time table contains start_time, hour, day, week, month, year and weekdays columns.
Song table consists of song_id, title, artist_id, year and duration columns.
Artist table contains artist_id, artist_name, artist_location, artist_latitude and artist_longitude.

# Running the Scripts
The `etl.py` file is run in either through an SSH connection from a CLI to an EMR cluster that has Spark running or directly from a notebook running from on the cluster via the AWS console. Either way, the spark job is submitted and the output from the job is either written to a file or viewed directly. The Spark WebUI can be used to view the DAG and different stages as well as the run time for each individual task. This can be analysed to optimise query time as well as know which tasks are taking longer time to run. It can also be used to troubleshoot errors through the log reports. 

# Example Query
The following code can be used to display the most active listeners on the Sparkify platform and help tailor loyalty schemes/marketing materials to such customers.

    %%sql
    select user_id, count(user_id) as n, level
    FROM songplays
    group by user_id, level
    order by n desc
    LIMIT 10;