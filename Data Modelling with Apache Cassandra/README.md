# Background 
It is important for the startup, and indeed all businesses, to understand their consumer beahaviour in order to target products and services to the right audience as well as drive product improvement. Therefore, Sparkify needs to be able to analyse its user data and derive actinable insights. The most vital data include knowing what songs, artists and genres are in high demand, the demographics of its users as well as how to use this data to effectively grow their subscriber base.
# Project Objectives
This database helps the analytics team to achieve three main queries. Firstly, it allows them to know what songs was listened to during specific sessions. Secondly, it allows them to know what songs was listened to by unique users during each session.  Lastly, this database also helps them to know all the users who have listened to a particular song. Other queries can also be performed, even though the tables have not been specifically designed for them. 
# Project Dataset
The project directory contains a data folder where the Sparkify's streaming data is located in csv format. The 'Check for Unique Keys.ipnb' file contains code to check for unique composite keys for each of the tables in the database. Different combination of columns was checked to know which ones ones are unique.  The 'Project_2_Cassandra.ipnb' file contains python code for creating the database, tables and executing the code.

Note that due to the ipnb checkpoint files located in the original 'event_data' folder, the data was downloaded as a zip folder and then unzipped into the directory while excluding the ipnb checkpoint.  The 'event_datafile_new.csv' which contains the extracted data was created to run checks for unique composite keys. 

To run the python script, always ensure that the kernel is restarted when switching from file to file in order not to duplicate connection to the database.
# Database Schema
Database schema employed consists of several denormalised table so as to speed up query. The ETL pipeline entails reading datasets in csv format containing information about the song sessions (streaming dataset).
