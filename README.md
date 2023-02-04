### DEProject3_CloudDWH
## Purpose of Project

The scenario is to create a relational database for a music streaming platform called Sparkify, to help analytical teams gain access to cleaned and structured data. It is also building an ETL pipleline to add new data continously

A couple of key analytical goals that this project is focusing on:
1. Popularity of songs among different locations
2. User behaviors comparison between free and paid membership
3. User pattern for the users that change from free to paid or vice versa

## How to run Python Script

1. Fill 'dwh.cfg' file with necessary info from Redshift Cluster created
2. Run create_tables.py
3. Run etl.py

## Files

1. create_tables.py: this is the main Python file that contains the functions of creating tables in Postgre SQL server
2. etl.py: this is the main Python file that serves as the ETL pipeline and it converts the json files to structured dataframe then insert line by line into SQL database
3. sql_queries.py: this is the file that contains all the SQL scripts: create tables, drop tables, insert and song select
4. AWS_Redshift_ETL.ipynb: a sandbox for creating te etl.py file and testings of ETL functions were done here

## Schema and ETL

Star schema was chosen to simplify querying data

The ETL pipeline takes the song data and extract song and artist table. It takes the log data and extract other tables needed.
