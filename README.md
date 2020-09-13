# Project: Data Warehouse

---

## Summary

---

As a Data Engineer of a music streaming app startup company named Sparkify, I was tasked to build an ETL pipeline to extract JSON data from S3 and load the data to Amazon Redshift. I was also tasked to transform the data into fact table and dimensional table so that the analytical team could query the data more effectively.


## How to run the Python scripts

---

To run the python scripts, first you must run the create_tables.py in your console to create the tables in the Redshift cluster. Next you must run the etl.py in your console to load the data from S3 to Redshift.


## Explanation of the files

---

#### sql_queries.py

This file drops and creates the tables to reset the data. It also has copy table queries to extract the JSON data from S3 and it has insert table queries to insert the extracted data into the dimensional tables to make the analytics team query the data easier.


#### create_tables.py

This file imports the sql_queries.py file. It executes the drop table and create table queries and connects to the Redshift cluster. By running this file in the console, you can make a connection to Redshift, and reset by dropping and creating tables in the cluster.


#### etl.py

THis file imports the sql_queries.py file. By running this file, you can connect to Redshift, load the log data and song data from S3 bucket to the former created tables(staging_events, staging_songs table) in Redshift. From the extracted data, it then transforms and inserts the data into the created fact table and dimensional tables.


