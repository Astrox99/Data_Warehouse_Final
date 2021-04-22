# Data Warehouse with Redshift project

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My role is to building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms
data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

There are two datasets that need to be extracted:
    - **Song dataset**: It contains metadata about a song and artist of the song.
    - **Log dataset**: It contains activity logs of users.
    
They both are in .JSON files.

##### A Star schehma is required in this project.

###### Fact table:
    1. songplays - Records in log data associated with song plays

###### Dimension Tables
    2. users - Users in the app
    3. songs - Songs in music database
    4. artists - Artists in music database
    5. time - timestamps of records in songplays broken down into specific units

###### The following staging tables are also requied to be used for loading data from S3.
    - Staging_events: Extracting data from Log dataset
    - Staging_songs: Extracting data from Song dataset


#### Project Steps:

##### Create table Schemas in Redshift:
    1. Write CREATE statements in sql_queries.py to create each table.
    2. Write DROP statements in sql_queries.py to drop each table if it exists.
    3. Lunch a Redshift cluster and create am IAM role that has read access to S3
    4. Add Redshift database and create an IAM role that has read access to S3.
    5. Run create_tables.py to establish a connection with Redshift and create table schemas in Redshift database.

##### Build ETL pipeline
    1. Run etl.py to do the following tasks:
        - Load data from S3 to staging tables on Redshift.
        - Load data from staging tables to analytics tables on Redshift.
    2. Delete the Redshift cluster when fisnished to save cost.

