# Sparkify DataLake


## Contents
* [Summary](#Summary)
* [Data Modelling](#Data-modelling)
* [Project structure](#Project-structure)
* [How to run](#How-to-run)
--------------------------------------------

#### Summary
This project aims to power the analytics of song plays for a music streaming app, Sparkify.

Spark is used to Extract, load and transform the data located in s3 into tables forming
a datalake.

--------------------------------------------
#### Data Modelling

Unlike DWHs, Spark-based Datalakes are not limited by relations and allow
a wide range of analytics on the row data. Therefore, optimizing
queries and analytics is not confined to traditinoal techniques in DWHs
such as using starschemas. However, we adopt in this project a starschema-like
database since it powers the analytics related to the songs played by
the users in a good way. Since we are using a DataLake, the user is always
able to query the data in its original form.


Considering the song dataset we can divide the fields into two groups. One group of fields concern the artist. Namely, 
“artist_id”, “artist_location”, etc… The other group of fields concern the song. Namely, “song_id”, “duration”, etc…

Similarly, looking at the log data, we recognize other cateogries of info. Most importantly, in the timestamped logdata 
we can discern each song play (defined as a record with page = next). We also find data related to the session 
(session_id, itemsinSession, etc... ) and the users(e.g. userAgent, userId, etc...).


Each record for a songplay needs to be indexed with a new field called songplay_id which will serve as a primary key for our central fact table.
 
Based on our user requirements, session data will not be required. As for all other categories, this project includes the code to create 
the tables and the ETL pipelines powering storing them in a starschema.

-------------------------
#### Project structure

* <b> etl.py </b> - A script defining the ETL process
* <b> dl.cfg </b> - A configuration file containing AWS credentials


#### How to run

1. SSH to the master node of AWS EMR spark cluster
2. use spark-submit to run the the script. e.g. 
`
/usr/bin/spark-sbumit --master yarn ./etl.py
`

