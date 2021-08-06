# Sparkify Analytics Using AWS
## Building a User Listening Data Pipeline on AWS Redshift
## Project Purpose
This project, developed as part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), was developed to help the fictional company Sparkify build a data pipeline on AWS Redshift.  Sparkify would like to analyze user listening habits on their platform and is especially interested in finding out which songs their users are listening to.

## Raw Data

The raw data that is used to build this pipeline consists of two types of files - one containing song data and one containing user listening logs. Both types of files are in .json format, and are stored in directories on Amazon S3.

### Song Files
Song files are nested in the project under the `song_data` directory in S3. Each file contains a single .json object. For example:

    {
        "num_songs": 1,
        "artist_id": "ARJIE2Y1187B994AB7",
        "artist_latitude": null,
        "artist_longitude": null,
        "artist_location": "",
        "artist_name": "Line Renaud",
        "song_id": "SOUPIRU12A6D4FA1E1",
        "title": "Der Kleine Dompfaff",
        "duration": 152.92036,
        "year": 0
    }

### Event Files
Events files consist of logs that are nested in the `log_data` directory. Each file contains .json objects on individual lines that represent a user action, along with information about the song played. For example:

    {
        "artist": "Des'ree",
        "auth": "Logged In",
        "firstName": "Kaylee",
        "gender": "F",
        "itemInSession": 1,
        "lastName": "Summers",
        "length": 246.30812,
        "level": "free",
        "location": "Phoenix-Mesa-Scottsdale, AZ",
        "method": "PUT",
        "page": "NextSong",
        "registration": 1540344794796.0,
        "sessionId": 139,
        "song": "You Gotta Be",
        "status": 200,
        "ts": 1541106106796,
        "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36\"",
        "userId": "8"
    }

The records that we care about in the context of this project are where `page` equals `NextSong`, as those are the records that represent a song being streamed by a user.

## Database
Since the purpose of this project is for analysis, a Data Warehouse design using a [Star Schema](https://en.wikipedia.org/wiki/Star_schema) is used to create the tables. The database being used for this is an [Amazon Redshift](https://aws.amazon.com/redshift/) cloud Data Warehouse.

### Schema Design
The schema design is broken up into the following 5 tables: `artists`, `songs`, `songplays`, `time` and `users`. The fact table in this design is the `songplays` table, which represents the action of a user selecting a song to play. The other 4 tables are dimension tables that represent details about the song play action, including information about the song, artist, time of selection and the user who selected it.

To take advantage of Redshift's distributed architecture, the distribution key for storage in each node is being set by the `song_id` column in the `songs` and `songplays` tables, as well on `artist_id` in the `artists` table. This choice was made because the analytics that are going to be run are primarily concerned with song play data along with a song's corresponding artist. Data in Redshift is being split across 4 nodes.

### AWS Automation

In order to quickly and automatically set up the AWS Redshift environment, the following steps can be taken:

1. In the AWS Management console, create a user with security credentials (Access Key ID and Secret).
2. Copy the Access Key ID, Secret and desired region into the `aws_credentials` file of the project. For example:

        [AWS]
        KEY=AA12345678GG...
        SECRET=AABBcDeFG1234567...
        REGION=us-west-2

3. In the terminal, run the `aws_redshift.py` script with the argument `setup`. This will create an IAM role, attach a Role Policy for access to S3, and create a Redshift cluster based on the values in `dwh.cfg`. For example:

        python3 aws_redshift.py setup

4. Run the command `aws_redshift.py` with the argument `status` to check on the status of the Redshift cluster creation. For example:

        python3 aws_redshift.py status

Wait until the output status says *available* and contains the Host value, like so:

        Status: available
        Host: sparkifydwhcluster.xxyyuunnbbvvpp.us-west-2.redshift.amazonaws.com

5. When the status is *available*, paste the *Host* value into `dwh.cfg` under `[CLUSTER][HOST]`.

6. Run the following script to get the Role ARN in order to access S3:

        python3 aws_redshift.py roleinfo

    Paste the *ARN* value into `dwh.cfg` under `[IAM_ROLE][ROLE_ARN]`.

5. Run the following command to open a TCP port so that the database can be accessed:

        python3 aws_redshift.py openport

### Creating Tables

In order to define the table structure within the database that was created in the above step, run the following script in the terminal:

    python3 create_tables.py

### ETL Process

Once the tables are created, run the following command to run the ETL process:

    python3 etl.py

Running this file will extract data from S3, load the data directly into staging tables, and then be transformed and inserted into the fact and dimension tables of the schema. After it has finished running, the database will be populated with data and ready to be queried for analytical purposes.

### Example Queries

The following are some example queries that an analyst may want to run once the data has been populated in the database.

***Querying the Top 5 Songs Played in 2018***

    SELECT
        sp.song_id,
	    sn.title,
	    sp.artist_id,
	    art.name,
	    tm.month,
	    tm.year,
	    count(sp.songplay_id) as plays
    FROM songplays sp
    INNER join songs sn on (sn.song_id = sp.song_id)
    INNER join artists art on (sn.artist_id = art.artist_id)
    INNER JOIN time tm on (sp.start_time = tm.start_time)
    WHERE tm.year = 2018
    GROUP BY
        sp.song_id,
        sn.title,
        sp.artist_id,
        art.name,
        tm.month,
        tm.year
    ORDER BY count(sp.songplay_id) desc
    LIMIT 5;

***Querying the Top 5 Artists Played in 2018***

    SELECT
        art.artist_id,
        art.name,
        count(sp.songplay_id) as plays
    FROM artists art
    INNER JOIN songplays sp on (art.artist_id = sp.artist_id)
    INNER JOIN time tm on (sp.start_time = tm.start_time)
    WHERE tm.year = 2018
    GROUP BY art.artist_id, art.name
    ORDER by plays desc LIMIT 5;

## Teardown

To delete the AWS Redshift environment altogether (and no longer be billed), run the following command:

    python3 aws_redshift.py teardown

## Project Requirements

AWS Account
Python 3 (Version Python 3.8.10 was used in development)

Python 3 Libraries

    psycopg2==2.9.1
    boto3==1.18.6
