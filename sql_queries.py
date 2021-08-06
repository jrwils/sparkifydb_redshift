import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(128),
        auth VARCHAR(16),
        firstName VARCHAR(128),
        gender VARCHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR(128),
        length NUMERIC(10, 5),
        level VARCHAR(4),
        location VARCHAR(256),
        method VARCHAR(5),
        page VARCHAR(16),
        registration NUMERIC,
        sessionId INTEGER,
        song VARCHAR(256),
        status INTEGER,
        ts BIGINT,
        userAgent TEXT,
        userId INTEGER
    );
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(18),
        artist_latitude NUMERIC(10, 5),
        artist_longitude NUMERIC(10, 5),
        artist_location VARCHAR(256),
        artist_name VARCHAR(256),
        song_id VARCHAR(18),
        title VARCHAR(256),
        duration NUMERIC(10, 5),
        year INTEGER
    );
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL,
        first_name VARCHAR(128),
        last_name VARCHAR(128),
        gender VARCHAR(1),
        level VARCHAR(4),
        PRIMARY KEY(user_id)
    );
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(18) NOT NULL,
        name VARCHAR(256),
        location VARCHAR(256),
        latitude NUMERIC(10, 5),
        longitude NUMERIC(10, 5),
        PRIMARY KEY(artist_id)
    ) DISTKEY(artist_id);
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(18) NOT NULL,
        title VARCHAR(256) NOT NULL,
        artist_id VARCHAR(18) NOT NULL,
        year INT,
        duration NUMERIC(10, 5),
        PRIMARY KEY(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    ) DISTKEY(song_id);
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL UNIQUE,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL,
        PRIMARY KEY(start_time)
    );
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0, 1) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(4),
        song_id VARCHAR(18) NOT NULL,
        artist_id VARCHAR(18) NOT NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR(256),
        user_agent TEXT,
        PRIMARY KEY(songplay_id),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
        FOREIGN KEY(start_time) REFERENCES time(start_time)
    ) DISTKEY(song_id) SORTKEY(start_time);
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events FROM '{}'
    iam_role '{}'
    json 'auto ignorecase';
    """).format(
        config.get('S3', 'LOG_DATA'),
        config.get('IAM_ROLE', 'ROLE_ARN')

    )

staging_songs_copy = (
    """
    COPY staging_songs FROM '{}'
    iam_role '{}'
    json 'auto ignorecase';
    """).format(
        config.get('S3', 'SONG_DATA'),
        config.get('IAM_ROLE', 'ROLE_ARN')
    )

# FINAL TABLES

user_table_insert = (
    """
    INSERT INTO users (
        SELECT se.userId, firstName, lastName, gender, uts_levels.level
        FROM staging_events se
        -- joined subqueries are here
        -- in order to get the most recent 'level' value
        INNER JOIN (
            SELECT userId, max(ts) as mts from staging_events
            group by userId
        ) max_ts on (max_ts.userId = se.userId)
        INNER JOIN (
            SELECT userId, ts, level from staging_events
        ) uts_levels on (
            max_ts.userId = uts_levels.userId
            and max_ts.mts = uts_levels.ts
        )
        GROUP BY se.userId, firstName, lastName, gender, uts_levels.level
        ORDER by se.userId
    );
    """
)

artist_table_insert = (
    """
    INSERT into artists (
        SELECT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
        GROUP BY
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    );
    """
)

song_table_insert = (
    """
    INSERT into songs (
        SELECT song_id, title, artist_id, year, duration
        FROM staging_songs
        GROUP BY song_id, title, artist_id, year, duration
    );
    """
)

time_table_insert = (
    """
    INSERT into time (
    SELECT (timestamp 'epoch' + ts * interval '.001 seconds') as start_time,
        EXTRACT(
            HOUR from (timestamp 'epoch' + ts * interval '.001 seconds')
        ) as hour,
        EXTRACT(
            DAY from (timestamp 'epoch' + ts * interval '.001 seconds')
        ) as day,
        EXTRACT(
            WEEK from (timestamp 'epoch' + ts * interval '.001 seconds')
        ) as week,
        EXTRACT(
            MONTH from (timestamp 'epoch' + ts * interval '.001 seconds')
        ) as month,
        EXTRACT(
            YEAR from (timestamp 'epoch' + ts * interval '.001 seconds')
        ) as year,
        EXTRACT(
            DOW from (timestamp 'epoch' + ts * interval '.001 seconds')
        ) as weekday
    FROM staging_events
    GROUP BY start_time, hour, day, week, month, year, weekday
    );
    """
)

songplay_table_insert = (
    """
    INSERT into songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) (
    SELECT (timestamp 'epoch' + ts * interval '.001 seconds') as start_time,
        userId,
        level,
        sng.song_id,
        art.artist_id,
        sessionId,
        se.location,
        userAgent
     FROM staging_events se
     INNER JOIN artists art on (se.artist = art.name)
     INNER JOIN songs sng on (art.artist_id = sng.artist_id)
     WHERE se.page = 'NextSong'
    );
    """
)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    time_table_drop,
    song_table_drop,
    artist_table_drop,
    user_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    user_table_insert,
    artist_table_insert,
    song_table_insert,
    time_table_insert,
    songplay_table_insert,
]
