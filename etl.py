import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    """
    Gets all files in a directory.
    :param filepath (str): The file path to the directory
    """ 
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files

def process_song_file(cur, filepath):
    """
    Processes song files and inserts the data into the `songs` and `artists` tables.
    :param cur (psycopg2.cursor): A database cursor
    :param filepath(str): The file path to the song file
    """
    # open song file
    # song_dir = 'data/song_data'
    song_files = get_files(filepath)

    # extract song data
    for song_file in song_files:
        selected_song_data = {}
        with open(song_file) as f:
            data = pd.read_json(f, lines=True)
            for key in ['song_id', 'title', 'artist_id', 'year', 'duration']:
                selected_song_data[key] = data[key].values[0]
            all_song_data.append(selected_song_data)

    song_df = pd.DataFrame(all_song_data)
    song_data = song_df.values.tolist()
    
    # insert song record
    for i in range(len(song_data)):
        try:
            cur.execute(song_table_insert, song_data[i])
            conn.commit()
        except:
            print('Insert error at index: ', i)

    # extract artist record
    all_artist_data = []

    for song_file in song_files:
        selected_artist_data = {}
        with open(song_file) as f:
            song_data = pd.read_json(f, lines=True)
            for key in ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']:
                selected_artist_data[key] = song_data[key].values[0]
            all_artist_data.append(selected_artist_data)

    artist_df = pd.DataFrame(all_artist_data)
    artist_data = artist_df.values.tolist()

    # insert artist record
    for i in range(len(artist_data)):
        try:
            cur.execute(artist_table_insert, artist_data[i])
            conn.commit()
        except:
            print('Insert error at index: ', i)

def process_log_file(cur, filepath):
    """
    Processes log files and inserts the data into the `time`, `users`, and `songplays` tables.
    :param cur (psycopg2.cursor): A database cursor
    :param filepath(str): The file path to the log file
    """
    # open log file
    # log_dir = 'data/log_data'
    log_files = get_files(filepath)

    # extract log data
    all_log_data = []
    # flilter out records with page = 'NextSong'
    for log_file in log_files:
        with open(log_file) as f:
            data = pd.read_json(f, lines=True)
            data = data[data['page'] == 'NextSong']
            all_log_data.append(data)

    log_df = pd.concat(all_log_data)

    # convert timestamp column to datetime
    t = pd.to_datetime(log_df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i in range(len(time_df)):
        try:
            cur.execute(time_table_insert, time_data[i])
            conn.commit()
        except:
            print('Insert error at index: ', i)

    # insert user records
    user_df = log_df[['userId', 'firstName','lastName','gender','level']]
    user_data = user_df.values.tolist()
    for i in range(len(user_data)):
        try:
            cur.execute(user_table_insert, user_data[i])
            conn.commit()
        except:
            print('Insert error at index: ', i)

    # insert songplay records
    for index, row in log_df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
            conn.commit()
        except:
            print('Insert error at index: ', i)

def main():
    """
    Driver function for the ETL process.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_song_file(cur, filepath='data/song_data')
    process_log_file(cur, filepath='data/log_data')

    conn.close()

    if __name__ == "__main__":
        main()