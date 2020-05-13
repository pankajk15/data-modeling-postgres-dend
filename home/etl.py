import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    This funcation reads song data from JSON file when filepath is passed as parameter and
    loads it into a dataframe, and insert the relevant data into songs and artists DB tables   
    
    Input:
    * cur the cursor variable
    * filepath the path of the song file
    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = song_data = tuple(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist())
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = tuple(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist())
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function reads log data from JSON file when filepath filepath is passed as parameter and
    loads it into a dataframe, filters the 'NextSong' action, and inserts relevant attributes 
    into time, users and songplays DB tables 
    
    Input:
    * cur the cursor variable
    * filepath the path of the song file
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts'] = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = ([df['ts'], 
              df['ts'].dt.hour, 
              df['ts'].dt.day, 
              df['ts'].dt.weekofyear, 
              df['ts'].dt.month,
              df['ts'].dt.year, 
              df['ts'].dt.weekday])
    
    column_labels = (['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    time_dict = dict((k,v) for (k,v) in zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts,unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function takes filepath and user funcation as parameter, locates and iterates
    over the files located in the filepath and applies the user specified function.
    
    Input:
    * cur the cursor variable
    * filepath the path of the song file
    * func user defined function to be applied to each file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Connects to sparkifydb DB, creates cursor and passes this to the process_data function
    Closes the connection to database at the end
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()