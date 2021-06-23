import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    '''
    Reads the song file whose filepath has been provided as an arugment and loads it to a dataframe object.
    '''
    
    df0 = pd.read_json(filepath, typ='DataFrame')
    df1 = df0.to_frame()
    df = df1.T 

    ''' 
    Extracts data from the song_data dataframe and inserts song table
    '''
    
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data[0])
    
    ''' 
    Extracts data from artist_data dataframe and inserts into the artist table
    '''
    
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data[0])


def process_log_file(cur, filepath):
    
    ''' 
    Opens songplay log data
    '''
    
    df = pd.read_json(filepath, lines= True)
    
    ''' 
    Filters the dataframe by "NextSong" action
    '''
    
    df = df[df['page']=='NextSong']
    
    ''' 
    Converts timestamp column to datetime
    '''
    
    t = pd.to_datetime(df['ts'], unit='ms')
    
    ''' 
    Inserts time data records
    '''
    
    time_data = [df['ts'],t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.day_name()]
    
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    
    time_dic = {}
    
    for i in range(len(time_data)):
        time_dic.update({column_labels[i]:time_data[i]})
    
    time_df = pd.DataFrame(time_dic)
    

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    ''' 
    Loads select columns from dataframe
    '''
    
    user_df = df[['userId','firstName','lastName','gender', 'level']]
    
    ''' 
    Inserts the selected columns into users table
    '''

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    ''' 
    Inserts songplay records
    '''

    for index, row in df.iterrows():
        
        ''' 
        Gets songid and artistid from song and artist tables
        '''
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        ''' 
        Inserts songplay record
        '''

        songplay_data = (row.ts,row.userId,row.level, songid, artistid,\
                         row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    ''' 
    Gets all files matching json extension from directory
    '''
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    ''' 
    Gets the total number of files found
    '''
    
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    ''' 
    iterates over files and processes them
    '''
    
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    ''' 
    Connects to sparkify database and create cursor object
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    ''' 
    Runs the functions defined above
    '''
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()