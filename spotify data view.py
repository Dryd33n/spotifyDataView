import math
from typing import Type
import pandas as pd
import matplotlib.pyplot as plt
from time import perf_counter
import numpy as np
import doctest

plt.rcParams['figure.dpi'] = 300
plt.rcParams.update({'font.size': 3})
plt.style.use('dark_background')

# Data Frame Column Indexes
SONG_DATA_TS = 0
SONG_DATA_PLAT = 1
SONG_DATA_MS = 2
SONG_DATA_COUNTRY = 3
SONG_DATA_NAME = 4
SONG_DATA_ALBUM = 5
SONG_DATA_ARTIST = 6
SONG_DATA_URI = 7
SONG_DATA_START = 8
SONG_DATA_END = 9
SONG_DATA_SHUFFLE = 10
SONG_DATA_SKIP = 11
SONG_DATA_INCOGNITO = 12

# USER PARAMS
STREAMING_DATA_PATH_LIST = ['Streaming_History_Audio_2017-2021_0.json',
                            'Streaming_History_Audio_2021-2022_1.json',
                            'Streaming_History_Audio_2022-2023_3.json',
                            'Streaming_History_Audio_2022_2.json',
                            'Streaming_History_Audio_2023_4.json',
                            'Streaming_History_Audio_2023_5.json']
UTC_OFFSET = -8

########################################################################################################################
#                                                                                                                      #
#                                                   DATA IMPORT                                                        #
#                                                                                                                      #
########################################################################################################################


def import_streaming_history(filename: str) -> pd.DataFrame:
    print(f'Importing data: {filename}')
    start = perf_counter()

    data = pd.read_json('data/' + filename)

    data.drop(columns=['username',
                       'ip_addr_decrypted',
                       'user_agent_decrypted',
                       'episode_name',
                       'episode_show_name',
                       'spotify_episode_uri'],
              inplace=True)

    data.rename(columns={'ts':                                 'Timestamp',
                         'platform':                           'Platform',
                         'ms_played':                          'MsPlayed',
                         'conn_country':                       'Country',
                         'master_metadata_track_name':         'TrackName',
                         'master_metadata_album_artist_name':  'TrackAlbum',
                         'master_metadata_album_album_name':   'TrackArtist',
                         'spotify_track_uri':                  'URI',
                         'reason_start':                       'StartReason',
                         'reason_end':                         'EndReason',
                         'shuffle':                            'Shuffle',
                         'skipped':                            'Skipped',
                         'offline':                            'Offline',
                         'offline_timestamp':                  'OfflineTimestamp',
                         'incognito_mode':                     'Incognito'},
                inplace=True)

    # Time Stamp Cleaning
    data = clean_timestamps(data)

    end = perf_counter()
    print(f'Successfully imported data, took {(end-start):.2f} seconds')

    return data


def clean_timestamps(data):
    data['Timestamp'] = data['Timestamp'].map(pd.to_datetime)
    data['OfflineTimestamp'] = data['OfflineTimestamp'].map(lambda ts: pd.to_datetime(ts).tz_localize('UTC'))
    data['Timestamp'] = data.apply(lambda row: combine_timestamps(
        row['Timestamp'], row['OfflineTimestamp']), axis=1)

    data.drop(columns=['OfflineTimestamp'], inplace=True)

    return data


def combine_timestamps(timestamp: pd.Timestamp, offline_timestamp: pd.Timestamp) -> pd.Timestamp:
    timestamp.floor('D')
    time_delta = pd.Timedelta((offline_timestamp.hour * 60 * 60) +
                              (offline_timestamp.minute * 60) +
                              offline_timestamp.second)

    return timestamp + time_delta


def import_streaming_history_set(filename_list: list[str]) -> pd.DataFrame:
    df_list = []

    for filename in filename_list:
        df_list.append(import_streaming_history(filename))

    streaming_history = pd.DataFrame(pd.concat(df_list))

    streaming_history.to_csv('out.csv')

    return streaming_history

########################################################################################################################
#                                                                                                                      #
#                                                   DATA ANALYSIS                                                      #
#                                                                                                                      #
########################################################################################################################


def most_played_songs(count: int, streaming_data: pd.DataFrame):
    print(f"""

        ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃           TOP SONGS            ┃ 
        ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

    Displays the total top {count} most played songs

    """)

    print(streaming_data['TrackName'].value_counts().nlargest(count))
    print("\n\n")


def most_played_artists(count: int, streaming_data: pd.DataFrame):
    print(f"""

        ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃          TOP ARTISTS           ┃ 
        ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

    Displays the total top {count} most played artists

    """)

    print(streaming_data['TrackArtist'].value_counts().nlargest(count))
    print("\n\n")


def most_played_albums(count: int, streaming_data: pd.DataFrame):
    print(f"""

        ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃           TOP ALBUMS           ┃ 
        ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

    Displays the total top {count} most played albums

    """)

    print(streaming_data['TrackAlbum'].value_counts().nlargest(count))
    print("\n\n")


def total_listen_time(streaming_data: pd.DataFrame):
    time_played_ms = streaming_data['MsPlayed'].sum()

    print("""

        ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃      TOTAL LISTENING TIME      ┃ 
        ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

    Displays the total time spent listening on spotify in hours.
    
    """)

    print(f'Total Listen Time: {(time_played_ms * (2.777778 * 10 ** -7)):.2f} hours\n\n')


def favourite_listening_times(streaming_data: pd.DataFrame):
    hour_ms_played_dict = {}
    hour_percent_played_dict = {}

    for i in range(0, 24):
        hour_ms_played_dict[i] = 0

    for row_data in streaming_data.itertuples(index=False):
        hour_played = row_data[SONG_DATA_TS].hour
        ms_played = row_data[SONG_DATA_MS]

        hour_ms_played_dict[hour_played] += int(ms_played)

    total_time = sum(hour_ms_played_dict.values())

    for key in hour_ms_played_dict:
        hour_percent_played_dict[key] = float((hour_ms_played_dict.get(key) / total_time)*100)

    print("""
    
        ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃   FAVOURITE LISTENING TIMES    ┃ 
        ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
    
    Displays the percentage of time spent listening for each
    hour of the day. 
    """)

    for tup in hour_percent_played_dict.items():
        if tup[0] > 12:
            print(f'{tup[0]-12}pm: {tup[1]:.2f}%')
        else:
            print(f'{tup[0]}am: {tup[1]:.2f}%')

    print('\n\n')


def analyze_streaming_history(data: pd.DataFrame):
    most_played_songs(10, data)
    most_played_albums(10, data)
    most_played_artists(10, data)
    total_listen_time(data)
    favourite_listening_times(data)


########################################################################################################################
#                                                                                                                      #
#                                                 PLOTTING FUNCTIONS                                                   #
#                                                                                                                      #
########################################################################################################################


def plot_listening_time(data: pd.DataFrame):
    plot_df = data[['Timestamp']].copy()
    plot_df['TimeOfDay'] = plot_df.map(get_second_of_day)

    plot_df.plot.scatter(x='Timestamp', y='TimeOfDay', s=0.01)
    ax = plt.subplot()

    ax.set_yticks([0,           14400,   28800,    43200,     57600,    72000,    86400],
                  ['12:00 PM', '4:00AM', '8:00AM', '12:00AM', '4:00PM', '8:00PM', '12:00PM'])

    plt.show()


def get_second_of_day(time_stamp: pd.Timestamp) -> int:
    return (time_stamp.hour * 60 * 60) + (time_stamp.minute * 60) + time_stamp.second


########################################################################################################################
#                                                                                                                      #
#                                                NOTES AND TEST CODE                                                   #
#                                                                                                                      #
########################################################################################################################


# analyze_streaming_history(import_streaming_history_set(STREAMING_DATA_PATH_LIST))


# plot_listening_time(import_streaming_history_set(STREAMING_DATA_PATH_LIST))
# plot_listening_time(import_streaming_history('Streaming_History_Audio_2017-2021_0.json'))
plot_listening_time(import_streaming_history('short_data.json'))

# import_streaming_history('short_data.json')
# import_streaming_history('Streaming_History_Audio_2017-2021_0.json')

