from typing import Type
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import doctest

plt.rcParams['figure.dpi'] = 300
plt.rcParams.update({'font.size': 7})

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
    data_index = {
        'TIME': 1,
        'PLATFORM': 3,
        'MSPLAYED': 4,
        'COUNTRY': 5,
        'NAME': 8,
        'ARTIST': 9,
        'ALBUM': 10,
        'URI': 11,
        'START': 15,
        'END': 16,
        'SHUFFLE': 17,
        'SKIP': 18,
        'INCOGNITO': 21
    }

    data = pd.read_json('data/' + filename)
    data_list = []

    for row_data in data.itertuples():
        if row_data[data_index['URI']]:
            row_data_dict = {'Timestamp': pd.to_datetime(row_data[data_index['TIME']]),
                             'Platform': str(row_data[data_index['PLATFORM']]),
                             'MsPlayed': int(row_data[data_index['MSPLAYED']]),
                             'Country': str(row_data[data_index['COUNTRY']]),
                             'TrackName': str(row_data[data_index['NAME']]),
                             'TrackAlbum': str(row_data[data_index['ALBUM']]),
                             'TrackArtist': str(row_data[data_index['ARTIST']]),
                             'URI': str(row_data[data_index['URI']]),
                             'StartReason': str(row_data[data_index['START']]),
                             'EndReason': str(row_data[data_index['END']]),
                             'Shuffle': bool(row_data[data_index['SHUFFLE']]),
                             'Skipped': bool(row_data[data_index['SKIP']]),
                             'Incognito': bool(row_data[data_index['INCOGNITO']])}

            data_list.append(row_data_dict)

    formatted_data = pd.DataFrame(data_list)

    return formatted_data


def import_streaming_history_set(filename_list: list[str]) -> pd.DataFrame:
    df_list = []

    for filename in filename_list:
        df_list.append(import_streaming_history(filename))

    streaming_history = pd.DataFrame(pd.concat(df_list))

    streaming_history.to_csv('out.csv', index=False)

    return streaming_history

#   ####################################################################################################################
#   #                                                 DATA PROCESSING                                                  #
#   ####################################################################################################################


def apply_utc_offset(hour):
    """
    Applies the UTC offset to a given hour of the day, returning the offset hour.

    >>> apply_utc_offset(0)
    16
    >>> apply_utc_offset(1)
    17
    >>> apply_utc_offset(8)
    24
    >>> apply_utc_offset(9)
    1

    :param hour:
    :return:
    """
    offset_time = hour + UTC_OFFSET

    if offset_time < 0:
        offset_time = 24 + offset_time

    return offset_time


def process_time_stamp(time_string: str) -> tuple[int, int, int, int, int]:
    """

    Examples:
    >>> process_time_stamp('2022-11-30T02:37:55Z')
    (2022, 11, 30, 2, 37)

    :param time_string:
    :return:
    """
    time_string = time_string[:-1]
    time_string = time_string.replace('T', '-')
    time_string = time_string.replace(':', '-')
    time_string = time_string.split('-')

    year = int(time_string[0])
    month = int(time_string[1])
    day = int(time_string[2])
    hour = apply_utc_offset(int(time_string[3]))
    minute = int(time_string[4])

    return year, month, day, hour, minute


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
    streaming_epoch = data['Timestamp'].min()
    plot_dict = {'Days': [],'Time of Day': []}

    for tup in data.itertuples(index=False):
        plot_dict.get('Days').append(tup[SONG_DATA_TS])
        plot_dict.get('Time of Day').append(get_minute_of_day(tup[SONG_DATA_TS]))

    print(plot_dict)

    plot_df = pd.DataFrame.from_dict(plot_dict)

    print(plot_df)
    plot_df.plot.scatter(x='Days', y='Time of Day', s=0.01)

    plt.show()


def get_minute_of_day(time_stamp: pd.Timestamp) -> int:
    return (time_stamp.hour * 60) + time_stamp.minute


def get_time_since(streaming_epoch: pd.Timestamp, time_stamp: pd.Timestamp) -> float:
    return (time_stamp - streaming_epoch).total_seconds()


########################################################################################################################
#                                                                                                                      #
#                                                NOTES AND TEST CODE                                                   #
#                                                                                                                      #
########################################################################################################################


analyze_streaming_history(import_streaming_history_set(STREAMING_DATA_PATH_LIST))

plot_listening_time(import_streaming_history_set(STREAMING_DATA_PATH_LIST))

