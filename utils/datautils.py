import pandas as pd
from utils import geoutils
import re
from datetime import datetime

# returns the path to the rides data for the given year-month.
def get_rides_data(year, month):
    pass

def get_metar_data(year, month):
    pass

# e.g. csv='../data/yellow_tripdata_2016-01_small.csv'
def read_rides(csv):
    fname = csv.split('/')[-1]
    ym_regex = re.search(r'(\d{4})-(\d{2})', fname)
    year = ym_regex.group(1)
    assert 2009 <= int(year) and int(year) <= 2017
    month = ym_regex.group(2)
    assert 1 <= int(month) and int(month) <= 12

    if year == '2017' or (year == '2016' and int(month) >= 7):
        raise RuntimeError('Pickup location format has changed since 2016-07.\
            The new format is not supported.')

    if year in ['2015','2016']:
        cols_orig = ['tpep_pickup_datetime', 'pickup_longitude', 'pickup_latitude']
    elif year in ['2014']:
        # the column names in 2014 data are padded by a space
        cols_orig = [' ' + colname for colname in ['pickup_datetime', 'pickup_longitude', 'pickup_latitude']]
    df = pd.read_csv(csv, usecols=cols_orig)
    
    # change column names to ['pickup_datetime', 'pickup_longitude', 'pickup_latitude']
    if year in ['2015','2016']:
        df = df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime'})
    if year in ['2014']:
        df = df.rename(columns = lambda colName: colName.strip())

    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    return df

# take a df with pickup_{latitude,longitude} columns, and add grid_{x,y} columns.
def add_grid_cols(df):
    df['grid_x'] = df.pickup_longitude.apply(geoutils._get_grid_cell_x)
    df['grid_y'] = df.pickup_latitude.apply(geoutils._get_grid_cell_y)
    return df

def clean_rides(df):
    in_nyc = df[['pickup_latitude','pickup_longitude']].apply(
            lambda row: geoutils.is_in_nyc(*row), axis=1)
    return df[in_nyc]

def read_metar(csv):
    usecols = ['valid', 'tmpf', ' p01i'] # p01i has a whitespace in its name
    df = pd.read_csv(csv, usecols=usecols)
    df.columns = ['datetime', 'fahrenheit', 'precip_in']
    df['datetime'] = pd.to_datetime(df['datetime'])

    # precipitation and temperature each has its own processing logic; need to work on them separately.
    precip = df[['datetime','precip_in']]

    # Precip info usually comes at the 51st minute of each hour.
    # If not, look for the nearest minute.
    # Another complication: the last record of a day is sometimes included in the file for the next day.
    #   e.g. "2013-12-31 23:51:00" is in 2014-01.csv.
    # These issues are currently ignored.
    # TODO: Ensure there is one precip record for each hour.
    # Take into account the issues above.
    #precip['month_day'] = precip.datetime.dt.strftime("%m/%d")
    #precip.groupby('month_day').apply(lambda grp: sum(grp.datetime.dt.minute == 51) == 24)

    precip = precip[precip['datetime'].dt.minute == 51]
    # Drop the minute information so the datetime format matches that of fahrenheit_avg.
    precip['datetime'] = pd.to_datetime(precip['datetime'].dt.strftime("%Y-%m-%d %H"))

    # Take the average of temperature records in each hour.
    fahrenheit = df[['datetime','fahrenheit']]
    # drop the minute information so records in the same hour are put in the same group.
    fahrenheit['datetime'] = pd.to_datetime(fahrenheit['datetime'].dt.strftime("%Y-%m-%d %H"))
    fahrenheit_avg = fahrenheit.groupby(['datetime']).mean()

    # Warning: with the TODO above not fixed, the inner join will drop some records.
    weather = pd.merge(precip, fahrenheit_avg, on='datetime', how='inner')
    print("Warning: read_metar is not fully developed. Some records may be improperly dropped.")

    # TODO: do the same kind of assert as above.
    #assert weather.shape[0] == 24, "Something is wrong at the join. There isn't one record for each hour."

    return weather

# Given rides and metar DataFrames, returns a DataFrame and a Series from which
# numpy arrays appropriate for model training can be obtained by calling value() on them.
# The columns of the DataFrame are ['weekday', 'hour', 'grid_x', 'grid_y', 'fahrenheit', 'precip_in'], and the Series contains counts.
def prep_for_ml(rides_df, metar_df):
    rides = clean_rides(rides_df)
    rides['join_datetime'] = pd.to_datetime(rides.pickup_datetime.dt.strftime("%Y-%m-%d %H"))
    rides = add_grid_cols(rides).drop(['pickup_latitude', 'pickup_longitude', 'pickup_datetime'], axis=1)
    counts = rides.groupby(['join_datetime', 'grid_x', 'grid_y']).size()
    counts = counts.reset_index(name='count')

    df = pd.merge(counts, metar_df, left_on='join_datetime', right_on='datetime', how='inner')
    df['weekday'] = df.datetime.dt.weekday
    df['hour'] = df.datetime.dt.hour
    df = df.drop(['join_datetime', 'datetime'], axis=1)
    features = ['weekday', 'hour', 'grid_x', 'grid_y', 'fahrenheit', 'precip_in']
    X = df[features]
    y = df['count']
    return (X, y)

