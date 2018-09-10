import pandas as pd
from utils import geoutils
import re
import os
from datetime import datetime

# Returns the path to the rides data for the given year-month.
# year and month should be ints.
def get_rides_data(year, month, size='tiny'):
    if size == 'full':
        fname = f'yellow_tripdata_{year}-{month:02}.csv'
    else:
        fname = f'yellow_tripdata_{year}-{month:02}_{size}.csv'
    return open(os.path.join(os.path.dirname(__file__), f'../data/{fname}'))

def get_metar_data(year, month):
    fname = f'lga_{year}-{month:02}.csv'
    return open(os.path.join(os.path.dirname(__file__), f'../data/metar_data/{fname}'))

# e.g. csv='../data/yellow_tripdata_2016-01_small.csv'
# Returns a DataFrame containing all rides within the bounds defined in geoutils.
# Its columns are 'pickup_datetime', 'pickup_latitude', 'pickup_longitude'.
def read_rides(csv):
    try:
        fname = csv.split('/')[-1]
    except:
        fname = csv.name
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
        # the column names in 2014 data are padded by a space
    elif year in ['2014']:
        cols_orig = [' ' + colname for colname in ['pickup_datetime', 'pickup_longitude', 'pickup_latitude']]
    elif year in [str(y) for y in range(2010, 2014)]:
        cols_orig = ['pickup_datetime', 'pickup_longitude', 'pickup_latitude']
    elif year in ['2009']:
        cols_orig = ['Trip_Pickup_DateTime', 'Start_Lon', 'Start_Lat']
    df = pd.read_csv(csv, usecols=cols_orig)
    
    # change column names to ['pickup_datetime', 'pickup_longitude', 'pickup_latitude']
    if year in ['2015','2016']:
        df = df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime'})
    elif year in ['2014']:
        df = df.rename(columns = lambda colName: colName.strip())
    elif year in [str(y) for y in range(2010, 2014)]:
        pass
    elif year in ['2009']:
        df = df.rename(columns = {'Trip_Pickup_DateTime': 'pickup_datetime', 'Start_Lon': 'pickup_longitude', 'Start_Lat': 'pickup_latitude'})

    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    return _clean_rides(df)

# take a df with pickup_{latitude,longitude} columns, and add grid_{x,y} columns.
def _add_grid_cols(df):
    df['grid_x'] = df.pickup_longitude.apply(geoutils._get_grid_cell_x)
    df['grid_y'] = df.pickup_latitude.apply(geoutils._get_grid_cell_y)
    return df

# the same as geoutils.is_in_nyc, but optimized for pd.DataFrame
def in_nyc_mask(df):
    lon_in_nyc = (df.pickup_longitude >= geoutils.LON_WEST) & (df.pickup_longitude <= geoutils.LON_EAST)
    lat_in_nyc = (df.pickup_latitude >= geoutils.LAT_SOUTH) & (df.pickup_latitude <= geoutils.LAT_NORTH)
    return lon_in_nyc & lat_in_nyc

def _clean_rides(df):
    #in_nyc = df[['pickup_latitude','pickup_longitude']].apply(
    #        lambda row: geoutils.is_in_nyc(*row), axis=1)
    return df[in_nyc_mask(df)]

# takes a raw (an output of read_rides) DataFrame, and counts the number of rides in each grid cell.
def counts_by_grid_cell(df):
    df = _add_grid_cols(df)
    count = df.groupby(['grid_x', 'grid_y']).size()
    return count

def extract_hour_weekday(df):
    df['weekday'] = df['pickup_datetime'].dt.weekday
    df['hour'] = df['pickup_datetime'].dt.hour
    return df

def read_metar(csv):
    usecols = ['valid', 'tmpf', ' p01i'] # p01i has a whitespace in its name
    df = pd.read_csv(csv, usecols=usecols)
    df.columns = ['datetime', 'fahrenheit', 'precip_in']
    df['datetime'] = pd.to_datetime(df['datetime'])

    # precipitation and temperature each has its own processing logic; need to work on them separately.
    precip = df[['datetime','precip_in']]
    # TODO: consolidate this filtering with fahrenheit
    if precip['precip_in'].dtype == 'object':
        pat = r'\d+(\.\d+)?'
        precip = precip[precip.precip_in.str.match(pat)]
        precip['precip_in'] = precip.precip_in.astype('float')


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
    # Some months contain strings in the fahrenheit column. e.g. 'M' in 2014-10.
    if fahrenheit['fahrenheit'].dtype == 'object':
        pat = r'\d+(\.\d+)?'
        fahrenheit = fahrenheit[fahrenheit.fahrenheit.str.match(pat)]
        fahrenheit['fahrenheit'] = fahrenheit.fahrenheit.astype('float')

    fahrenheit['datetime'] = pd.to_datetime(fahrenheit['datetime'].dt.strftime("%Y-%m-%d %H"))
    fahrenheit_avg = fahrenheit.groupby(['datetime']).mean()

    # Warning: with the TODO above not fixed, the inner join will drop some records.
    weather = pd.merge(precip, fahrenheit_avg, on='datetime', how='inner')
    print("Warning: read_metar is not fully developed. Some records may be improperly dropped.")

    # TODO: do the same kind of assert as above.
    #assert weather.shape[0] == 24, "Something is wrong at the join. There isn't one record for each hour."

    return weather

# group by datetime (resampled by the hour) and grid, followed by count()
def get_counts(rides_df):
    rides_df['pickup_datetime'] = pd.to_datetime(rides_df.pickup_datetime.dt.strftime("%Y-%m-%d %H"))
    rides_df = _add_grid_cols(rides_df).drop(['pickup_latitude', 'pickup_longitude'], axis=1)
    counts = rides_df.groupby(['pickup_datetime', 'grid_x', 'grid_y']).size()
    return counts.reset_index(name='count')

def scale_counts(df):
    df['count_scaled'] = df['count']/df['count'].max()
    return df

# Joins rides and metar DataFrames (outputs of read_{rides,metar})
# The columns of the DataFrame are ['weekday', 'hour', 'grid_x', 'grid_y', 'fahrenheit', 'precip_in', 'count'].
def join_rides_metar(rides_df, metar_df):
    counts = get_counts(rides_df)
    df = pd.merge(counts, metar_df, left_on='pickup_datetime', right_on='datetime', how='inner')
    df['weekday'] = df.datetime.dt.weekday
    df['hour'] = df.datetime.dt.hour
    df = df.drop(['datetime'], axis=1)
    return df
    
# Given an output DataFrame of join_rides_metar, converts it to the numpy
# format (datetime, location, and weather) that a sklearn model takes.
def extract_ml_features(joined_df):
    features = ['weekday', 'hour', 'grid_x', 'grid_y', 'fahrenheit', 'precip_in']
    X = joined_df[features].values
    y = joined_df['count'].values
    return (X, y)
