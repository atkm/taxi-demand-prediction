import re
import pandas as pd
from utils import geoutils

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
