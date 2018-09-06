import pyspark
from pyspark.sql.functions import udf, date_format, to_timestamp, col, desc, min, max
from pyspark.sql.types import BooleanType, IntegerType, DoubleType
from utils import geoutils

def drop_minutes(df, colName):
    fmt = "yyyy-MM-dd HH:00:00"
    return df.withColumn(colName,
                            to_timestamp(
                                date_format(col(colName), fmt),
                                "yyyy-MM-dd HH:mm:ss"))

def filter_numerical(df, colName):
    df = df.filter(col(colName).rlike(r'\d+(\.\d+)?'))
    return df.withColumn(colName, col(colName).cast(DoubleType()))

# extract weekday and hour from a datetime column
def extract_datetime(df, colName):
    return df.withColumn("weekday", date_format(col(colName),'u').cast(IntegerType()))\
                .withColumn("hour", date_format(col(colName), "H").cast(IntegerType()))\
                .drop(colName)

def clean_metar(metar):
    precip = metar.select('datetime', 'precip_in')
    precip = clean_precip(precip)
    temperature = metar.select('fahrenheit', 'datetime')
    temperature = clean_temperature(temperature)
    return temperature.join(precip, 'datetime')

def clean_precip(precip):
    if dict(precip.dtypes)['precip_in'] == 'string':
        precip = filter_numerical(precip, 'precip_in')
    precip = precip.filter(date_format(col('datetime'), "m") == 51)
    return drop_minutes(precip, 'datetime')    

def clean_temperature(temperature):
    if dict(temperature.dtypes)['fahrenheit'] == 'string':
        temperature = filter_numerical(temperature, 'fahrenheit')
    temperature = drop_minutes(temperature, 'datetime')
    temperature = temperature.groupby('datetime').mean()
    return temperature.withColumnRenamed('avg(fahrenheit)', 'fahrenheit')


is_in_nyc = udf(geoutils.is_in_nyc, BooleanType())
get_grid_x = udf(geoutils._get_grid_cell_x, IntegerType())
get_grid_y = udf(geoutils._get_grid_cell_y, IntegerType())

def clean_rides(rides):
    return rides.filter(is_in_nyc(rides.pickup_latitude,
                           rides.pickup_longitude) == True)

def add_grid_cols(rides):
    return rides.withColumn("grid_x",
                 get_grid_x(rides.pickup_longitude))\
                .withColumn("grid_y", get_grid_y(rides.pickup_latitude))\
                .drop('pickup_latitude')\
                .drop('pickup_longitude')

def get_counts(rides):
    return rides.groupby('pickup_datetime','grid_x','grid_y').count()
    
# takes an output of get_counts, and scales the counts to [0,1].
def normalize_counts(counted):
    minmax = counted.agg(max(col('count')))
    max_count = minmax.first()[0]
    return counted.withColumn('count_scaled', col('count')/max_count)

# takes an output of load_rides, and prepares it for a join with metar data.
def count_rides(rides):
    rides = clean_rides(rides)
    rides = add_grid_cols(rides)
    rides = drop_minutes(rides, 'pickup_datetime')
    return normalize_counts(get_counts(rides))

# takes outputs of count_rides and
def join_rides_metar(rides, metar):
    joined = rides.join(metar, rides.pickup_datetime == metar.datetime).drop('pickup_datetime')
    return extract_datetime(joined, 'datetime')
