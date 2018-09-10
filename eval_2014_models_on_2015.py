import pyspark
import time, datetime, os, pickle

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, OneHotEncoderEstimator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


spark = pyspark.sql.SparkSession.builder.appName("Rides Preprocessor").getOrCreate()

spark.sparkContext.addPyFile('sparkutils.zip')
from utils import sparkutils

def get_ride_data(year, month, size='tiny'):
    if size in ['tiny','small','mid']:
        return 'gs://nyc-taxi-8472/yellow_tripdata_{0}-{1:02d}_{2}.csv'.format(year, month, size)
    elif size == 'full':
        return 'gs://nyc-taxi-8472/yellow_tripdata_{0}-{1:02d}.csv'.format(year, month, size)
    else:
        raise "size={0} is not a supported value.".format(size)

def get_metar_data(year, month):
    #return f'/home/atkm/lga_{year}-{month:02}.csv'
    #return 'file:///home/atkm/taxi-demand-prediction/data/lga_{0}-{1:02d}.csv'.format(year, month)
    return 'gs://nyc-taxi-8472/lga_{0}-{1:02d}.csv'.format(year, month)


def read_csv(path):
    return spark.read.format("csv")      .option("header", "true")      .option("inferSchema", "true")      .load(path)

def load_metar(metarPath):
    metar = read_csv(metarPath)
    metar = metar.select("valid","tmpf", " p01i") # note whitespace in p01i
    return metar.withColumnRenamed('valid', 'datetime')        .withColumnRenamed('tmpf', 'fahrenheit')        .withColumnRenamed(' p01i', 'precip_in')

def load_rides(ridesPath):
    rides = read_csv(ridesPath)
    # 2015
    if 'tpep_pickup_datetime' in rides.schema.names:
        rides = rides.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')
    # 2014
    colNames = map(lambda name: name.strip(), rides.columns)
    rides = rides.toDF(*colNames)
    return rides.select("pickup_datetime","pickup_latitude", "pickup_longitude")


# df = rides and metar joined.
# model_type = '1', '2', or '3'.
def rf_pipeline(df_2014, df_2015, model_type, grid_dict, numFolds=5):
    
    rf = RandomForestRegressor(
        featuresCol='features',
        labelCol='count_scaled'
    )
    
    # Build pipeline.
    if model_type == '1':
        assembler = VectorAssembler(
            inputCols = ['grid_x', 'grid_y', 'weekday', 'hour', 'fahrenheit', 'precip_in'],
            outputCol = 'features'
        )
        pipeline = Pipeline(
            stages=[assembler, rf]
        )
    elif model_type == '2':
        encoder = OneHotEncoderEstimator(
            inputCols=['weekday', 'hour'],
            outputCols=['weekday_vec', 'hour_vec']
        )
        assembler = VectorAssembler(
            inputCols = ['grid_x', 'grid_y', 'weekday_vec', 'hour_vec', 'fahrenheit', 'precip_in'],
            outputCol = 'features'
        )
        pipeline = Pipeline(
            stages=[encoder, assembler, rf]
        )
    elif model_type == '3':
        encoder = OneHotEncoderEstimator(
            inputCols=['weekday','hour','grid_x','grid_y'],
            outputCols=['weekday_vec','hour_vec','grid_x_vec','grid_y_vec']
        )
        assembler = VectorAssembler(
            inputCols = ['grid_x_vec', 'grid_y_vec', 'weekday_vec', 'hour_vec', 'fahrenheit', 'precip_in'],
            outputCol = 'features'
        )
        pipeline = Pipeline(
            stages=[encoder, assembler, rf]
        )
    else:
        raise ValueError("Model type must be either 1, 2, or 3.")
        
    paramGrid = ParamGridBuilder()         .addGrid(rf.numTrees, grid_dict['numTrees'])         .addGrid(rf.maxDepth, grid_dict['maxDepth'])         .addGrid(rf.minInstancesPerNode, grid_dict['minInstancesPerNode'])         .build()
    
    train = df_2014
    dev, test = df_2015.randomSplit([0.5, 0.5])
    
    evaluator = RegressionEvaluator(
        labelCol='count_scaled', predictionCol='prediction', metricName='rmse'
    )

    crossval = CrossValidator(estimator=pipeline,
                         estimatorParamMaps=paramGrid,
                         evaluator=evaluator,
                         numFolds=numFolds)
    
    model = crossval.fit(train)
    pred_on_test = model.transform(test)
    rmse_on_test = evaluator.evaluate(pred_on_test)
    # consider returning mean error and its stddev as well
    #mean_error = pred_on_test.agg(pyspark.sql.functions.abs(col('count') - col('prediction')))
    #error_stddev = pred_on_test.agg(stddev(col('count') - col('prediction')))
    
    
    # get metric values with model.avgMetrics,
    # and parameters with model.getEstimatorParamMaps
    return model, pred_on_test, rmse_on_test

def get_model_stats(model):
    
    def _parse_params(params):
        for p, val in params.items():
            yield (p.name, val)
            
    pmaps = model.getEstimatorParamMaps()
    pmaps = [list(_parse_params(params)) for params in pmaps]
    return list(zip(pmaps, model.avgMetrics))

def get_ride_data_year(year,size):
    rides = sparkutils.count_rides(
        load_rides(get_ride_data(year, 1, size))
    )
    for m in range(2,13):
        rides = rides.unionAll(
                sparkutils.count_rides(load_rides(get_ride_data(year, m, size)))
                )
    return rides

def get_metar_data_year(year):
    metar = sparkutils.clean_metar(
        load_metar(get_metar_data(year, 1))
    )
    for m in range(2,13):
        metar = metar.unionAll(
                sparkutils.clean_metar(load_metar(get_metar_data(year, m)))
                )
    return metar


size = 'tiny' #TODO: change to small
rides_2014 = get_ride_data_year(2014, size)
metar_2014 = get_metar_data_year(2014)
rides_2015 = get_ride_data_year(2015, size)
metar_2015 = get_metar_data_year(2015)
joined_2014 = sparkutils.join_rides_metar(rides_2014, metar_2014)
joined_2015 = sparkutils.join_rides_metar(rides_2015, metar_2015)

# TODO: change to numTrees: [200]; maxDepth: [30]
grid_dict = {'numTrees': [5],
             'maxDepth': [10],
             'minInstancesPerNode': [1]}
# cv=5, 2 parameter sets to search, 2 months => 29m32s
# cv=2, 2 parameter sets to search, 2 months => 11m40s
time_start = time.time()
model, pred, rmse = rf_pipeline(joined_2014, joined_2015, '1', grid_dict, 2) #TODO: change 2 -> 5.
time_end = time.time()
time_spent = int(time_end - time_start)

# Save results
dirname = 'rfmodel_' + str(int(time.mktime(datetime.datetime.today().timetuple())))
os.mkdir(dirname)
with open(dirname + '/results.txt', 'w') as f:
    f.write("Data: train year=2014, test/dev year=2015, size={0}\n".format(size))
    f.write("Time (in seconds): {0}\n = {1}h{2:02d}m\n".format(time_spent, time_spent // 3600, (time_spent % 3600) // 60))
    for params in get_model_stats(model):
        for x in params:
            f.write(str(x))
            f.write(' - ')
        f.write('\n')
    f.write('Test rmse: ' + str(rmse) + '\n')

model.bestModel.save(dirname + '.model')
print("Result saved in {0}/, and model in hdfs as {0}.model".format(dirname))
