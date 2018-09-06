
# coding: utf-8

# In[1]:


import pyspark
import time, datetime, os, pickle

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, OneHotEncoderEstimator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# In[2]:


spark = pyspark.sql.SparkSession.builder.appName("Rides Preprocessor").getOrCreate()

spark.sparkContext.addPyFile('sparkutils.zip')
from utils import sparkutils


# In[3]:


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
    # 2014
    colNames = map(lambda name: name.strip(), rides.columns)
    rides = rides.toDF(*colNames)
    return rides.select("pickup_datetime","pickup_latitude", "pickup_longitude")


# In[4]:


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
        
    # TODO: is there a random search module?
    # start with numTrees: [10, 100, 1000]
    # maxDepth: [10, 100, 1000]
    # minInstancesPerNode: [1, 10, 100, 1000]
    paramGrid = ParamGridBuilder()         .addGrid(rf.numTrees, grid_dict['numTrees'])         .addGrid(rf.maxDepth, grid_dict['maxDepth'])         .addGrid(rf.minInstancesPerNode, grid_dict['minInstancesPerNode'])         .build()
    
    train = df_2014
    dev, test = df_2015.randomSplit([0.5, 0.5)
    
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
    mean_error = pred.agg(pyspark.sql.functions.abs(col('count') - col('prediction')))
    error_stddev = pred.agg(stddev(col('count') - col('prediction')))
    
    
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


# In[ ]:


rides = sparkutils.count_rides(
    load_rides(get_ride_data(2014,1))
)
metar = sparkutils.clean_metar(
    load_metar(get_metar_data(2014,1))
)
for m in range(0): # TODO: replace range(1) with range(12)
    rides = rides.unionAll(
        sparkutils.count_rides(load_rides(get_ride_data(2014,m+2)))
    )
    metar = metar.unionAll(
        sparkutils.clean_metar(load_metar(get_metar_data(2014,m+2)))
    )
    
joined = sparkutils.join_rides_metar(rides,metar)


# In[ ]:


grid_dict = {'numTrees': [5],
             'maxDepth': [10],
             'minInstancesPerNode': [1,5]}
# cv=5, 2 parameter sets to search, 2 months => 29m32s
# cv=2, 2 parameter sets to search, 2 months => 11m40s
model, pred, rmse = rf_pipeline(joined, '1', grid_dict, 2)

