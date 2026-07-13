from pyspark import pipelines as dp
from pyspark_datasources import OpenSkyDataSource

spark.dataSource.register(OpenSkyDataSource)

#to define our streaming table and create a table called ingest_flights
@dp.table  
def ingest_flights():
    return spark.readStream.format("opensky").load()  #it is a streaming table because of readstream() and not read()