from pyspark.sql import SparkSession


appa = SparkSession.\
    builder.\
    appName("ahamed").\
    master("yarn-client").\
    config("hive.metastore.uris","thrift://rm01.itversity.com:9083").\
    config("spark.sql.warehouse.dir","hdfs://nn01.itversity.com:8020/apps/hive/warehouse/").\
    getOrCreate()



amma= appa.sql('select * from jarvis.crime3 where primary_type = "THEFT" limit 10')
amma.show()

