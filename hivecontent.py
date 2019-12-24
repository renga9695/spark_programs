from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

appa = SparkSession.\
    builder.\
    appName("ahamed").\
    master("yarn-client").\
    config("hive.metastore.uris","thrift://rm01.itversity.com:9083").\
    config("spark.sql.warehouse.dir","hdfs://nn01.itversity.com:8020/apps/hive/warehouse/jarvis/").\
    enableHiveSupport().\
    getOrCreate()


amma= SQLContext(appa)
amma.sql("use jarvis")
rajagopal= amma.sql("select primary_type,year from crime where year = '2017'")
rajagopal.show()