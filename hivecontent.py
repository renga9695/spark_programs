from pyspark.sql import SparkSession


appa = SparkSession.\
    builder.\
    appName("ahamed").\
    master("yarn-client").\
    config("hive.metastore.uris","thrift://rm01.itversity.com:9083").\
    config("spark.sql.warehouse.dir","hdfs://nn01.itversity.com:8020/apps/hive/warehouse/jarvis/").\
    enableHiveSupport().\
    getOrCreate()



amma= appa.sql('select * from crime3 limit 10')
amma.show()

