from pyspark.sql import SparkSession


appa = SparkSession.\
    builder.\
    appName("ahamed").\
    master("yarn-client").\
    config("sspark.sql.warehouse.dir","hdfs://nn01.itversity.com:8020/apps/hive/warehouse/jarvis.db/").\
    getOrCreate()


appa.sql("use jarvis")
amma= appa.sql('select * from crime3 where primary_type = "THEFT" limit 10')
amma.show()

