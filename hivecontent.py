from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

renga = SparkSession.builder.appName("ahamed").master("yarn-client").enableHiveSupport().getOrCreate()
appa= SQLContext(renga)

appa.setConf("spark.sql.shuffle.partitions","2")

appa.sql("use jarvis")
amma= appa.sql('select * from crime3 where primary_type = "THEFT" limit 10')
amma.show()

