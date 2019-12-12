from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

renga = SparkSession.builder.appName("ahamed").master("yarn-client").enableHiveSupport().getOrCreate()
appa= HiveContext(renga)

appa.sql("use jarvis")
amma= appa.sql('select * from crime3 where primary_type = "THEFT" limit 10')
amma.show()

