from pyspark.sql import SparkSession

renga = SparkSession.builder.appName("ahamed").master("yarn-client").enableHiveSupport().getOrCreate()

renga.sql("use jarvis")
amma= renga.sql("select * from crime3 where primary_type = 'THEFT' limit 10")
amma.show()

