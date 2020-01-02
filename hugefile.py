from pyspark.sql import SparkSession

renga = SparkSession.builder.master("yarn-client").appName("rajagopal").getOrCreate()
amma=renga.read.json("/user/shashankbh/jarvis/git/*.json.gz")
amma.printSchema()
amma.show()