from pyspark.sql import SparkSession

renga = SparkSession.\
        builder.\
        appName("json_processing").\
        master("yarn").\
        getOrCreate()

amma= renga.read.json("/user/shashankbh/jarvis/git/*")
amma.show()