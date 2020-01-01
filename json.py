from pyspark.sql import SparkSession

renga = SparkSession.\
        builder.\
        appName("json_processing").\
        master("yarn-client").\
        getOrCreate()

amma= renga.read.json("/user/shashankbh/jarvis/git/2019-*.json.gz")
amma.show()