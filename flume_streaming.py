from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.flume import FlumeUtils
from operator import add

amma = SparkSession().builder.appName("karthik").master("yarn-client").getOrCreate()
appa=StreamingContext(amma,30)
addresses= [("gw02.itversity.com",12354)]
rajagopal= FlumeUtils.createPollingStream(appa,addresses,storageLevel=StorageLevel.MEMORY_AND_DISK_2)
mohan= rajagopal.map(lambda m:m[1])
kowsi= mohan.flatMap(lambda fm : fm.split(" ")[6] == "deparment")
ujesh= kowsi.map(lambda m : (m.split(" ")[6].split("/")[1],1))
balaji=ujesh.reduceByKey(add)

balaji.saveAsTextFiles("hdfs://nn01.itversity.com/user/shashankbh/jarvis/data/flume/conf")

appa.start()
appa.awaitTermination()