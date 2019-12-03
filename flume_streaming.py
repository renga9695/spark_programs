from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
import sys
from operator import add
from pyspark.sql import functions
hostname= sys.argv[1]
port= sys.argv[2]
renga = SparkConf().setMaster("yarn-client").setAppName("karthik")
amma= SparkContext(conf=renga)
appa=StreamingContext(amma,30)
addresses= [(hostname,port)]
rajagopal= FlumeUtils.createPollingStream(appa,addresses)
mohan= rajagopal.map(lambda m:m[1])
kowsi= mohan.flatMap(lambda fm : fm.split(" ")[6] == "deparment")
ujesh= kowsi.map(lambda m : (m.split(" ")[6].split("/")[1],1))
balaji=ujesh.reduceByKey(add)

balaji.saveAsTextFiles("hdfs://nn01.itversity.com/user/shashankbh/jarvis/data/flume/conf")

appa.start()
appa.awaitTermination()