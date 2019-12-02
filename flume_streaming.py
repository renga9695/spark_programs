from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
import sys
from operator import add
from pyspark.sql import functions
hostname= sys.argv[1]
port= sys.argv[2]
output=sys.argv[3]
renga = SparkConf().setMaster("local").setAppName("karthik")
amma= SparkContext(conf=renga)

appa=StreamingContext(amma,30)
rajagopal= FlumeUtils.createStream(appa,hostname,port)

kowsi= rajagopal.flatMap(lambda fm : fm.split(" ")[6] == "deparment")
ujesh= kowsi.map(lambda m : (m.split(" ")[6].split("/")[1],1))
balaji=ujesh.reduceByKey(add)

balaji.saveAsTextFiles(output)