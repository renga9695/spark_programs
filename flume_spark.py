from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
import sys

host= sys.argv[1]
port =sys.argv[2]

renga = SparkSession().builder.master("yarn-client").appName("buddy").getOrCreate()
ssc= StreamingContext(renga,30)

address= [(host,port)]
polling = FlumeUtils.createStream(ssc,address)

