from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys

hostname=(sys.argv[1])
port=int(sys.argv[2])
output=(sys.argv[3])

ab= SparkConf().setMaster("yarn-client").setAppName("departmentCount")
renga= SparkContext(conf=ab)
amma= StreamingContext(renga,10)
amma.start()
from operator import add
file= amma.socketTextStream(hostname,port)
appa=file.filter(lambda f: f.split(" ")[6].split("/"))
raja=appa.map(lambda m: (m.appa,1))
r_gopal= raja.reduceByKey(add)
r_gopal.saveAsTextFiles(output)
amma.awaitTermination()