from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
renga=SparkSession.builder.appName("streaming").master("local").getOrCreate()
amma=StreamingContext(renga,30)

amma.start()

ujesh = amma.socketTextStream("gw02.itversity.com",19999)
buddy= ujesh.flatMap(lambda fm : fm.split(" "))
partha= buddy.map(lambda m : (m,1))
myth= partha.reduceByKey(lambda x,y : x+y)
myth.pprint()
amma.awaitTermination()