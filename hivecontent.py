from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

appa = SparkSession.\
    builder.\
    appName("ahamed").\
    master("yarn-client").\
    config("hive.metastore.uris","thrift://rm01.itversity.com:9083").\
    config("spark.sql.warehouse.dir","hdfs://nn01.itversity.com:8020/apps/hive/warehouse/jarvis/").\
    config("hive.exec.dynamic.partition.mode","nonstrict").\
    config("hive.exec.dynamic.partition","true").\
    config("hive.support.concurrency","true").\
    config("hive.txn.manager","org.apache.hadoop.hive.ql.lockmgr.DbTxnManager").\
    enableHiveSupport().\
    getOrCreate()


amma= SQLContext(appa)
amma.sql("use jarvis")
rajagopal= amma.sql("select * from crime3")
mohan=rajagopal.write.mode("Append").partitionBy("yeared").insertInto("appa",overwrite=False)
mohan.show()