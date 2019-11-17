from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import IntegerType,DoubleType
from pyspark.sql.functions import *

renga = SparkSession.builder.appName("poc").master("local").\
    getOrCreate()

amma_orders= renga.read.csv("E:\itversity\orders.csv",header="True")
amma_orders_items= renga.read.csv("E:\itversity\order_items.csv",header="True")
orders=amma_orders.withColumn("order_id",amma_orders.order_id.cast(IntegerType())).\
                    withColumn("order_customer_id",amma_orders.order_customer_id.cast(IntegerType()))
order_items= amma_orders_items.withColumn("order_item_id",amma_orders_items.order_item_id.cast(IntegerType())).\
                    withColumn("order_item_order_id",amma_orders_items.order_item_order_id.cast(IntegerType())).\
                    withColumn("order_item_quantity",amma_orders_items.order_item_quantity.cast(IntegerType())).\
                    withColumn("order_item_subtotal",amma_orders_items.order_item_subtotal.cast(DoubleType())).\
                    withColumn("order_item_product_price",amma_orders_items.order_item_product_price.cast(DoubleType()))

orders.createOrReplaceTempView("order_table")
order_items.createOrReplaceTempView("order_items_table")

total=orders.\
    where(orders.order_status.isin("COMPLETE","CLOSED")).\
    join(order_items,orders.order_id==order_items.order_item_order_id).\
    where(substring(orders.order_date,1,7)== "2014-06")

kowsi=total.groupBy(total.order_id).agg(sum(total.order_item_subtotal))
print(kowsi)
