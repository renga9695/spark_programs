from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,DoubleType
renga= SparkSession.builder.master('yarn-client').appName("Demo").getOrCreate()

renga_orders = renga.read.csv("/user/shashankbh/jarvis/data/orders/part-00000").toDF("order_id","order_date","order_customer_id" ,"order_status")
renga_orders = renga_orders.withColumn("order_id",renga_orders.order_id.cast(IntegerType())).withColumn("order_customer_id",renga_orders.order_customer_id.cast(IntegerType()))
renga_order_items = renga.read.csv("/user/shashankbh/jarvis/data/order_items/part-00000").toDF("order_item_id","order_item_order_id","order_item_product_id" ,"order_item_quantity","order_item_subtotal","order_item_product_price")
renga_order_items = renga_order_items.withColumn("order_item_subtotal",renga_order_items.order_item_subtotal.cast(DoubleType())).withColumn("order_item_product_price",renga_order_items.order_item_product_price.cast(DoubleType()))
renga_orders.printSchema()
renga_order_items.show()