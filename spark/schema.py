from pyspark.sql.types import *

schema = StructType() \
    .add("Days for shipment (scheduled)", IntegerType()) \
    .add("Order Item Quantity", IntegerType()) \
    .add("Sales per customer", DoubleType()) \
    .add("Order Item Discount", DoubleType()) \
    .add("Order Profit Per Order", DoubleType()) \
    .add("Shipping Mode", StringType()) \
    .add("Category Name", StringType()) \
    .add("Customer Segment", StringType())