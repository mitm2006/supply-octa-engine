from pyspark.sql.functions import expr, col

def apply_prediction(df):
    return df \
        .withColumn("pred_delay", expr("`Days for shipment (scheduled)` * 0.5")) \
        .withColumn("pred_profit", col("Order Profit Per Order"))