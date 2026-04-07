from pyspark.sql.functions import col

def add_features(df):
    return df.withColumn(
        "shipping_efficiency",
        col("Days for shipment (scheduled)") / (col("Days for shipment (scheduled)") + 1)
    )