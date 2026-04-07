from pyspark.sql.functions import when, col

def apply_decision(df):
    return df \
        .withColumn("risk_score",
            (col("pred_delay")/5)*60 +
            when(col("pred_profit") < 0, 40).otherwise(0)
        ) \
        .withColumn("decision",
            when((col("pred_delay") > 3) & (col("pred_profit") < 0), "Reject Order")
            .when(col("pred_delay") > 2, "Change Shipping Mode")
            .when(col("pred_profit") < 0, "Reduce Discount")
            .otherwise("Accept Order")
        )