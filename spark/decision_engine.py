from pyspark.sql.functions import when, col, round

def apply_decision(df):
    return df \
        .withColumn(
            "clean_delay",
            round(when(col("pred_delay") < 0, 0).otherwise(col("pred_delay")), 2)
        ) \
        .withColumn(
            "delay_score",
            when(col("clean_delay") > 5, 1.0) 
            .otherwise(col("clean_delay") / 5)
        ) \
        .withColumn(
            "profit_flag",
            when(col("pred_profit") < 0, 1).otherwise(0)
        ) \
        .withColumn(
            "risk_score",
            round((col("delay_score") * 60) + (col("profit_flag") * 40), 2)
        ) \
        .withColumn(
            "decision",
            when((col("clean_delay") > 3) & (col("pred_profit") < 0), "Reject Order")
            .when(col("clean_delay") > 2, "Change Shipping Mode")
            .when(col("pred_profit") < 0, "Reduce Discount")
            .otherwise("Accept Order")
        )