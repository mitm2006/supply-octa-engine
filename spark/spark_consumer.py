from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from spark.schema import schema
from spark.feature_engineering import add_features
from spark.prediction import apply_prediction
from spark.decision_engine import apply_decision


def format_output(df, epoch_id):
    rows = df.collect()

    for row in rows:
        print("\n===== SUPPLY CHAIN ANALYSIS =====")

        print(f"Sales: {row['Sales per customer']}")
        print(f"Quantity: {row['Order Item Quantity']}")
        print(f"Category: {row['Category Name']}")
        print(f"Customer Segment: {row['Customer Segment']}")

        print("\n----- PREDICTION -----")
        print(f"Predicted Delay: {row['pred_delay']:.2f} days")
        print(f"Predicted Profit: ${row['pred_profit']:.2f}")

        print("\n----- RISK ANALYSIS -----")
        print(f"Risk Score: {row['risk_score']}")

        print("\n----- FINAL DECISION -----")
        print(f"Decision: {row['decision']}")

        print("\n=================================\n")


def start_stream():
    spark = SparkSession.builder \
        .appName("SupplyChainStreaming") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "supply_chain") \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    df_features = add_features(df_parsed)
    df_pred = apply_prediction(df_features)
    df_final = apply_decision(df_pred)

    query = df_final.writeStream \
        .foreachBatch(format_output) \
        .outputMode("append") \
        .start()

    query.awaitTermination()