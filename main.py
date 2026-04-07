from spark.spark_consumer import start_stream

# expose it explicitly
__all__ = ["start_stream"]

if __name__ == "__main__":
    start_stream()