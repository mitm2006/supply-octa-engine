from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import DoubleType
import joblib
import pandas as pd
import warnings
warnings.filterwarnings("ignore", message="X does not have valid feature names")

model        = joblib.load(r"C:\Study\Programming\PYTHON\og_python\BDA\model\shipping_delay_model.pkl")
feature_cols = joblib.load(r"C:\Study\Programming\PYTHON\og_python\BDA\model\feature_columns.pkl")
label_enc    = joblib.load(r"C:\Study\Programming\PYTHON\og_python\BDA\model\label_encoders.pkl")
scaler       = joblib.load(r"C:\Study\Programming\PYTHON\og_python\BDA\model\scaler.pkl")

def predict_delay(row):
    try:
        values = []
        for c in feature_cols:
            v = row[c]
            if c in label_enc:
                try:
                    v = int(label_enc[c].transform([str(v)])[0])
                except Exception:
                    v = -1
            else:
                try:
                    v = float(v) if v is not None else 0.0
                except Exception:
                    v = 0.0
            values.append(v)
        scaled = scaler.transform([values])
        return float(model.predict(scaled)[0])
    except Exception as e:
        print("Prediction error:", e)
        return None

delay_udf = udf(predict_delay, DoubleType())

def apply_prediction(df):
    return df \
        .withColumn(
            "pred_delay",
            delay_udf(struct(*feature_cols))
        ) \
        .withColumn(
            "pred_profit",
            col("Order Profit Per Order").cast(DoubleType())
        )