from pyspark.sql.types import *
import pandas as pd
import joblib

feature_cols = joblib.load(r"C:\Study\Programming\PYTHON\og_python\BDA\model\feature_columns.pkl")

df_sample = pd.read_csv(
    r"C:\Study\Programming\PYTHON\og_python\BDA\supply_chain_dataset\DataCoSupplyChainDataset.csv",
    encoding='latin-1'
)

def generate_schema(df):
    schema = StructType()
    for col, dtype in zip(df.columns, df.dtypes):
        if dtype == 'int64':
            schema.add(col, IntegerType())
        elif dtype == 'float64':
            schema.add(col, DoubleType())
        else:
            schema.add(col, StringType())
    return schema

schema = generate_schema(df_sample)