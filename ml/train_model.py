import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib

df = pd.read_csv("../data/DataCoSupplyChainDataset.csv", encoding='latin-1')

features = [
    'Days for shipment (scheduled)',
    'Order Item Quantity',
    'Sales per customer',
    'Order Item Discount'
]

X = df[features]
y_delay = df['Order Profit Per Order']

model = RandomForestRegressor()
model.fit(X, y_delay)

joblib.dump(model, "model.pkl")