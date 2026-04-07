import pandas as pd
import numpy as np
import joblib

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor

df = pd.read_csv(r"C:\Study\Programming\PYTHON\og_python\BDA\supply_chain_dataset\DataCoSupplyChainDataset.csv", encoding='latin-1')

print("Initial Shape:", df.shape)

df.drop_duplicates(inplace=True)
drop_cols = [
    'Order Zipcode', 'Customer Zipcode', 'Product Description',
    'Customer Email', 'Customer Password', 'Order Id'
]
df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)

for col in df.select_dtypes(include=['object']).columns:
    df[col].fillna("Unknown", inplace=True)

for col in df.select_dtypes(include=['int64', 'float64']).columns:
    df[col].fillna(df[col].median(), inplace=True)

df['shipping_delay'] = df['Days for shipping (real)'] - df['Days for shipment (scheduled)']

df.drop(columns=[
    'Days for shipping (real)',
    'Days for shipment (scheduled)',
    'Delivery Status'
], inplace=True, errors='ignore')

label_encoders = {}

for col in df.select_dtypes(include=['object']).columns:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    label_encoders[col] = le

X = df.drop(columns=['shipping_delay'])
y = df['shipping_delay']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

scaler = StandardScaler()

X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

model = RandomForestRegressor(
    n_estimators=100,
    max_depth=15,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print("\nModel Performance:")
print("MAE :", mae)
print("RMSE:", rmse)
print("R2  :", r2)

joblib.dump(model, r"C:\Study\Programming\PYTHON\og_python\BDA\model\shipping_delay_model.pkl")
joblib.dump(scaler, r"C:\Study\Programming\PYTHON\og_python\BDA\model\scaler.pkl")
joblib.dump(label_encoders, r"C:\Study\Programming\PYTHON\og_python\BDA\model\label_encoders.pkl")
joblib.dump(X.columns.tolist(), r"C:\Study\Programming\PYTHON\og_python\BDA\model\feature_columns.pkl")

print("\nModel and preprocessing objects saved successfully!")