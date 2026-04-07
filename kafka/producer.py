from kafka import KafkaProducer
import json
import pandas as pd
import time

df = pd.read_csv(r"C:\Study\Programming\PYTHON\og_python\BDA\supply_chain_dataset\DataCoSupplyChainDataset.csv", encoding='latin-1')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic = "supply_chain"

for _, row in df.head(10000).iterrows():  
    producer.send(topic, row.to_dict())
    print("Sent one row")
    time.sleep(1)