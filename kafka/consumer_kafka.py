from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='group1'
)

for message in consumer:
    msg = message.value.decode('utf-8')
    if "ERROR" in msg or "WARNING" in msg:
        print(f"ALERT: {msg}")


