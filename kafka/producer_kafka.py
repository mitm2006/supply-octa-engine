from kafka import KafkaProducer
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092')
logs = ["INFO: OK", "WARNING: CPU High", "ERROR: Disk Fail"]

i = 0
while True:
    log = random.choice(logs)
    producer.send('test-topic', log.encode('utf-8'))
    print(f"Sent: {log}")
    time.sleep(1)
    



