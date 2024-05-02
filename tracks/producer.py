
from kafka import KafkaProducer
import pandas as pd
import json

producer = KafkaProducer(
    bootstrap_servers='mutual-shrimp-13505-us1-kafka.upstash.io:9092',
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_SSL',
    sasl_plain_username='dW5pZmllZC1hbnQtMTA3MTYkItO4Rn2B-30afeSuzZ0XhMIBoBcGJflfedqUQsE',
    sasl_plain_password='YjVhM2RmODAtNzY0YS00MmUxLWI2NjEtYjcxNTg5MDI3M2Ji',
    api_version_auto_timeout_ms=100000,    
)

tracks = pd.read_csv('track.csv')

for dt in tracks.to_dict(orient='records'):
    data = json.dumps(dt).encode('utf-8')

    try:
        result = producer.send('tracks', data).get(timeout = 60)    
        print("Message produced:", result)
    except Exception as e:
        print(f"Error producing message: {e}")
producer.close()