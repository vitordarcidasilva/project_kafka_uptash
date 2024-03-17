
from kafka import KafkaProducer
import pandas as pd
import json



producer = KafkaProducer(
    bootstrap_servers='pet-eft-11457-us1-kafka.upstash.io:9092',
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_SSL',
    sasl_plain_username='cGV0LWVmdC0xMTQ1NyQyyYyQflKfRj9QfdWQoY2oXfVvzulQufnkNO9Nv_xHiFY',
    sasl_plain_password='YzAzZWI3M2UtYmIyYy00YzFlLWI3ZjEtMDZjOWVhMzJjMmI5',
    api_version_auto_timeout_ms=100000,    
)


tracks = pd.read_csv('albums.csv')

for dt in tracks.to_dict(orient='records'):
    data = json.dumps(dt).encode('utf-8')

    try:
        result = producer.send('artist', data).get(timeout = 60)    
        print("Message produced:", result)
    except Exception as e:
        print(f"Error producing message: {e}")
    finally:
        producer.close()
    
    break