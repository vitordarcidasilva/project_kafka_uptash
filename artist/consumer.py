from kafka import KafkaConsumer
import pandas as pd
import json

consumer = KafkaConsumer(
    'artist',
    bootstrap_servers='pet-eft-11457-us1-kafka.upstash.io:9092',
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_SSL',
    sasl_plain_username='cGV0LWVmdC0xMTQ1NyQyyYyQflKfRj9QfdWQoY2oXfVvzulQufnkNO9Nv_xHiFY',
    sasl_plain_password='YzAzZWI3M2UtYmIyYy00YzFlLWI3ZjEtMDZjOWVhMzJjMmI5',
    group_id='YOUR_CONSUMER_GROUP',
    auto_offset_reset='earliest'
)

DATA = []
try:
    for message in consumer:
        DATA.append(json.loads(message.value))
        if len(DATA)>=1:
            df = pd.DataFrame(DATA)
            df.to_csv('data.csv',index=False)
            DATA = []
except KeyboardInterrupt:
    pass
finally:
    consumer.close()