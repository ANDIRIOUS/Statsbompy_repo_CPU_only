from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'statsbomb-eventos',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='test-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for messages...")
for message in consumer:
    print(f"Received: {message.value}")
    break # Just need one
