from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
    'topic_in',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Ожидание сообщений из incoming_messages")
for message in consumer:
    print(f"Получено сообщение: {message.value}")

