from kafka import KafkaProducer
import json
import time
import random


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


for i in range(5):
    message = {
        "id": i,
        "timestamp": time.time(),
        "value": random.randint(100, 999)
    }
    producer.send('topic_out', value=message)
    print(f"📤 Отправлено: {message}")
    time.sleep(1)

producer.flush()
producer.close()
print("Все сообщения отправлены!")

