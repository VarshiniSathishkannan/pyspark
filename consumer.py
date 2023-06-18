from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'first_kafka_topic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

l = []
for message in consumer:
    message = message.value
    l.append(message)
    print('{} received'.format(message))