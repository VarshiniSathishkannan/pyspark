from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

# value serializer is for converting the int or other datatypes to a stream of bytes
# Kafka sends only stream of bytes
# json.dumps() function will convert a subset of Python objects into a json string. 
# Not all objects are convertible and you may need to create a dictionary of data you wish to expose before serializing to JSON
# Passing the Python dictionary to json.dumps() function will return a string. 
# The encode() method encodes the string, using the specified encoding. 
# If no encoding is specified, UTF-8 will be used.

for e in range(1000):
    data = {'number' : e}
    producer.send('first_kafka_topic', value=data)
    sleep(5)
