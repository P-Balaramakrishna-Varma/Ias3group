## Stage 1
from kafka import KafkaProducer, KafkaConsumer
import json


producer = KafkaProducer(bootstrap_servers='localhost:9092')
log = { 'name': 'John', 'age': 30 }
producer.send("logs", json.dumps(log).encode('utf-8'))
producer.flush()



## Stage 2
# Create a node
# For each subsystem configuration, run the corresponding process.
# Configuration = Node : Id, path, command json with three keys.