from kafka import KafkaProducer, KafkaConsumer
import json


producer = KafkaProducer(bootstrap_servers='localhost:9092')
log = { 'Process': 'vm_manager', 'message': 'I have been run' }
producer.send("logs", json.dumps(log).encode('utf-8'))
producer.flush()

