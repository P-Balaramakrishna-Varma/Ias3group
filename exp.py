from kafka import KafkaProducer, KafkaConsumer
consumer = KafkaConsumer('logs', bootstrap_servers='localhost:9092', auto_offset_reset='latest')
msg = next(consumer)