from kafka import KafkaProducer, KafkaConsumer
import json


requests = [
    {
        'node_id': '0',
        'method': 'start_process',
        'args': {
            'config': {
                'name': 'process1',
                'command': 'python3 process1.py',
                'env' : {'key1': 'value1', 'key2': 'value2'}
                }
            },
        'response_topic': 'logs',
    },
    {
        'node_id': '0',
        'method': 'kill_process',
        'args': {
            'process_id': '1'
            },
        'response_topic': 'logs',
    },
    {
        'node_id': '0',
        'method': 'reset_process',
        'args': {
            'process_id': '1'
            },
        'response_topic': 'logs',
    },
    {
        'node_id': '0',
        'method': 'get_health',
        'response_topic': 'logs',
    },
    {
        'node_id': '0',
        'method': 'get_processes',
        'response_topic': 'logs',
    }
]

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    for request in requests:
        consumer = KafkaConsumer("logs", bootstrap_servers='localhost:9092')
        producer.send("agent", json.dumps(request).encode('utf-8'))
    producer.flush()