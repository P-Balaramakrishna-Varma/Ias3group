from kafka import KafkaProducer, KafkaConsumer
import json, time


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
    },
    {
        'node_id': '0',
        'method': 'kill_process',
        'args': {
            'process_id': '1'
            },
    },
    {
        'node_id': '0',
        'method': 'reset_process',
        'args': {
            'process_id': '1'
            },
    },
    {
        'node_id': '0',
        'method': 'get_health',
    },
    {
        'node_id': '0',
        'method': 'get_processes',
    }
]


def kafka_rpc(topic, request):
        # Sending the request
        request['timestamp'] = time.time()
        producer.send(topic + 'In', json.dumps(request).encode('utf-8'))
        producer.flush()
        
        # Wait for the response
        consumer = KafkaConsumer(topic + "Out", bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
        for msg in consumer:
            try:
                val = json.loads(msg.value)
                if val['request'] == request:
                    return val
            except json.JSONDecodeError:
                pass
            except KeyError:
                pass


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    for request in requests:
       val = kafka_rpc("Agent", request)
       print(val)
    producer.flush()