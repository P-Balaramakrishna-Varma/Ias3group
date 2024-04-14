from kafka import KafkaProducer, KafkaConsumer
import json, time


requests = [
    {
        'node_id': '0',
        'method': 'start_process',
        'args': {
            'config': {
                'name': 'process1',
                'path': '/home/sreejan/IAS/MLOps/vmManager/',
                'command': 'python3 vm_manager.py',
                }
            },
    },
    {
        'node_id': '0',
        'method': 'kill_process',
        'args': {
            'process_id': 'e77fb035-52c1-41c6-baba-027ce9c422cd'
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
    },
]


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='10.1.36.50:9092')
    for request in requests:
        # Sending the request
        request['timestamp'] = time.time()
        producer.send("AgentIn", json.dumps(request).encode('utf-8'))
        
        # Wait for the response
        consumer = KafkaConsumer("AgentOut", bootstrap_servers='10.1.36.50:9092', auto_offset_reset='earliest')
        for msg in consumer:
            try:
                val = json.loads(msg.value)
                if val['request'] == request:
                    print(val)
                    break
            except json.JSONDecodeError:
                pass
            except KeyError:
                pass
    producer.flush()