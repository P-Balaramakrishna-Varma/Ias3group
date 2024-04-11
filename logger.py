from kafka import KafkaConsumer
import json

    
if __name__ == '__main__':
    consumer = KafkaConsumer('logs', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
    log_file = open('logs.txt', 'w')
    for msg in consumer:
        json_string = msg.value.decode('utf-8')
        log = json.loads(json_string)
        
        log_file.write(json.dumps(log, indent=4) + '\n')
        print(json.dumps(log, indent=4) + '\n')