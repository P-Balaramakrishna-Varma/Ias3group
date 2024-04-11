from kafka import KafkaConsumer
import json

    
if __name__ == '__main__':
    consumer = KafkaConsumer('logs', bootstrap_servers='localhost:9092')  ## earliest to get all the logs
    for msg in consumer:
        json_string = msg.value.decode('utf-8')
        log = json.loads(json_string)
        
        log_file = open('logs.txt', 'a')
        log_file.write(json.dumps(log, indent=4) + '\n')
        log_file.close()
        print(json.dumps(log, indent=4) + '\n')