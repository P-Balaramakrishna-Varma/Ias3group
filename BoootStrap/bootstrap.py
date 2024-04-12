import subprocess
from kafka import KafkaProducer, KafkaConsumer
import json

def run_process(process_config, kafka_producer):
    #request = {"agent id": "to be implemented", "method": }
    kafka_producer.send("agent", json.dumps(process_config).encode('utf-8'))
    consumer = KafkaConsumer("bootstrapRead", bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
    print("Waiting for agent to run the process " + process_config["name"])
    for msg in consumer:
        value = json.loads(msg.value.decode('utf-8'))
        if(value['request'] == process_config):
            print("The process " + process_config["name"] + "has started\n")
            break

# Stage 1
producer = KafkaProducer(bootstrap_servers='localhost:9092')
# agent_process = subprocess.Popen(['python', 'agent.py'])
for process_config in json.load(open('boot_config.json'))['stage 1']:
    run_process(process_config, producer)





# agent_process.kill()
## Stage 2
# Create a node
# For each subsystem configuration, run the corresponding process.
# Configuration = Node : Id, path, command json with three keys.