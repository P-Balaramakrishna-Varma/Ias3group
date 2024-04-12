from kafka import KafkaProducer, KafkaConsumer
import json


producer = KafkaProducer(bootstrap_servers='localhost:9092')
log = { 'Process': 'vm_manager', 'message': 'I have been run' }
producer.send("logs", json.dumps(log).encode('utf-8'))
producer.flush()

class VmManager:
    def __init__(self):
        pass
    
    def allocate_vm(self, process_config):
        pass
        
    def remove_vm(self, process_config):
        pass

    def reset_vm(self):
        ## needed for health check
        pass
    
    def get_health(self):
        pass
    
    
if __name__ == "__main__":
    consumer = KafkaConsumer('vm_manager', bootstrap_servers='localhost:9092')
    for msg in consumer:
        # Take input
        request = json.loads(msg.value.decode('utf-8'))
        log = { 'Process': 'vm_manager', 'message': 'I have received a message', 'text': request}
        producer.send("logs", json.dumps(log).encode('utf-8'))
        
        # Process RPC request
        result = agent.start_process(request)
        
        # Send output
        response = {"request": request, "result": result}
        producer.send("logs", json.dumps(response).encode('utf-8'))
        producer.send("bootstrapRead", json.dumps(response).encode('utf-8'))