from kafka import KafkaProducer, KafkaConsumer
import json


producer = KafkaProducer(bootstrap_servers='localhost:9092')
log = { 'Process': 'node_manager', 'message': 'I have been run' }
producer.send("logs", json.dumps(log).encode('utf-8'))
producer.flush()


## Have a list of corresponding node ids and their agents
class NodeManager:
    def __init__(self):
        pass
    
    def create_node(self, process_config):
        pass
        
    def remove_node(self, process_config):
        pass

    def reset_node(self):
        ## needed for health check
        pass
    
    def get_health(self, nodeid):   ## ask agent of the corresponding nodes 
        pass
    
    def run_process_on_node(self, process_config, nodeid):
        pass   ## call the run process of the agent of the node id.
    
if __name__ == "__main__":
    consumer = KafkaConsumer('node_manager', bootstrap_servers='localhost:9092')
    for msg in consumer:
        # Take input
        request = json.loads(msg.value.decode('utf-8'))
        log = { 'Process': 'node_manager', 'message': 'I have received a message', 'text': request}
        producer.send("logs", json.dumps(log).encode('utf-8'))
        
        # Process RPC request
        result = NodeManager.start_process(request)
        
        # Send output
        response = {"request": request, "result": result}
        producer.send("logs", json.dumps(response).encode('utf-8'))
        producer.send("bootstrapRead", json.dumps(response).encode('utf-8'))