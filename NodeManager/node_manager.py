from kafka import KafkaProducer, KafkaConsumer
import json


## Have a list of corresponding node ids and their agents
class NodeManager:
    def __init__(self):
        pass
    
    def create_node(self):
        return {"method": "create_node"}
        
    def remove_node(self, node_id):
        return {"method": "remove_node", "nodeid": node_id}

    def reset_node(self, node_id):
        return {"method": "reset_node", "nodeid": node_id}
    
    def get_health(self, node_id):   ## ask agent of the corresponding nodes 
        return {"method": "get_health", "nodeid": node_id}
        
    def run_process_on_node(self, node_id, process_config):
        return {"method": "run_process_on_node", "process_config": process_config, "nodeid": node_id}
    
    
    
if __name__ == "__main__":
    # create a producer, log that node_manager has started.
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    log = { 'Process': 'node_manager', 'message': 'I have been run' }
    producer.send("logs", json.dumps(log).encode('utf-8'))

    # Start node_manager server
    node_manager = NodeManager()
    consumer = KafkaConsumer('NodeManagerIn', bootstrap_servers='localhost:9092')
    print("Starting Node Manager Server")

    for msg in consumer:
        # Take input
        request = json.loads(msg.value.decode('utf-8'))
        log = { 'Process': 'node_manager', 'message': 'I have received a message', 'text': request}
        producer.send("logs", json.dumps(log).encode('utf-8'))
        
        # Process RPC request
        if(request['method'] == 'create_node'):
            result = node_manager.create_node()
        elif(request['method'] == 'remove_node'):
            result = node_manager.remove_node(request['args']['node_id'])
        elif(request['method'] == 'reset_node'):
            result = node_manager.reset_node(request['args']['node_id'])
        elif(request['method'] == 'get_health'):
            result = node_manager.get_health(request['args']['node_id'])
        elif(request['method'] == 'run_process_on_node'):
            result = node_manager.run_process_on_node(request['args']['node_id'], request['args']['config'])
        else:
            result = {"error": "method not found"}
        
        # Send output
        response = {"request": request, "result": result}
        producer.send("logs", json.dumps(response).encode('utf-8'))
        producer.send("NodeManagerOut", json.dumps(response).encode('utf-8'))