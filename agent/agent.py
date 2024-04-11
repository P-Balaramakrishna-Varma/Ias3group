from kafka import KafkaProducer, KafkaConsumer
import json, time
import sys

# maintain the list of all processes
class Agent:
    def __init__(self):
        pass
    
    def start_process(self, process_config):
        return {'method': 'start_process', 'process_config': process_config}

    def kill_process(self, process_id):
        return {'method': 'kill_process', 'process_id': process_id}

    def reset_process(self, process_id):
        return {'method': 'reset_process', 'process_id': process_id}
       
    def get_processes(self): ## return with ids
        return {'method': 'get_processes'}
    
    def get_health(self):
        return {'method': 'get_health'}


if __name__ == "__main__":
    # get the node_id.
    node_id = sys.argv[1]
    
    # create a producer, log that agent has started.
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    log = { 'Process': 'Agent' + node_id, 'message': 'I have been run' }
    producer.send("logs", json.dumps(log).encode('utf-8'))

    # Start agent server
    agent = Agent()
    consumer = KafkaConsumer('agent', bootstrap_servers='localhost:9092')
    print("Starting the agent server\n")
   
    for msg in consumer:
        request = json.loads(msg.value)
        if(request['node_id'] == node_id):
            log = { 'Process': 'Agent ' + node_id, 'message': 'I have received a message', 'text': request}
            # producer.send("logs", json.dumps(log).encode('utf-8'))
            
            # Process RPC request
            if(request['method'] == 'start_process'):
                result = agent.start_process(request['args']['config'])
            elif(request['method'] == 'kill_process'):
                result = agent.kill_process(request['args']['process_id'])
            elif(request['method'] == 'reset_process'):
                result = agent.reset_process(request['args']['process_id'])
            elif(request['method'] == 'get_processes'):
                result = agent.get_processes()
            elif(request['method'] == 'get_health'):
                result = agent.get_health()
            else:
                result = {'error': 'Invalid method'}
                print("Invalid method ", request['method'])
            
            # Send output
            response = {"request": request, "result": result}
            # producer.send("logs", json.dumps(response).encode('utf-8'))
            producer.send(request['response_topic'], json.dumps(response).encode('utf-8'))