from kafka import KafkaProducer, KafkaConsumer
import json, paramiko


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

class Node:
    def __init__(self, node_id, ip, username, password):
        self.node_id = node_id
        self.ip = ip
        self.username = username
        self.password = password
        self.is_active = False

    def activate_node(self):
        if(self.is_active == False):
            # Connect to node with ssh
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.ip, username=self.username, password=self.password)

            # Type the command to run the initialization script
            sftp_client = ssh_client.open_sftp()
            sftp_client.put(SCRIPT_PATH, 'bootstrap.sh')
            sftp_client.close()
            
            # Run the initialization script
            ssh_client.exec_command('sh bootstrap.sh')

            # Close the ssh connection
            ssh_client.close()
            self.is_active = True

    def reset_node(self):
        # Get all processes from agent
        # Kill all processes
        pass
    
    def get_health(self):
        # Ask agent for health
        pass
    
    def run_process(self, process_config):
        process_details = kafka_rpc("Agent", {"method": "start_process", "node_id": self.node_id, "args": process_config})
        if(process_details['status'] == 'success'):
            return True
        else:
            return False



## Have a list of corresponding node ids and their agents
class NodeManager:
    def __init__(self):
        self.nodes = {}
    
    def create_node(self):
        Vm_details = kafka_rpc("VmManager", {"method": "allocate_vm"})
        if(Vm_details['status'] == 'success'):
            node = Node(len(self.nodes), Vm_details['ip'], Vm_details['username'], Vm_details['password'])
            self.nodes[node.node_id] = node
            return {'status': 'success', "msg": "created a node"}
        else:
            return {'status': 'failure', "error": "No VMs available"}
            
    def remove_node(self, node_id):
        return {"method": "remove_node", "nodeid": node_id}

    def reset_node(self, node_id):
        return {"method": "reset_node", "nodeid": node_id}
    
    def get_health(self, node_id):   ## ask agent of the corresponding nodes 
        return {"method": "get_health", "nodeid": node_id}
        
    def run_process_on_node(self, node_id, process_config):
        if(node_id not in self.nodes):
            return {"status": 'failure', "error": "Node not found", "nodeid": node_id}
        
        sucess = self.nodes[node_id].run_process(process_config)
        if(sucess):
            return {'status': 'success', "msg": "Process started", "nodeid": node_id, "process_config": process_config}
        else:
            return {'status': 'failure', "error": "Process failed to start", "nodeid": node_id, "process_config": process_config}
    
    
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