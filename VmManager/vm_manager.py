from kafka import KafkaProducer, KafkaConsumer
import json


BOOTSTRAP_SERVER = 'localhost:9092'


# Maintain the list of all VMs
class VmManager:
    def __init__(self):
        pass
    
    def allocate_vm(self, process_config):
        return {'method': 'allocate_vm', 'process_config': process_config}
        
    def remove_vm(self, vm_id):
        return {'method': 'remove_vm', 'vm_id': vm_id}

    def reset_vm(self, vm_id):
        return {'method': 'reset_vm', 'vm_id': vm_id}
    
    def get_vms(self):
        return {'method': 'get_vms'}
    
    def get_health(self):
        return {'method': 'get_health'}
    

if __name__ == "__main__":
    BOOTSTRAP_SERVER = sys.argv[-1]
    # create a producer, log that vm_manager has started.
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    log = { 'Process': 'vm_manager', 'message': 'I have been run' }
    producer.send("logs", json.dumps(log).encode('utf-8'))

    # Start vm_manager server
    vm_manager = VmManager()
    consumer = KafkaConsumer('VmManagerIn', bootstrap_servers='localhost:9092')
    print("Starting the VM Manager\n")
    
    for msg in consumer:
        # Parse the request
        request = json.loads(msg.value)
        log = { 'Process': 'vm_manager', 'message': 'I have received a message', 'text': request}
        producer.send("logs", json.dumps(log).encode('utf-8'))
        
        # Process RPC request
        if(request['method'] == 'allocate_vm'):
            result = vm_manager.allocate_vm(request['args']['config'])
        elif(request['method'] == 'remove_vm'):
            result = vm_manager.remove_vm(request['args']['vm_id'])
        elif(request['method'] == 'reset_vm'):
            result = vm_manager.reset_vm(request['args']['vm_id'])
        elif(request['method'] == 'get_vms'):
            result = vm_manager.get_vms()
        elif(request['method'] == 'get_health'):
            result = vm_manager.get_health()
        else:
            result = {'error': 'Invalid method'}
            print("Invalid method ", request['method'])
            
        # Send the response
        response = {"request": request, "result": result}
        producer.send("VmManagerOut", json.dumps(response).encode('utf-8'))
        producer.send("logs", json.dumps(response).encode('utf-8'))
