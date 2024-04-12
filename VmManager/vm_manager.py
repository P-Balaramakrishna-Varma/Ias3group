import json
import paramiko
import threading
from kafka import KafkaConsumer, KafkaProducer

SCRIPT_PATH = './bootstrap.sh'
VM_LIST_PATH = './vm_list.json'
GET_HEALTH_SCRIPT = './get_health.py'

class VM:
    def __init__(self, ip, username, password):
        self.ip = ip
        self.username = username
        self.password = password
        self.is_active = False

    def activate_vm(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.ip, username=self.username, password=self.password)
        sftp_client = ssh_client.open_sftp()
        sftp_client.put(SCRIPT_PATH, 'bootstrap.sh')
        sftp_client.close()
        ssh_client.exec_command('sh bootstrap.sh')
        self.is_active = True
        ssh_client.close()

    def get_health(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.ip, username=self.username, password=self.password)
        sftp_client = ssh_client.open_sftp()
        sftp_client.put(GET_HEALTH_SCRIPT, 'get_health.py')
        sftp_client.close()
        _, stdout, _ = ssh_client.exec_command('python3 get_health.py')
        output = stdout.read().decode()
        cpu_usage, memory_free = map(float, output.strip().split(','))
        ssh_client.close()
        return cpu_usage, memory_free
    
    def to_dict(self):
        # Return VM details without sensitive data like password
        return {
            'ip': self.ip,
            'username': self.username,
            'is_active': self.is_active
        }

class VMManager:
    def __init__(self, vm_list_path):
        self.vm_list_path = vm_list_path
        self.vms = self.load_vms()
        self.lock = threading.Lock()

    def load_vms(self):
        with open(self.vm_list_path, 'r') as file:
            vm_data = json.load(file)
            return [VM(vm['ip'], vm['username'], vm['password']) for vm in vm_data]

    def allocate_vm(self):
        with self.lock:
            for vm in self.vms:
                if not vm.is_active:
                    vm.activate_vm()
                    return {"msg": "VM allocated", "ip": vm.ip, "username": vm.username, "password": vm.password}
            
            # Assess health of all active VMs
            health_stats = [(vm.get_health(), vm) for vm in self.vms if vm.is_active]
            if health_stats:
                # Select the VM with the lowest CPU usage and most free memory
                best_vm = min(health_stats, key=lambda x: (x[0][0], -x[0][1]))[1]
                return {"msg": "VM allocated", "ip": best_vm.ip, "username": best_vm.username, "password": best_vm.password}

            return {"msg": "No inactive VM available"}
    
    def remove_vm(self, vm_id):
        return {'method': 'remove_vm', 'vm_id': vm_id}

    def reset_vm(self, vm_id):
        return {'method': 'reset_vm', 'vm_id': vm_id}
    
    def get_health(self):
        return {'method': 'get_health'}
    
    
    def get_vms(self):
        # Return a list of VMs with their current status
        with self.lock:
            return [vm.to_dict() for vm in self.vms]


if __name__ == '__main__':
    # create a producer, log that vm_manager has started.
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    log = { 'Process': 'vm_manager', 'message': 'I have been run' }
    producer.send("logs", json.dumps(log).encode('utf-8'))

    # Start vm_manager server
    vm_manager = VMManager(VM_LIST_PATH)
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