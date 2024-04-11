from kafka import KafkaProducer, KafkaConsumer
import json, time


# maintain the list of all processes
class Agent:
    def __init__(self):
        pass
    
    def start_process(self, process_config):
        return True

    def kill_process(self, process_config):
        pass

    def reset_processes(self):
        ## needed for health check
        pass
    
    def get_health(self):
        pass


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    log = { 'Process': 'Agent', 'message': 'I have been run' }
    producer.send("logs", json.dumps(log).encode('utf-8'))
    producer.flush()

    agent = Agent()
    consumer = KafkaConsumer('agent', bootstrap_servers='localhost:9092')
    for msg in consumer:
        # Take input
        request = json.loads(msg.value.decode('utf-8'))
        log = { 'Process': 'Agent', 'message': 'I have received a message', 'text': request}
        producer.send("logs", json.dumps(log).encode('utf-8'))
        
        # Process RPC request
        result = agent.start_process(request)
        
        # Send output
        response = {"request": request, "result": result}
        producer.send("logs", json.dumps(response).encode('utf-8'))
        producer.send("bootstrapRead", json.dumps(response).encode('utf-8'))