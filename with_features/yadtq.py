import os
import json
import uuid
from kafka import KafkaProducer, KafkaConsumer
import redis

class YADTQ:
    def __init__(self, broker, backend):
        self.broker = broker
        self.backend = backend
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.redis_client = redis.Redis(host=backend, port=6379, db=0)

    def send_task(self, task_type, args):
        task_id = str(uuid.uuid4())
        task = {"task_id": task_id, "task": task_type, "args": args}
        self.redis_client.hset(task_id, "status", "queued")
        self.redis_client.hset(task_id, "result", "")  # Initialize an empty result
        self.producer.send('task_queue1', task)
        self.producer.flush()  # Ensure that the message is sent immediately
        print(f"Task sent with ID: {task_id}")  # Add a print statement for logging
        return task_id

    def get_task_status(self, task_id):
        task_data = self.redis_client.hgetall(task_id)
        if not task_data:
            return {"status": "not_found", "result": "Task ID not found in the system."}
        
        status = task_data.get(b"status", b"").decode('utf-8')
        result = task_data.get(b"result", b"").decode('utf-8')

        return {"status": status, "result": result}

# Initialization function for client/worker
def config(broker, backend):
    broker = "localhost:9092"
    backend = "localhost"
    return YADTQ(broker, backend)

