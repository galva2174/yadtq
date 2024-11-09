import json
import uuid
from confluent_kafka import Producer
import redis

class YADTQ:
    def __init__(self, broker, backend):
        self.broker = broker
        self.backend = backend
        # Initialize the Confluent Kafka Producer without value.serializer
        self.producer = Producer({
            'bootstrap.servers': broker  # Only specify bootstrap servers here
        })
        # Initialize the Redis client
        self.redis_client = redis.Redis(host=backend, port=6379, db=0)

    def send_task(self, task_type, args):
        task_id = str(uuid.uuid4())
        task = {"task_id": task_id, "task": task_type, "args": args}
        self.redis_client.hset(task_id, "status", "queued")
       
        # Manually serialize the task to JSON (as bytes) before sending it to Kafka
        task_bytes = json.dumps(task).encode('utf-8')
       
        # Send the serialized task to Kafka topic 'task_queue'
        self.producer.produce('task_queue', value=task_bytes)

        # Flush the producer to ensure the task is sent
        self.producer.flush()

        print(f"Task sent with ID: {task_id}")
        return task_id

    def get_task_status(self, task_id):
        return self.redis_client.hgetall(task_id)

# Initialization function for client/worker
def config(broker, backend):
    broker = "localhost:9092"  # Default broker
    backend = "localhost"      # Default Redis backend
    return YADTQ(broker, backend)