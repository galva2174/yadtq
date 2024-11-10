# yadtq.py
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
        self.producer.send('task_queue', task)
        print(f"Task sent with ID: {task_id}")
        return task_id

    def get_task_status(self, task_id):
        return self.redis_client.hgetall(task_id)

def config(broker, backend):
    broker = "localhost:9092"
    backend = "localhost"
    return YADTQ(broker, backend)