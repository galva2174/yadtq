import yadtq
import logging
import time
from kafka import KafkaConsumer 
import json

yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')  # Change 'kafka:9092' to 'localhost:9092'

def process_task(task):
    task_id = task["task_id"]
    task_type = task["task"]
    args = task["args"]

    yadtq_system.redis_client.hset(task_id, "status", "processing")
    
    if task_type == "add":
        result = sum(args)
    else:
        result = "unknown_task"

    yadtq_system.redis_client.hset(task_id, "status", "success")
    yadtq_system.redis_client.hset(task_id, "result", result)

def start_worker():
    consumer = KafkaConsumer(
    'task_queue',
    bootstrap_servers='localhost:9092',
    group_id='yadtq_workers',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    enable_auto_commit=True
    )
    for message in consumer:
        task = message.value
        print(f"Processing task: {task}")
        process_task(task)

if __name__ == "__main__":
    start_worker()