# worker.py
import yadtq
import logging
import time
from kafka import KafkaConsumer 
import json

# Configure logging
logging.basicConfig(level=logging.INFO)

# Configure the YADTQ module with broker and backend IPs
yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')  # Change 'kafka:9092' to 'localhost:9092'

def process_task(task):
    task_id = task["task_id"]
    task_type = task["task"]
    args = task["args"]

    # Mark as processing
    yadtq_system.redis_client.hset(task_id, "status", "processing")
    
    # Simple task execution example
    if task_type == "add":
        result = sum(args)
    else:
        result = "unknown_task"

    # Mark as success and store the result
    yadtq_system.redis_client.hset(task_id, "status", "success")
    yadtq_system.redis_client.hset(task_id, "result", result)
    logging.info(f"Task {task_id} processed with result: {result}")

def start_worker():
    logging.info("Worker started and waiting for tasks...")
    consumer = KafkaConsumer(
    'task_queue',
    bootstrap_servers='localhost:9092',
    group_id='yadtq_workers',  # Add this if not already present
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    enable_auto_commit=True    # Ensure auto commit is enabled
    )
    for message in consumer:
        task = message.value
        logging.info(f"Received task: {task}")
        print(f"Processing task: {task}")
        process_task(task)

if __name__ == "__main__":
    start_worker()
