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
    
    # Process the task based on the operation type
    if task_type == "add":
        result = sum(args)
    elif task_type == "subtract":
        result = args[0] - sum(args[1:])
    elif task_type == "multiply":
        result = 1
        for num in args:
            result *= num
    elif task_type == "divide":
        try:
            result = args[0]
            for num in args[1:]:
                if num == 0:
                    result = "Error: Division by zero"
                    break
                result /= num
        except Exception as e:
            result = f"Error: {str(e)}"
    else:
        result = "unknown_task"

    # Mark as success and store the result
    yadtq_system.redis_client.hset(task_id, "status", "success")
    yadtq_system.redis_client.hset(task_id, "result", result)

    # Log the result of the task
    logging.info(f"Task {task_id} processed with result: {result}")
    print(f"Task {task_id} processed with result: {result}")

def start_worker():
    logging.info("Worker started and waiting for tasks...")
    consumer = KafkaConsumer(
        'task_queue1',
        bootstrap_servers='localhost:9092',
        group_id='yadtq_workers',  # Add this if not already present
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        enable_auto_commit=True    # Ensure auto commit is enabled
    )

    for message in consumer:
        task = message.value
        logging.info(f"Received task: {task}")  # Log received task
        print(f"Processing task: {task}")  # Confirm received task
        process_task(task)

if __name__ == "__main__":
    start_worker()

