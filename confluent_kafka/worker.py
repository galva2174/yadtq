import yadtq
import logging
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
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
   
    # Execute task based on task type
    if task_type == "add":
        result = sum(args)
    elif task_type == "subtract":
        result = args[0] - sum(args[1:])  # Subtract all subsequent arguments from the first one
    elif task_type == "multiply":
        result = 1
        for arg in args:
            result *= arg  # Multiply all arguments together
    elif task_type == "divide":
        try:
            result = args[0]
            for arg in args[1:]:
                result /= arg  # Divide the first argument by all subsequent ones
        except ZeroDivisionError:
            result = "Error: Division by zero"
    else:
        result = "unknown_task"

    # Mark as success or failure and store the result
    if result == "unknown_task" or result == "Error: Division by zero":
        yadtq_system.redis_client.hset(task_id, "status", "failed")
    else:
        yadtq_system.redis_client.hset(task_id, "status", "success")
        yadtq_system.redis_client.hset(task_id, "result", result)
       
    logging.info(f"Task {task_id} processed with result: {result}")

def start_worker():
    logging.info("Worker started and waiting for tasks...")
   
    # Create the Kafka Consumer
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',  # Broker address
        'group.id': 'yadtq_workers',  # Consumer group
        'auto.offset.reset': 'earliest',  # Start consuming from the earliest message if no offset is stored
        'enable.auto.commit': True,  # Enable auto-commit (ensure the consumer's offset is tracked)
    })

    # Subscribe to the 'task_queue' topic
    consumer.subscribe(['task_queue'])

    try:
        # Infinite loop to keep consuming messages
        while True:
            # Poll for new messages (timeout of 1 second)
            msg = consumer.poll(timeout=1.0)
           
            if msg is None:
                # No new message
                continue
            if msg.error():
                # Handle errors from Kafka
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition reached, no new messages
                    logging.warning(f"End of partition reached {msg.partition}")
                else:
                    # Some other error
                    raise KafkaException(msg.error())
            else:
                # Successfully received a message
                task = json.loads(msg.value().decode('utf-8'))  # Deserialize the JSON data
                logging.info(f"Received task: {task}")
                print(f"Processing task: {task}")
                process_task(task)

    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        logging.info("Worker interrupted. Shutting down...")

    finally:
        # Ensure consumer is closed properly
        consumer.close()

if __name__ == "__main__":
    start_worker()