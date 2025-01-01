import yadtq
import logging
import time
from kafka import KafkaConsumer
import json
import threading

logging.basicConfig(level=logging.INFO)

# Configure the YADTQ system
yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')


def process_task(task, topic):
    """Process the given task and update its status in Redis."""
    task_id = task["task_id"]
    task_type = task["task"]
    args = task["args"]

    yadtq_system.redis_client.hset(task_id, "status", "processing")
    result = "invalid"
    try:
        # Simulate task processing
        if task_type == "add":
            time.sleep(20)
            result = sum(args)
        elif task_type == "subtract":
            time.sleep(5)
            result = args[0] - sum(args[1:])
        elif task_type == "multiply":
            result = 1
            for num in args:
                result *= num
        elif task_type == "divide":
            result = args[0]
            for num in args[1:]:
                if num == 0:
                    raise ValueError("Division by zero error.")
                result /= num
        else:
            raise ValueError("Unknown operation")

        yadtq_system.redis_client.hset(task_id, "status", "success")
        yadtq_system.redis_client.hset(task_id, "result", str(result))

    except Exception as e:
        yadtq_system.redis_client.hset(task_id, "status", "failed")
        yadtq_system.redis_client.hset(task_id, "result", f"Error: {str(e)}")

    # Decrement the task count for the topic
    yadtq_system.task_done(topic)

    # Remove the task from the worker's task list
    worker_id = topic.split("_")[-1]
    yadtq_system.redis_client.hdel(f"worker:{worker_id}:tasks", task_id)

    logging.info(f"Task {task_id} processed with result: {result}")


def send_heartbeat(worker_id):
    """Send a periodic heartbeat signal to Redis."""
    while True:
        yadtq_system.redis_client.set(f"heartbeat:worker_{worker_id}", time.time())
        time.sleep(5)


def start_worker(worker_id):
    """Start a worker to consume tasks from a specific queue."""
    topic = f"worker_queue_{worker_id}"
    logging.info(f"Worker {worker_id} started and listening to {topic}")

    # Start the heartbeat thread
    threading.Thread(target=send_heartbeat, args=(worker_id,), daemon=True).start()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id=f'yadtq_worker_{worker_id}',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        enable_auto_commit=True
    )
    for _ in consumer.poll(timeout_ms = 1000).values():
        pass
    for message in consumer:
        task = message.value
        task_id = task["task_id"]

        logging.info(f"Worker {worker_id} received task: {task}")
        process_task(task, topic)

if __name__ == "__main__":
    import sys
    from threading import Thread

    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
    else:
        worker_id = int(sys.argv[1])  # Pass worker ID as a command-line argument
        Thread(target=start_worker, args=(worker_id,)).start()
        Thread(target=yadtq_system.monitor_workers, daemon=True).start()

