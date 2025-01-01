import os
import json
import uuid
import time
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import redis

logging.basicConfig(level=logging.INFO)


class YADTQ:
    def __init__(self, broker, backend):
        self.broker = broker
        self.backend = backend
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.redis_client = redis.Redis(host=backend, port=6379, db=0)
        self.admin_client = KafkaAdminClient(bootstrap_servers=broker)

        self.topics = ['worker_queue_1', 'worker_queue_2', 'worker_queue_3']
        self.create_topics_if_not_exist()
        self.initialize_topic_counters()

    def create_topics_if_not_exist(self):
        """Create Kafka topics if they do not already exist."""
        existing_topics = self.admin_client.list_topics()
        new_topics = []

        for topic in self.topics:
            if topic not in existing_topics:
                new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

        if new_topics:
            self.admin_client.create_topics(new_topics=new_topics)

    def initialize_topic_counters(self):
        """Initialize Redis counters for each topic if not already present."""
        for topic in self.topics:
            if not self.redis_client.exists(f"count:{topic}"):
                self.redis_client.set(f"count:{topic}", 0)
   
    def get_queue_load(self):
        """Get the current load (task count) for each queue from Redis."""
        queue_load = {topic: int(self.redis_client.get(f"count:{topic}")) for topic in self.topics}
        return queue_load
    def get_task_status(self, task_id):
        """Retrieve the status and result of a task from Redis."""
        if not self.redis_client.exists(task_id):
            return {"status": "not_found", "result": "Task ID not found."}

        task_data = self.redis_client.hgetall(task_id)

        # Decode task details from Redis
        status = task_data.get(b'status', b'unknown').decode('utf-8')
        result = task_data.get(b'result', b'').decode('utf-8')

        return {"status": status, "result": result}


    def send_task(self, task_type, args):
        """Send a task to the least loaded queue."""
        task_id = str(uuid.uuid4())
        task = {"task_id": task_id, "task": task_type, "args": args}

        # Determine the topic with the least task count
        queue_load = self.get_queue_load()
        target_topic = min(queue_load, key=queue_load.get)

        # Increment the task count for the selected topic
        self.redis_client.incr(f"count:{target_topic}")

        # Assign the task to the worker's task list
        worker_id = target_topic.split("_")[-1]
        self.redis_client.hset(f"worker:{worker_id}:tasks", task_id, "processing")

        # Send the task to the selected topic
        self.redis_client.hset(task_id, "status", "queued")
        self.redis_client.hset(task_id, "args", json.dumps(args))
        self.redis_client.hset(task_id, "task", task_type)
        self.redis_client.hset(task_id, "result", "")
        self.producer.send(target_topic, task)
        self.producer.flush()

       
        return task_id

    def task_done(self, topic):
        """Decrement the task count for a topic when a task is completed."""
        self.redis_client.decr(f"count:{topic}")

    def reassign_tasks(self, worker_id):
        """Reassign tasks from an inactive worker."""
        task_key = f"worker:{worker_id}:tasks"
        tasks = self.redis_client.hgetall(task_key)

        for task_id, status in tasks.items():
            if status.decode('utf-8') == "processing":
                logging.info(f"Reassigning task {task_id}")
                task_data = self.redis_client.hgetall(task_id)

                # Reassign task to a new queue
                args = json.loads(task_data[b'args'])
                task_type = task_data[b'task'].decode('utf-8')
                new_task_id = self.send_task(task_type, args)

                # Communicate the reassignment
                self.redis_client.hset(task_id, "status", "reassigned")
                self.redis_client.hset(task_id, "result", f"Reassigned to new task ID: {new_task_id}")

                logging.info(f"Task {task_id} reassigned to new task ID: {new_task_id}")

                # Clean up Redis
                self.redis_client.hdel(task_key, task_id)


    def monitor_workers(self):
        """Monitor worker heartbeats and reassign tasks from inactive workers."""
        while True:
            current_time = time.time()
            for worker_id in range(1, 4):  # Adjust range based on the number of workers
                heartbeat_key = f"heartbeat:worker_{worker_id}"
                last_heartbeat = self.redis_client.get(heartbeat_key)

                if last_heartbeat and current_time - float(last_heartbeat) > 15:
                   
                    self.reassign_tasks(worker_id)
            time.sleep(5)


def config(broker, backend):
    broker = "localhost:9092"
    backend = "localhost"
    return YADTQ(broker, backend)