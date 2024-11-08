# client.py
import yadtq

# Configure the YADTQ module with broker and backend IPs
yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')  # Change 'kafka:9092' to 'localhost:9092'

# Submit a sample task and fetch its status
task_id = yadtq_system.send_task("add", [1, 2])
print(f"Task ID: {task_id}")

# Check task status
status = yadtq_system.get_task_status(task_id)
print(f"Task status: {status}")