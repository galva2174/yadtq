
import yadtq

yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')

task_id = yadtq_system.send_task("add", [1, 2])
print(f"Task ID: {task_id}")

status = yadtq_system.get_task_status(task_id)
print(f"Task status: {status}")