import yadtq
import json

# Configure the YADTQ module with broker and backend IPs
yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')  # Change 'kafka:9092' to 'localhost:9092'

def get_user_input():
    """Get task type and arguments from the user."""
    # Ask for task type
    task_type = input("Enter the task type (e.g., add, subtract, multiply): ").strip()

    # Ask for task arguments
    args_input = input("Enter the task arguments as a comma-separated list (e.g., 1, 2, 3): ").strip()

    # Convert the input string to a list of numbers
    try:
        args = list(map(int, args_input.split(',')))
    except ValueError:
        print("Invalid input for arguments. Please enter a list of integers.")
        return None, None

    return task_type, args

# Get task details from the user
task_type, args = get_user_input()

if task_type and args:
    # Submit the task to YADTQ and fetch the task ID
    task_id = yadtq_system.send_task(task_type, args)
    print(f"Task ID: {task_id}")

    # Check task status
    status = yadtq_system.get_task_status(task_id)
    print(f"Task status: {status}")
else:
    print("Task could not be created due to invalid input.")
