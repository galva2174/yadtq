import yadtq
import time

# Configure the YADTQ module with broker and backend IPs
yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')  # Change 'kafka:9092' to 'localhost:9092'

# Function to prompt the user for input and send the task
def get_user_input_for_task_creation():
    # Ask the user for the operation type (e.g., add, subtract, multiply, divide)
    operation = input("Enter the operation (e.g., add, subtract, multiply, divide): ").strip().lower()

    # Ask the user for the arguments (expecting space-separated numbers)
    args_input = input("Enter the arguments (space-separated numbers): ").strip()

    # Convert the arguments from string to a list of integers
    try:
        args = list(map(int, args_input.split()))
    except ValueError:
        print("Invalid input for arguments. Please enter valid integers.")
        return None, None

    return operation, args

def get_user_input_for_task_status():
    # Ask the user for a task ID to check its status
    task_id = input("Enter the task ID to query the status or result: ").strip()
    return task_id

def poll_for_task_status(task_id):
    # Poll for task status every 500 ms until it's either 'success' or 'failed'
    while True:
        # Check task status
        task_status = yadtq_system.get_task_status(task_id)

        if task_status["status"] == "success":
            print(f"Task succeeded with result: {task_status['result']}")
            break
        elif task_status["status"] == "failed":
            print(f"Task failed with error: {task_status['result']}")
            break
        else:
            print(f"Task is still {task_status['status']}. Checking again...")
            time.sleep(0.5)  # Sleep for 500 ms before checking again

def query_task_status(task_id):
    task_status = yadtq_system.get_task_status(task_id)
    if task_status["status"] == "success":
        print(f"Task succeeded with result: {task_status['result']}")
    elif task_status["status"] == "failed":
        print(f"Task failed with error: {task_status['result']}")
    else:
        print(f"Task is still {task_status['status']}.")

# Main function to drive the client
def main():
    while True:
        # Ask if the user wants to create a new task or query an existing one
        action = input("Do you want to create a new task (1) or query an existing task (2)? (Enter 1 or 2): ").strip()

        if action == "1":
            # Get user input for creating a new task
            operation, args = get_user_input_for_task_creation()

            # Proceed if valid input is provided
            if operation and args:
                # Submit the task to YADTQ system
                task_id = yadtq_system.send_task(operation, args)
                print(f"Task ID: {task_id}")

                # Poll for task status until it's either 'success' or 'failed'
                poll_for_task_status(task_id)

        elif action == "2":
            # Get user input for querying an existing task
            task_id = get_user_input_for_task_status()

            # Query task status and result
            query_task_status(task_id)

        else:
            print("Invalid choice. Please enter 1 to create a task or 2 to query an existing task.")

if __name__ == "__main__":
    main()

