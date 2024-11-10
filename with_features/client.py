import yadtq

# Configure the YADTQ module with broker and backend IPs
yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')  # Change 'kafka:9092' to 'localhost:9092'

# Function to prompt the user for input and send the task
def get_user_input():
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

# Get user input for operation and arguments
operation, args = get_user_input()

# Proceed if valid input is provided
if operation and args:
    # Submit the task to YADTQ system
    task_id = yadtq_system.send_task(operation, args)
    print(f"Task ID: {task_id}")

    # Check task status
    status = yadtq_system.get_task_status(task_id)
    print(f"Task status: {status}")

