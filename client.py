import yadtq
import time

yadtq_system = yadtq.config(broker='localhost:9092', backend='localhost')
def get_user_input_for_task_creation():
   
    operation = input("Enter the operation (e.g., add, subtract, multiply, divide): ").strip().lower()

    args_input = input("Enter the arguments (space-separated numbers): ").strip()

    try:
        args = list(map(int, args_input.split()))
    except ValueError:
        print("Invalid input for arguments. Please enter valid integers.")
        return None, None

    return operation, args

def get_user_input_for_task_status():
   
    task_id = input("Enter the task ID to query the status or result: ").strip()
    return task_id

def query_task_status(task_id):
    task_status = yadtq_system.get_task_status(task_id)

    if task_status["status"] == "success":
        print(f"Task succeeded with result: {task_status['result']}")
    elif task_status["status"] == "failed":
        print(f"Task failed with error: {task_status['result']}")
    elif task_status["status"] == "reassigned":
        new_task_id = task_status["result"].split(": ")[-1]
        print(f"Task was reassigned. New task ID: {new_task_id}")
        print("You can query the new task ID for the updated status.")
    else:
        print(f"Task is still {task_status['status']}.")

def main():
    while True:
       
        action = input("Do you want to create a new task (1) or query an existing task (2)? (Enter 1 or 2): ").strip()

        if action == "1":
           
            operation, args = get_user_input_for_task_creation()

            if operation and args:
               
                task_id = yadtq_system.send_task(operation, args)
                print(f"Task ID: {task_id}")

                # poll_for_task_status(task_id)

        elif action == "2":
           
            task_id = get_user_input_for_task_status()

            query_task_status(task_id)

        else:
            print("Invalid choice. Please enter 1 to create a task or 2 to query an existing task.")

if __name__ == "__main__":
    main()
