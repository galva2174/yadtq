


# Yadtq Project Setup Guide

This guide will walk you through setting up and running the necessary environment, services, and application components for the Yadtq project.

## Prerequisites
Ensure you have `python3`, `kafka`, `zookeeper`, and `redis` installed on your system.

## Setup Instructions

### Step 1: Create a Virtual Environment
1. Create a virtual environment called `yadtq_env`:
   ```bash
   python3 -m venv yadtq_env
   ```

### Step 2: Activate the Environment
We will use 4 terminal windows for this project.

1. Open two terminals for the worker and client processes.
2. Activate the virtual environment in each terminal:
   ```bash
   source yadtq_env/bin/activate
   ```

### Step 3: Install Required Libraries
In one of the activated terminals, install the required libraries:
```bash
pip install kafka-python redis
```

### Step 4: Start Zookeeper and Kafka
1. Open a third terminal (do not activate the virtual environment here).
   ```bash
   sudo systemctl start zookeeper
   sudo systemctl start kafka
   ```
3. Start Zookeeper and Kafka by checking their statuses:
   ```bash
   sudo systemctl status zookeeper
   sudo systemctl status kafka
   ```
   
   If either service is not running, restart them with:
   ```bash
   sudo systemctl restart zookeeper
   sudo systemctl restart kafka
   ```

### Step 5: Start Redis Server
In the third terminal, start the Redis server:
```bash
redis-server
```

### Step 6: Open Redis CLI
1. Use a fourth terminal for testing and checking purposes.
2. Access the Redis CLI:
   ```bash
   redis-cli
   ```

3. To retrieve the status of a task, use:
   ```bash
   HGETALL task_id
   ```

## Running the Application
To run the application, we will use the first two terminals.

1. In the **first terminal** (with the virtual environment activated), start the worker (consumer):
   ```bash
   python worker.py
   ```

2. In the **second terminal** (also activated), start the client (producer):
   ```bash
   python client.py
   ```

## Troubleshooting
### Kafka Broker Connection Issues
If you encounter Kafka broker connection errors, follow these steps:

1. Modify the Kafka `server.properties` file to advertise localhost as the listener:
   ```bash
   sudo nano /usr/local/kafka/config/server.properties
   ```

2. Add these lines (anywhere in the file):
   ```plaintext
   listeners=PLAINTEXT://localhost:9092
   advertised.listeners=PLAINTEXT://localhost:9092
   ```

3. Save the file and restart Kafka:
   ```bash
   sudo systemctl restart kafka
   ```
### Redis Server in use
Step 1:
   ```bash
   sudo systemctl stop redis
   ```
Step 2:
   ```bash
   redis-server
   ```
---
### If six module isn't install properly

Please run the code from the folder 'confluent-kafka'
**Note**: This README assumes that `worker.py` and `client.py` are set up to communicate with Kafka and Redis correctly and handle tasks as required.
``` 

This README file outlines each step and command in detail, providing a clear setup and troubleshooting guide.
