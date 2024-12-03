# MemFlow - Dynamic Distributed Memory Allocator and File Management System

MemFlow is a robust distributed file system designed to manage memory and files dynamically across a network of interconnected devices. It addresses the challenges of resource utilization, fault tolerance, scalability, and efficient data retrieval in distributed environments.

## Table of Contents
- [Features](#features)
- [System Architecture](#system-architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Contributors](#contributors)

## Features
- **Dynamic Allocation and Distribution**: Files are split into chunks and distributed across nodes based on available resources.
- **Heartbeat Monitoring**: Real-time node health monitoring for fault detection.
- **Fault Tolerance**: Automatic data redistribution in case of node failure.
- **Scalability**: Seamless integration of new devices into the system.
- **Query Processing**: Efficient record retrieval using a master index.
- **Heterogeneous Device Support**: Compatibility with devices of varying capabilities.

## System Architecture
The system comprises:
- **Server**: Manages connections, chunk distribution, and query routing.
- **Clients**: Store data chunks, send heartbeat signals, and handle local queries.
- **Master Index**: Tracks the location of each data chunk for efficient retrieval.

### High-Level Workflow
1. **File Upload**: Large files are divided into smaller chunks.
2. **Chunk Distribution**: Chunks are assigned to client nodes based on available space.
3. **Heartbeat Monitoring**: Nodes periodically send status signals to the server.
4. **Redistribution**: In case of failure, data is reassigned to active nodes.
5. **Query Processing**: Clients retrieve specific records using the master index.

## Prerequisites
- Python 3.x
- Required Python libraries:
  - `requests`
  - `tabulate`
  - `tqdm`

Install dependencies with:
```bash
pip install requests tabulate tqdm
```

## Clone the Repository 
```bash
git clone https://github.com/yddesai/Distributed-Memory-Allocator.git
```
```bash
cd Distributed-Memory-Allocator
```

## Setup and Installation 
1. Ensure Python dependecies are installed 
```bash 
pip install -r requirements.txt
```
2. Enable Portforwarding on your Access Point
- You can do this on the admin page or mobile application of your Internet Service Provider.
  For our project, enable port forwarding on ports ```8085, 8081, 8080, and 8084```. 
  This is what it looks like if you are using Xfinity.

  ![image](https://github.com/user-attachments/assets/621aa59d-f1f3-48a8-8728-8f39d997328c)


## Running the project 
1. Clone the repository on the coordinator ( i.e the central server node ) and  on the client nodes. 
2. For the server node, use this command
```bash 
python aws_server.py
```
3. For the client node, use this comamnd
```bash 
python client.py
```

### Commands and Usage
After running the client code, enter the memory capacity on the that particular client or node.

Client Commands
- distribute: Upload a JSON file for distribution to connected nodes.
- metrics: Display current system metrics, including storage usage and connection status.
- query: Retrieve a specific record by its unique ID.
- exit: Safely terminate the client application.
### Client / Server Metrics
- Track connected devices, their storage usage, and heartbeat status.
- Verify chunk distribution and redistribution processes.
  
This is what the client metrics looks like as a table in command line.

![image](https://github.com/user-attachments/assets/0d0e99c9-7737-4588-8d29-279087bdb980)

Project Demo Video Link (Needs SCU Email-ID to Access): [Video Link](https://drive.google.com/file/d/1GxMpvzOdt_qHy2LP8kfetHgYhTTjJgO-/view?usp=drive_link)
