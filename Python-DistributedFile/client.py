# import threading
# import time
# import os
# import shutil
# import json
# import requests
# from http.server import BaseHTTPRequestHandler, HTTPServer
# from tqdm import tqdm

# first_heartbeat = True  # To track if the first heartbeat has been sent

# def send_heartbeat(server_ip):
#     global first_heartbeat
#     server_url = f"http://{server_ip}:8080"
#     heartbeat_data = {}

#     while True:
#         try:
#             headers = {"X-Request-Type": "heartbeat"}
#             response = requests.post(server_url, headers=headers, timeout=10)
#             if first_heartbeat:
#                 print("[Client] First heartbeat sent to server.")
#                 first_heartbeat = False

#             if response.status_code != 200:
#                 print(f"[Client] Failed to send heartbeat. Status code: {response.status_code}")

#         except Exception as e:
#             print(f"[Client] Error sending heartbeat: {e}")

#         time.sleep(30)  # Send heartbeat every 30 seconds

# def connect_to_server(server_ip, free_space):
#     server_url = f"http://{server_ip}:8080"
#     data = {"free_space": free_space}

#     try:
#         headers = {"X-Request-Type": "connection"}
#         print(f"[Client] Connecting to server at {server_ip}...")
#         response = requests.post(server_url, json=data, headers=headers, timeout=10)

#         if response.status_code == 200:
#             print("[Client] Connected to server successfully.")

#             # Start the heartbeat thread after connecting
#             threading.Thread(target=send_heartbeat, args=(server_ip,), daemon=True).start()
#         else:
#             print(f"[Client] Failed to connect. Server responded with status code: {response.status_code}")

#     except Exception as e:
#         print(f"[Client] An error occurred: {e}")

# def start_client_server():
#     """
#     Start a simple HTTP server to handle incoming chunk transfers.
#     """
#     class ClientRequestHandler(BaseHTTPRequestHandler):
#         total_chunks = 0
#         chunks_received = 0
#         pbar = None

#         def do_POST(self):
#             content_length = int(self.headers['Content-Length'])
#             post_data = self.rfile.read(content_length)
#             data = json.loads(post_data)

#             request_type = self.headers.get("X-Request-Type")

#             if request_type == "initiate_transfer":
#                 # Clear the received_files folder
#                 received_folder = "received_files"
#                 if os.path.exists(received_folder):
#                     shutil.rmtree(received_folder)
#                     print(f"[Client] Cleared existing data in {received_folder}.")
#                 os.makedirs(received_folder, exist_ok=True)
#                 print("[Client] Ready to receive new chunks.")

#                 # Get total_chunks for progress bar
#                 self.total_chunks = data.get("total_chunks", 0)
#                 self.chunks_received = 0
#                 if self.pbar:
#                     self.pbar.close()
#                 self.pbar = tqdm(total=self.total_chunks, desc="Receiving Chunks", unit="chunk")

#                 self.send_response(200)
#                 self.end_headers()
#                 self.wfile.write(b"Ready for new transfer")

#             elif request_type == "chunk_transfer":
#                 chunk_name = data["chunk"]
#                 chunk_data = data["data"]
#                 self.handle_chunk_transfer(chunk_name, chunk_data)

#                 self.chunks_received += 1
#                 if self.pbar:
#                     self.pbar.update(1)

#                 self.send_response(200)
#                 self.end_headers()
#                 self.wfile.write(b"Chunk received successfully")

#                 if self.chunks_received == self.total_chunks:
#                     if self.pbar:
#                         self.pbar.close()
#                     print("[Client] All chunks received and saved to received_files.")
#                     print("[Client] File distribution completed successfully.")

#             else:
#                 self.send_response(400)
#                 self.end_headers()
#                 self.wfile.write(b"Invalid request")

#         def handle_chunk_transfer(self, chunk_name, chunk_data):
#             """
#             Handle the received chunk data and save it in the `received_files` folder.
#             """
#             received_folder = "received_files"
#             chunk_path = os.path.join(received_folder, chunk_name)
#             with open(chunk_path, "w") as f:
#                 json.dump(chunk_data, f, indent=4)

#             # Update client_local_index
#             client_local_index_path = os.path.join(received_folder, "client_local_index.json")
#             local_index = []
#             if os.path.exists(client_local_index_path):
#                 with open(client_local_index_path, "r") as f:
#                     local_index = json.load(f)

#             local_index.append({
#                 "chunk": chunk_name,
#                 "start_id": chunk_data[0]["id"],
#                 "end_id": chunk_data[-1]["id"]
#             })

#             with open(client_local_index_path, "w") as f:
#                 json.dump(local_index, f, indent=4)

#         def log_message(self, format, *args):
#             return  # Suppress default logging

#     server_address = ('', 8081)
#     httpd = HTTPServer(server_address, ClientRequestHandler)
#     print("[Client] Starting local server on port 8081 to receive chunks.")
#     httpd.serve_forever()

# def send_json_file_to_server(server_ip, folder_path):
#     """
#     Send a JSON file to the AWS server for distribution using streaming to avoid blocking.
#     """
#     server_url = f"http://{server_ip}:8080"
#     try:
#         # Find the JSON file
#         files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
#         if not files:
#             print("[Client] No JSON file found in the specified folder.")
#             return

#         json_file_path = os.path.join(folder_path, files[0])
#         file_size = os.path.getsize(json_file_path)

#         print("[Client] Sending JSON file to the server for distribution...")

#         class ProgressFileWrapper:
#             def __init__(self, fileobj, total_size, progress_bar):
#                 self.fileobj = fileobj
#                 self.total_size = total_size
#                 self.progress_bar = progress_bar

#             def __iter__(self):
#                 return self

#             def __next__(self):
#                 data = self.fileobj.read(1024 * 1024)  # 1 MB chunks
#                 if not data:
#                     self.progress_bar.close()
#                     raise StopIteration
#                 self.progress_bar.update(len(data))
#                 return data

#             def read(self, size):
#                 data = self.fileobj.read(size)
#                 if not data:
#                     return b''
#                 self.progress_bar.update(len(data))
#                 return data

#         with open(json_file_path, 'rb') as f, tqdm(total=file_size, desc="Uploading JSON File", unit="B", unit_scale=True) as pbar:
#             wrapped_file = ProgressFileWrapper(f, file_size, pbar)
#             headers = {"X-Request-Type": "file_transfer", "Content-Length": str(file_size)}
#             response = requests.post(
#                 server_url,
#                 data=wrapped_file,
#                 headers=headers,
#                 timeout=300  # Adjust timeout as needed
#             )

#         if response.status_code == 200:
#             print("[Client] JSON file sent successfully.")
#         else:
#             print(f"[Client] Failed to send data. Server responded with status code: {response.status_code}")

#     except Exception as e:
#         print(f"[Client] Error sending JSON file: {e}")

# if __name__ == "__main__":
#     # Replace with your AWS server's public IP
#     server_ip = "18.144.165.108"
#     free_space = int(input("Enter free space in MB (e.g., 100, 200): "))

#     # Step 1: Connect to the server
#     connect_to_server(server_ip, free_space)

#     # Step 2: Start client server to receive chunks
#     threading.Thread(target=start_client_server, daemon=True).start()

#     # Step 3: Command interface
#     while True:
#         command = input("Enter command (type 'distribute' to send JSON, or 'exit' to quit): ").strip().lower()
#         if command == "distribute":
#             folder_path = input("Enter the folder path containing the JSON file: ").strip()
#             send_json_file_to_server(server_ip, folder_path)
#         elif command == "exit":
#             print("[Client] Exiting...")
#             break
#         else:
#             print("Invalid command. Please try again.")
############################################################################################################################################


# import threading
# import time
# import os
# import shutil
# import json
# import requests
# from http.server import BaseHTTPRequestHandler, HTTPServer
# from tqdm import tqdm

# first_heartbeat = True  # To track if the first heartbeat has been sent

# def send_heartbeat(server_ip):
#     global first_heartbeat
#     server_url = f"http://{server_ip}:8080"
#     heartbeat_data = {}

#     while True:
#         try:
#             headers = {"X-Request-Type": "heartbeat"}
#             response = requests.post(server_url, headers=headers, timeout=10)
#             if first_heartbeat:
#                 print("[Client] First heartbeat sent to server.")
#                 first_heartbeat = False

#             if response.status_code != 200:
#                 print(f"[Client] Failed to send heartbeat. Status code: {response.status_code}")

#         except Exception as e:
#             print(f"[Client] Error sending heartbeat: {e}")

#         time.sleep(30)  # Send heartbeat every 30 seconds

# def connect_to_server(server_ip, free_space):
#     server_url = f"http://{server_ip}:8080"
#     data = {"free_space": free_space}

#     try:
#         headers = {"X-Request-Type": "connection"}
#         print(f"[Client] Connecting to server at {server_ip}...")
#         response = requests.post(server_url, json=data, headers=headers, timeout=10)

#         if response.status_code == 200:
#             print("[Client] Connected to server successfully.")

#             # Start the heartbeat thread after connecting
#             threading.Thread(target=send_heartbeat, args=(server_ip,), daemon=True).start()
#         else:
#             print(f"[Client] Failed to connect. Server responded with status code: {response.status_code}")

#     except Exception as e:
#         print(f"[Client] An error occurred: {e}")

# class ClientRequestHandler(BaseHTTPRequestHandler):
#     def do_POST(self):
#         content_length = int(self.headers['Content-Length'])
#         post_data = self.rfile.read(content_length)
#         data = json.loads(post_data)

#         request_type = self.headers.get("X-Request-Type")

#         if request_type == "initiate_transfer":
#             # Clear the received_files folder
#             received_folder = "received_files"
#             if os.path.exists(received_folder):
#                 shutil.rmtree(received_folder)
#                 print(f"[Client] Cleared existing data in {received_folder}.")
#             os.makedirs(received_folder, exist_ok=True)
#             print("[Client] Ready to receive new chunks.")

#             # Get total_chunks for progress bar
#             self.server.total_chunks = data.get("total_chunks", 0)
#             self.server.chunks_received = 0
#             self.server.transfer_complete.clear()

#             self.send_response(200)
#             self.end_headers()
#             self.wfile.write(b"Ready for new transfer")

#         elif request_type == "chunk_transfer":
#             chunk_name = data["chunk"]
#             chunk_data = data["data"]
#             self.handle_chunk_transfer(chunk_name, chunk_data)

#             # Update shared variable
#             self.server.chunks_received += 1

#             if self.server.chunks_received == self.server.total_chunks:
#                 self.server.transfer_complete.set()

#             self.send_response(200)
#             self.end_headers()
#             self.wfile.write(b"Chunk received successfully")

#         else:
#             self.send_response(400)
#             self.end_headers()
#             self.wfile.write(b"Invalid request")

#     def handle_chunk_transfer(self, chunk_name, chunk_data):
#         """
#         Handle the received chunk data and save it in the `received_files` folder.
#         """
#         received_folder = "received_files"
#         chunk_path = os.path.join(received_folder, chunk_name)
#         with open(chunk_path, "w") as f:
#             json.dump(chunk_data, f, indent=4)

#         # Update client_local_index
#         client_local_index_path = os.path.join(received_folder, "client_local_index.json")
#         local_index = []
#         if os.path.exists(client_local_index_path):
#             with open(client_local_index_path, "r") as f:
#                 local_index = json.load(f)

#         local_index.append({
#             "chunk": chunk_name,
#             "start_id": chunk_data[0]["id"],
#             "end_id": chunk_data[-1]["id"]
#         })

#         with open(client_local_index_path, "w") as f:
#             json.dump(local_index, f, indent=4)

#     def log_message(self, format, *args):
#         return  # Suppress default logging

# def send_json_file_to_server(server_ip, folder_path):
#     """
#     Send a JSON file to the AWS server for distribution using streaming to avoid blocking.
#     """
#     server_url = f"http://{server_ip}:8080"
#     try:
#         # Find the JSON file
#         files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
#         if not files:
#             print("[Client] No JSON file found in the specified folder.")
#             return

#         json_file_path = os.path.join(folder_path, files[0])
#         file_size = os.path.getsize(json_file_path)

#         print("[Client] Sending JSON file to the server for distribution...")

#         class ProgressFileWrapper:
#             def __init__(self, fileobj, total_size, progress_bar):
#                 self.fileobj = fileobj
#                 self.total_size = total_size
#                 self.progress_bar = progress_bar

#             def __iter__(self):
#                 return self

#             def __next__(self):
#                 data = self.fileobj.read(1024 * 1024)  # 1 MB chunks
#                 if not data:
#                     self.progress_bar.close()
#                     raise StopIteration
#                 self.progress_bar.update(len(data))
#                 return data

#             def read(self, size):
#                 data = self.fileobj.read(size)
#                 if not data:
#                     return b''
#                 self.progress_bar.update(len(data))
#                 return data

#         with open(json_file_path, 'rb') as f, tqdm(total=file_size, desc="Uploading JSON File", unit="B", unit_scale=True) as pbar:
#             wrapped_file = ProgressFileWrapper(f, file_size, pbar)
#             headers = {"X-Request-Type": "file_transfer", "Content-Length": str(file_size)}
#             response = requests.post(
#                 server_url,
#                 data=wrapped_file,
#                 headers=headers,
#                 timeout=300  # Adjust timeout as needed
#             )

#         if response.status_code == 200:
#             print("[Client] JSON file sent successfully.")
#         else:
#             print(f"[Client] Failed to send data. Server responded with status code: {response.status_code}")

#     except Exception as e:
#         print(f"[Client] Error sending JSON file: {e}")

# def monitor_progress(server):
#     # Wait until total_chunks is set
#     while server.total_chunks == 0:
#         time.sleep(0.1)
#     with tqdm(total=server.total_chunks, desc="Receiving Chunks", unit="chunk") as pbar:
#         while not server.transfer_complete.is_set():
#             pbar.n = server.chunks_received
#             pbar.refresh()
#             time.sleep(0.1)
#         # Ensure progress bar reaches 100%
#         pbar.n = server.total_chunks
#         pbar.refresh()
#         pbar.close()
#     print("[Client] All chunks received and saved to received_files.")
#     print("[Client] File distribution completed successfully.")

# if __name__ == "__main__":
#     # Replace with your AWS server's public IP
#     server_ip = "18.144.165.108"
#     free_space = int(input("Enter free space in MB (e.g., 100, 200): "))

#     # Step 1: Connect to the server
#     connect_to_server(server_ip, free_space)

#     # Step 2: Set up the server to receive chunks
#     server_address = ('', 8081)
#     server = HTTPServer(server_address, ClientRequestHandler)

#     # Initialize shared variables
#     server.total_chunks = 0
#     server.chunks_received = 0
#     server.transfer_complete = threading.Event()

#     # Start the client server in a separate thread
#     threading.Thread(target=server.serve_forever, daemon=True).start()
#     print("[Client] Starting local server on port 8081 to receive chunks.")

#     # Step 3: Command interface
#     while True:
#         command = input("Enter command (type 'distribute' to send JSON, or 'exit' to quit): ").strip().lower()
#         if command == "distribute":
#             folder_path = input("Enter the folder path containing the JSON file: ").strip()
#             send_json_file_to_server(server_ip, folder_path)
#             # Start progress monitoring
#             monitor_progress(server)
#         elif command == "exit":
#             print("[Client] Exiting...")
#             server.shutdown()  # Stop the server
#             break
#         else:
#             print("Invalid command. Please try again.")


############################################################################################################################

# import threading
# import time
# import os
# import shutil
# import json
# import requests
# from http.server import BaseHTTPRequestHandler, HTTPServer
# from tqdm import tqdm
# from tabulate import tabulate

# first_heartbeat = True
# client_metrics = {
#     "total_space": 0,
#     "used_space": 0,
#     "free_space": 0,
#     "chunks_stored": 0,
#     "connection_status": "Disconnected"
# }

# def calculate_chunk_size(chunk_path):
#     """Calculate the size of a chunk file in MB"""
#     return os.path.getsize(chunk_path) / (1024 * 1024)

# def update_client_metrics():
#     """Update client metrics based on current state"""
#     global client_metrics
#     received_folder = "received_files"
    
#     # Calculate used space from chunks
#     used_space = 0
#     num_chunks = 0
#     if os.path.exists(received_folder):
#         for chunk_file in os.listdir(received_folder):
#             if chunk_file.endswith('.json') and chunk_file != "client_local_index.json":
#                 chunk_path = os.path.join(received_folder, chunk_file)
#                 used_space += calculate_chunk_size(chunk_path)
#                 num_chunks += 1

#     client_metrics["used_space"] = round(used_space, 2)
#     client_metrics["free_space"] = round(client_metrics["total_space"] - used_space, 2)
#     client_metrics["chunks_stored"] = num_chunks

# def print_client_metrics():
#     """Print formatted client metrics"""
#     headers = ["Metric", "Value"]
#     table_data = [
#         ["Total Space (MB)", client_metrics["total_space"]],
#         ["Used Space (MB)", client_metrics["used_space"]],
#         ["Free Space (MB)", client_metrics["free_space"]],
#         ["Chunks Stored", client_metrics["chunks_stored"]],
#         ["Connection Status", client_metrics["connection_status"]]
#     ]
    
#     print("\n=== Client Metrics ===")
#     print(tabulate(table_data, headers=headers, tablefmt="grid"))
#     print("===================\n")

# def send_heartbeat(server_ip):
#     global first_heartbeat, client_metrics
#     server_url = f"http://{server_ip}:8080"
#     heartbeat_data = {}

#     while True:
#         try:
#             headers = {"X-Request-Type": "heartbeat"}
#             response = requests.post(server_url, headers=headers, timeout=10)
            
#             if first_heartbeat:
#                 print("[Client] First heartbeat sent to server.")
#                 first_heartbeat = False
#                 client_metrics["connection_status"] = "Connected"
#                 print_client_metrics()

#             if response.status_code != 200:
#                 print(f"[Client] Failed to send heartbeat. Status code: {response.status_code}")
#                 client_metrics["connection_status"] = "Connection Error"
            
#         except Exception as e:
#             print(f"[Client] Error sending heartbeat: {e}")
#             client_metrics["connection_status"] = "Connection Error"

#         time.sleep(30)  # Send heartbeat every 30 seconds

# def connect_to_server(server_ip, free_space):
#     global client_metrics
#     server_url = f"http://{server_ip}:8080"
#     client_metrics["total_space"] = free_space
#     client_metrics["free_space"] = free_space
#     data = {"free_space": free_space}

#     try:
#         headers = {"X-Request-Type": "connection"}
#         print(f"[Client] Connecting to server at {server_ip}...")
#         response = requests.post(server_url, json=data, headers=headers, timeout=10)

#         if response.status_code == 200:
#             print("[Client] Connected to server successfully.")
#             client_metrics["connection_status"] = "Connected"

#             # Start the heartbeat thread after connecting
#             threading.Thread(target=send_heartbeat, args=(server_ip,), daemon=True).start()
#         else:
#             print(f"[Client] Failed to connect. Server responded with status code: {response.status_code}")
#             client_metrics["connection_status"] = "Connection Failed"

#     except Exception as e:
#         print(f"[Client] An error occurred: {e}")
#         client_metrics["connection_status"] = "Connection Error"

#     print_client_metrics()


# class ClientRequestHandler(BaseHTTPRequestHandler):
#     def do_POST(self):
#         content_length = int(self.headers['Content-Length'])
#         post_data = self.rfile.read(content_length)
#         data = json.loads(post_data) if content_length > 0 else {}

#         request_type = self.headers.get("X-Request-Type")

#         if request_type == "initiate_transfer":
#             self.handle_initiate_transfer(data)
#         elif request_type == "chunk_transfer":
#             self.handle_chunk_transfer(data)
#         elif request_type == "redistribution_failed":
#             self.handle_redistribution_failed()
#         elif request_type == "clear_chunks":
#             self.handle_clear_chunks()
#         else:
#             self.send_response(400)
#             self.end_headers()
#             self.wfile.write(b"Invalid request")

#     def handle_initiate_transfer(self, data):
#         received_folder = "received_files"
#         if os.path.exists(received_folder):
#             shutil.rmtree(received_folder)
#             print(f"[Client] Cleared existing data in {received_folder}.")
#         os.makedirs(received_folder, exist_ok=True)
#         print("[Client] Ready to receive new chunks.")

#         self.server.total_chunks = data.get("total_chunks", 0)
#         self.server.chunks_received = 0
#         self.server.transfer_complete.clear()

#         self.send_response(200)
#         self.end_headers()
#         self.wfile.write(b"Ready for new transfer")

#     def handle_chunk_transfer(self, data):
#         chunk_name = data["chunk"]
#         chunk_data = data["data"]
#         self.save_chunk(chunk_name, chunk_data)

#         self.server.chunks_received += 1
#         if self.server.chunks_received == self.server.total_chunks:
#             self.server.transfer_complete.set()
#             update_client_metrics()
#             print_client_metrics()

#         self.send_response(200)
#         self.end_headers()
#         self.wfile.write(b"Chunk received successfully")

#     def handle_redistribution_failed(self):
#         print("\n[Client] WARNING: Server redistribution failed due to insufficient space!")
#         print("[Client] Some chunks may be temporarily unavailable.")
#         self.send_response(200)
#         self.end_headers()

#     def handle_clear_chunks(self):
#         print("\n[Client] Clearing all chunks for redistribution...")
#         received_folder = "received_files"
#         if os.path.exists(received_folder):
#             shutil.rmtree(received_folder)
#             os.makedirs(received_folder)
#         update_client_metrics()
#         print_client_metrics()
#         self.send_response(200)
#         self.end_headers()

#     def save_chunk(self, chunk_name, chunk_data):
#         received_folder = "received_files"
#         chunk_path = os.path.join(received_folder, chunk_name)
#         with open(chunk_path, "w") as f:
#             json.dump(chunk_data, f, indent=4)

#         client_local_index_path = os.path.join(received_folder, "client_local_index.json")
#         local_index = []
#         if os.path.exists(client_local_index_path):
#             with open(client_local_index_path, "r") as f:
#                 local_index = json.load(f)

#         local_index.append({
#             "chunk": chunk_name,
#             "start_id": chunk_data[0]["id"],
#             "end_id": chunk_data[-1]["id"]
#         })

#         with open(client_local_index_path, "w") as f:
#             json.dump(local_index, f, indent=4)

#     def log_message(self, format, *args):
#         return  # Suppress default logging


# def send_json_file_to_server(server_ip, folder_path):
#     server_url = f"http://{server_ip}:8080"
#     try:
#         files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
#         if not files:
#             print("[Client] No JSON file found in the specified folder.")
#             return

#         json_file_path = os.path.join(folder_path, files[0])
#         file_size = os.path.getsize(json_file_path)

#         print("[Client] Sending JSON file to the server for distribution...")

#         class ProgressFileWrapper:
#             def __init__(self, fileobj, total_size, progress_bar):
#                 self.fileobj = fileobj
#                 self.total_size = total_size
#                 self.progress_bar = progress_bar

#             def __iter__(self):
#                 return self

#             def __next__(self):
#                 data = self.fileobj.read(1024 * 1024)  # 1 MB chunks
#                 if not data:
#                     self.progress_bar.close()
#                     raise StopIteration
#                 self.progress_bar.update(len(data))
#                 return data

#             def read(self, size):
#                 data = self.fileobj.read(size)
#                 if not data:
#                     return b''
#                 self.progress_bar.update(len(data))
#                 return data

#         with open(json_file_path, 'rb') as f, tqdm(total=file_size, desc="Uploading JSON File", 
#                                                   unit="B", unit_scale=True) as pbar:
#             wrapped_file = ProgressFileWrapper(f, file_size, pbar)
#             headers = {"X-Request-Type": "file_transfer", "Content-Length": str(file_size)}
#             response = requests.post(
#                 server_url,
#                 data=wrapped_file,
#                 headers=headers,
#                 timeout=300
#             )

#         if response.status_code == 200:
#             print("[Client] JSON file sent successfully.")
#         else:
#             print(f"[Client] Failed to send data. Server responded with status code: {response.status_code}")

#     except Exception as e:
#         print(f"[Client] Error sending JSON file: {e}")

# def monitor_progress(server):
#     while server.total_chunks == 0:
#         time.sleep(0.1)
#     with tqdm(total=server.total_chunks, desc="Receiving Chunks", unit="chunk") as pbar:
#         while not server.transfer_complete.is_set():
#             pbar.n = server.chunks_received
#             pbar.refresh()
#             time.sleep(0.1)
#         pbar.n = server.total_chunks
#         pbar.refresh()
#         pbar.close()
#     print("[Client] All chunks received and saved to received_files.")
#     print("[Client] File distribution completed successfully.")
#     update_client_metrics()
#     print_client_metrics()

# if __name__ == "__main__":
#     server_ip = input("Enter the server IP address: ").strip()
#     free_space = int(input("Enter free space in MB (e.g., 100, 200): "))

#     # Step 1: Connect to the server
#     connect_to_server(server_ip, free_space)

#     # Step 2: Set up the server to receive chunks
#     server_address = ('', 8081)
#     server = HTTPServer(server_address, ClientRequestHandler)

#     # Initialize shared variables
#     server.total_chunks = 0
#     server.chunks_received = 0
#     server.transfer_complete = threading.Event()

#     # Start the client server in a separate thread
#     threading.Thread(target=server.serve_forever, daemon=True).start()
#     print("[Client] Starting local server on port 8081 to receive chunks.")

#     # Step 3: Command interface
#     while True:
#         print("\nAvailable commands:")
#         print("1. distribute - Send JSON file for distribution")
#         print("2. metrics   - Display current metrics")
#         print("3. exit      - Exit the client")
        
#         command = input("\nEnter command: ").strip().lower()
        
#         if command == "distribute":
#             folder_path = input("Enter the folder path containing the JSON file: ").strip()
#             send_json_file_to_server(server_ip, folder_path)
#             monitor_progress(server)
#         elif command == "metrics":
#             update_client_metrics()
#             print_client_metrics()
#         elif command == "exit":
#             print("[Client] Exiting...")
#             server.shutdown()
#             break
#         else:
#             print("Invalid command. Please try again.")

#########################################################################################

# import threading
# import time
# import os
# import shutil
# import json
# import requests
# import sys 
# from http.server import BaseHTTPRequestHandler, HTTPServer
# from tqdm import tqdm
# from tabulate import tabulate

# # Server ports configuration
# MAIN_PORT = 8080        # Command & Control
# HEARTBEAT_PORT = 8081   # Heartbeat traffic
# UPLOAD_PORT = 8082      # File uploads
# DOWNLOAD_PORT = 8083    # Chunk distribution
# CLIENT_PORT = 8081      # Local client port for receiving chunks

# # Global variables
# first_heartbeat = True
# server_config = {
#     "server_ip": None,
#     "device_id": None,
#     "heartbeat_port": None,
#     "upload_port": None,
#     "download_port": None
# }

# client_metrics = {
#     "total_space": 0,
#     "used_space": 0,
#     "free_space": 0,
#     "chunks_stored": 0,
#     "connection_status": "Disconnected",
#     "last_updated": None,
#     "current_transfer": None,
#     "failed_chunks": []
# }

# def calculate_chunk_size(chunk_path):
#     """Calculate the size of a chunk file in MB"""
#     try:
#         return os.path.getsize(chunk_path) / (1024 * 1024)
#     except Exception as e:
#         print(f"[Client] Error calculating chunk size for {chunk_path}: {e}")
#         return 0

# def update_client_metrics():
#     """Update client metrics based on current state"""
#     global client_metrics
#     received_folder = "received_files"
    
#     used_space = 0
#     num_chunks = 0
#     if os.path.exists(received_folder):
#         for chunk_file in os.listdir(received_folder):
#             if chunk_file.endswith('.json') and chunk_file != "client_local_index.json":
#                 chunk_path = os.path.join(received_folder, chunk_file)
#                 used_space += calculate_chunk_size(chunk_path)
#                 num_chunks += 1

#     client_metrics["used_space"] = round(used_space, 2)
#     client_metrics["free_space"] = round(client_metrics["total_space"] - used_space, 2)
#     client_metrics["chunks_stored"] = num_chunks
#     client_metrics["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")

# def print_client_metrics():
#     """Print formatted client metrics"""
#     headers = ["Metric", "Value"]
#     table_data = [
#         ["Device ID", server_config["device_id"] or "Not assigned"],
#         ["Total Space (MB)", client_metrics["total_space"]],
#         ["Used Space (MB)", client_metrics["used_space"]],
#         ["Free Space (MB)", client_metrics["free_space"]],
#         ["Chunks Stored", client_metrics["chunks_stored"]],
#         ["Connection Status", client_metrics["connection_status"]],
#         ["Last Updated", client_metrics["last_updated"] or "Never"],
#         ["Current Transfer", client_metrics["current_transfer"] or "None"],
#         ["Failed Chunks", len(client_metrics["failed_chunks"])]
#     ]
    
#     print("\n=== Client Metrics ===")
#     print(tabulate(table_data, headers=headers, tablefmt="grid"))
#     if client_metrics["failed_chunks"]:
#         print("\nFailed Chunks:")
#         for chunk in client_metrics["failed_chunks"]:
#             print(f"  - {chunk}")
#     print("===================\n")

# def send_heartbeat():
#     """Send periodic heartbeats to server"""
#     global first_heartbeat, client_metrics
#     heartbeat_url = f"http://{server_config['server_ip']}:{server_config['heartbeat_port']}"

#     while True:
#         try:
#             headers = {"X-Request-Type": "heartbeat"}
#             response = requests.post(heartbeat_url, headers=headers, timeout=10)
            
#             if first_heartbeat and response.status_code == 200:
#                 print("[Client] First heartbeat sent to server.")
#                 first_heartbeat = False
#                 client_metrics["connection_status"] = "Connected"
#                 print_client_metrics()

#             if response.status_code != 200:
#                 client_metrics["connection_status"] = "Connection Error"
            
#         except Exception as e:
#             print(f"[Client] Error sending heartbeat: {e}")
#             client_metrics["connection_status"] = "Connection Error"

#         time.sleep(30)

# def connect_to_server(server_ip, free_space):
#     """Connect to server and get configuration"""
#     global client_metrics, server_config
#     server_config["server_ip"] = server_ip
#     client_metrics["total_space"] = free_space
#     client_metrics["free_space"] = free_space
    
#     server_url = f"http://{server_ip}:{MAIN_PORT}"
#     data = {"free_space": free_space}

#     retry_count = 0
#     max_retries = 3
#     while retry_count < max_retries:
#         try:
#             headers = {"X-Request-Type": "connection"}
#             print(f"[Client] Connecting to server at {server_ip}...")
#             response = requests.post(server_url, json=data, headers=headers, timeout=10)

#             if response.status_code == 200:
#                 config_data = response.json()
#                 server_config.update({
#                     "device_id": config_data["device_id"],
#                     "heartbeat_port": config_data["heartbeat_port"],
#                     "upload_port": config_data["upload_port"],
#                     "download_port": config_data["download_port"]
#                 })
                
#                 print("[Client] Connected to server successfully.")
#                 print(f"[Client] Assigned Device ID: {server_config['device_id']}")
#                 client_metrics["connection_status"] = "Connected"
                
#                 # Start heartbeat in separate thread
#                 threading.Thread(target=send_heartbeat, daemon=True).start()
#                 break
#             else:
#                 print(f"[Client] Failed to connect. Server responded with status code: {response.status_code}")
#                 client_metrics["connection_status"] = "Connection Failed"
#                 retry_count += 1

#         except Exception as e:
#             print(f"[Client] Connection attempt {retry_count + 1} failed: {e}")
#             client_metrics["connection_status"] = "Connection Error"
#             retry_count += 1
#             if retry_count < max_retries:
#                 print(f"[Client] Retrying in 2 seconds...")
#                 time.sleep(2)

#     print_client_metrics()

# class ClientRequestHandler(BaseHTTPRequestHandler):
#     def do_POST(self):
#         try:
#             content_length = int(self.headers.get('Content-Length', 0))
#             post_data = self.rfile.read(content_length)
#             data = json.loads(post_data) if content_length > 0 else {}

#             if self.path == '/notify_chunks':
#                 self.handle_notify_chunks(data)
#             elif self.path == '/notify_redistribution':
#                 self.handle_notify_redistribution(data)
#             elif self.path == '/redistribution_failed':
#                 self.handle_redistribution_failed()
#             elif self.path == '/clear_chunks':
#                 self.handle_clear_chunks()
#             else:
#                 self.send_response(404)
#                 self.end_headers()
#                 self.wfile.write(b"Invalid endpoint")
                
#         except Exception as e:
#             print(f"[Client] Error handling request: {e}")
#             self.send_response(500)
#             self.end_headers()
#             self.wfile.write(str(e).encode())

#     def handle_notify_chunks(self, data):
#         """Handle notification of new chunk assignments"""
#         chunks = data.get("chunks", [])
#         if not chunks:
#             print("[Client] Received empty chunk list")
#             return

#         print(f"[Client] Server assigned {len(chunks)} chunks")
#         self.download_chunks(chunks)
        
#         self.send_response(200)
#         self.end_headers()

#     def handle_notify_redistribution(self, data):
#         """Handle redistribution notification"""
#         chunks = data.get("chunks", [])
#         print(f"[Client] Received redistribution assignment for {len(chunks)} chunks")
#         self.download_chunks(chunks)
        
#         self.send_response(200)
#         self.end_headers()

#     def handle_redistribution_failed(self):
#         """Handle redistribution failure notification"""
#         print("\n[Client] WARNING: Server redistribution failed due to insufficient space!")
#         print("[Client] Some chunks may be temporarily unavailable.")
#         client_metrics["connection_status"] = "Redistribution Failed"
#         print_client_metrics()
        
#         self.send_response(200)
#         self.end_headers()

#     def handle_clear_chunks(self):
#         """Handle request to clear local chunks"""
#         print("\n[Client] Clearing all chunks for redistribution...")
#         received_folder = "received_files"
#         if os.path.exists(received_folder):
#             shutil.rmtree(received_folder)
#             os.makedirs(received_folder)
        
#         client_metrics["current_transfer"] = "Awaiting redistribution"
#         update_client_metrics()
#         print_client_metrics()
        
#         self.send_response(200)
#         self.end_headers()

#     def download_chunks(self, chunks):
#         """Download assigned chunks from server"""
#         download_url = f"http://{server_config['server_ip']}:{server_config['download_port']}"
        
#         for chunk in tqdm(chunks, desc="Downloading chunks", unit="chunk"):
#             try:
#                 response = requests.get(f"{download_url}/download_chunk?name={chunk}", timeout=30)
#                 if response.status_code == 200:
#                     self.save_chunk(chunk, response.json())
#                 else:
#                     print(f"[Client] Failed to download chunk {chunk}")
#                     client_metrics["failed_chunks"].append(chunk)
#             except Exception as e:
#                 print(f"[Client] Error downloading chunk {chunk}: {e}")
#                 client_metrics["failed_chunks"].append(chunk)

#         update_client_metrics()
#         print_client_metrics()

#     def save_chunk(self, chunk_name, chunk_data):
#         """Save received chunk to disk"""
#         received_folder = "received_files"
#         chunk_path = os.path.join(received_folder, chunk_name)
        
#         # Atomic write using temporary file
#         temp_path = chunk_path + '.tmp'
#         try:
#             with open(temp_path, "w") as f:
#                 json.dump(chunk_data, f, indent=4)
#             os.replace(temp_path, chunk_path)
            
#             self.update_local_index(chunk_name, chunk_data)
        
#         except Exception as e:
#             if os.path.exists(temp_path):
#                 os.remove(temp_path)
#             raise e

#     def update_local_index(self, chunk_name, chunk_data):
#         """Update local index with new chunk information"""
#         received_folder = "received_files"
#         index_path = os.path.join(received_folder, "client_local_index.json")
        
#         try:
#             local_index = []
#             if os.path.exists(index_path):
#                 with open(index_path, "r") as f:
#                     local_index = json.load(f)

#             local_index.append({
#                 "chunk": chunk_name,
#                 "start_id": chunk_data[0]["id"],
#                 "end_id": chunk_data[-1]["id"],
#                 "received_time": time.strftime("%Y-%m-%d %H:%M:%S")
#             })

#             temp_path = index_path + '.tmp'
#             with open(temp_path, "w") as f:
#                 json.dump(local_index, f, indent=4)
#             os.replace(temp_path, index_path)

#         except Exception as e:
#             print(f"[Client] Error updating local index: {e}")
#             if os.path.exists(temp_path):
#                 os.remove(temp_path)

#     def log_message(self, format, *args):
#         return

# def send_json_file_to_server(server_ip, folder_path):
#     """Send JSON file to server for distribution"""
#     upload_url = f"http://{server_config['server_ip']}:{server_config['upload_port']}"
#     retry_count = 0
#     max_retries = 3

#     while retry_count < max_retries:
#         try:
#             files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
#             if not files:
#                 print("[Client] No JSON file found in the specified folder.")
#                 return False

#             json_file_path = os.path.join(folder_path, files[0])
#             file_size = os.path.getsize(json_file_path)

#             print("[Client] Preparing to send JSON file...")
#             print(f"File size: {file_size / (1024*1024):.2f} MB")

#             with open(json_file_path, 'rb') as f, tqdm(total=file_size, desc="Uploading JSON File", 
#                                                       unit="B", unit_scale=True) as pbar:
#                 headers = {"X-Request-Type": "file_transfer", "Content-Length": str(file_size)}
                
#                 def read_in_chunks(file_object, chunk_size=1024*1024):
#                     """Generator to read file in chunks"""
#                     while True:
#                         data = file_object.read(chunk_size)
#                         if not data:
#                             break
#                         pbar.update(len(data))
#                         yield data

#                 response = requests.post(
#                     upload_url,
#                     data=read_in_chunks(f),
#                     headers=headers,
#                     timeout=300
#                 )

#             if response.status_code == 200:
#                 print("[Client] JSON file sent successfully.")
#                 return True
#             else:
#                 raise Exception(f"Server responded with status code: {response.status_code}")

#         except Exception as e:
#             retry_count += 1
#             print(f"[Client] Error sending JSON file (attempt {retry_count}): {e}")

#             if retry_count < max_retries:
#                 print(f"[Client] Retrying in 2 seconds...")
#                 time.sleep(2)
#             else:
#                 print("[Client] Failed to send file after maximum retries.")
#                 return False

# def main():
#     """Main entry point"""
#     try:
#         # Get server information
#         server_ip = input("Enter the server IP address: ").strip()
#         free_space = int(input("Enter free space in MB (e.g., 100, 200): "))

#         # Connect to server
#         connect_to_server(server_ip, free_space)

#         # Start local server for receiving chunks
#         server_address = ('', CLIENT_PORT)
#         server = HTTPServer(server_address, ClientRequestHandler)
#         server_thread = threading.Thread(target=server.serve_forever, daemon=True)
#         server_thread.start()
#         print(f"[Client] Starting local server on port {CLIENT_PORT}.")

#         # Create necessary directories
#         os.makedirs("received_files", exist_ok=True)

#         # Main command loop
#         while True:
#             print("\nAvailable commands:")
#             print("1. distribute - Send JSON file for distribution")
#             print("2. metrics    - Display current metrics")
#             print("3. exit       - Exit the client")
            
#             command = input("\nEnter command: ").strip().lower()

#             if command == "distribute":
#                 folder_path = input("Enter the folder path containing the JSON file: ").strip()
#                 if send_json_file_to_server(server_ip, folder_path):
#                     print("[Client] Waiting for server to process and distribute chunks...")
#             elif command == "metrics":
#                 update_client_metrics()
#                 print_client_metrics()
#             elif command == "exit":
#                 print("[Client] Exiting...")
#                 server.shutdown()
#                 break
#             else:
#                 print("Invalid command. Please try again.")

#     except KeyboardInterrupt:
#         print("\n[Client] Shutting down...")
#         if 'server' in locals():
#             server.shutdown()
#     except Exception as e:
#         print(f"[Client] Fatal error: {e}")
#         sys.exit(1)

# if __name__ == "__main__":
#     main()

##########################################################################################################################

import threading
import time
import os
import shutil
import json
import requests
import sys 
from http.server import BaseHTTPRequestHandler, HTTPServer
from tqdm import tqdm
from tabulate import tabulate

# Server ports configuration
MAIN_PORT = 8080        # Command & Control
HEARTBEAT_PORT = 8081   # Heartbeat traffic
UPLOAD_PORT = 8082      # File uploads
DOWNLOAD_PORT = 8083    # Chunk distribution
CLIENT_PORT = 8081      # Local client port for receiving chunks
CLIENT_NOTIFICATION_PORT = 8084
QUERY_PORT = 8085  # For querying records

# Global variables
first_heartbeat = True
server_config = {
    "server_ip": None,
    "device_id": None,
    "heartbeat_port": None,
    "upload_port": None,
    "download_port": None
}

client_metrics = {
    "total_space": 0,
    "used_space": 0,
    "free_space": 0,
    "chunks_stored": 0,
    "connection_status": "Disconnected",
    "last_updated": None,
    "current_transfer": None,
    "failed_chunks": []
}

def calculate_chunk_size(chunk_path):
    """Calculate the size of a chunk file in MB"""
    try:
        return os.path.getsize(chunk_path) / (1024 * 1024)
    except Exception as e:
        print(f"[Client] Error calculating chunk size for {chunk_path}: {e}")
        return 0

def update_client_metrics():
    """Update client metrics based on current state"""
    global client_metrics
    received_folder = "received_files"
    
    used_space = 0
    num_chunks = 0
    if os.path.exists(received_folder):
        for chunk_file in os.listdir(received_folder):
            if chunk_file.endswith('.json') and chunk_file != "client_local_index.json":
                chunk_path = os.path.join(received_folder, chunk_file)
                used_space += calculate_chunk_size(chunk_path)
                num_chunks += 1

    client_metrics["used_space"] = round(used_space, 2)
    client_metrics["free_space"] = round(client_metrics["total_space"] - used_space, 2)
    client_metrics["chunks_stored"] = num_chunks
    client_metrics["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")

def print_client_metrics():
    """Print formatted client metrics"""
    headers = ["Metric", "Value"]
    table_data = [
        ["Device ID", server_config["device_id"] or "Not assigned"],
        ["Total Space (MB)", client_metrics["total_space"]],
        ["Used Space (MB)", client_metrics["used_space"]],
        ["Free Space (MB)", client_metrics["free_space"]],
        ["Chunks Stored", client_metrics["chunks_stored"]],
        ["Connection Status", client_metrics["connection_status"]],
        ["Last Updated", client_metrics["last_updated"] or "Never"],
        ["Current Transfer", client_metrics["current_transfer"] or "None"],
        ["Failed Chunks", len(client_metrics["failed_chunks"])]
    ]
    
    print("\n=== Client Metrics ===")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    if client_metrics["failed_chunks"]:
        print("\nFailed Chunks:")
        for chunk in client_metrics["failed_chunks"]:
            print(f"  - {chunk}")
    print("===================\n")

def send_heartbeat():
    """Send periodic heartbeats to server"""
    global first_heartbeat, client_metrics
    heartbeat_url = f"http://{server_config['server_ip']}:{server_config['heartbeat_port']}"

    while True:
        try:
            headers = {"X-Request-Type": "heartbeat"}
            response = requests.post(heartbeat_url, headers=headers, timeout=10)
            
            if first_heartbeat and response.status_code == 200:
                print("[Client] First heartbeat sent to server.")
                first_heartbeat = False
                client_metrics["connection_status"] = "Connected"

            if response.status_code != 200:
                client_metrics["connection_status"] = "Connection Error"
            
        except Exception as e:
            print(f"[Client] Error sending heartbeat: {e}")
            client_metrics["connection_status"] = "Connection Error"

        time.sleep(30)

def connect_to_server(server_ip, free_space):
    """Connect to server and get configuration"""
    global client_metrics, server_config
    server_config["server_ip"] = server_ip
    client_metrics["total_space"] = free_space
    client_metrics["free_space"] = free_space
    
    server_url = f"http://{server_ip}:{MAIN_PORT}"
    data = {"free_space": free_space}

    retry_count = 0
    max_retries = 3
    while retry_count < max_retries:
        try:
            headers = {"X-Request-Type": "connection"}
            print(f"[Client] Connecting to server at {server_ip}...")
            response = requests.post(server_url, json=data, headers=headers, timeout=10)

            if response.status_code == 200:
                config_data = response.json()
                server_config.update({
                    "device_id": config_data["device_id"],
                    "heartbeat_port": config_data["heartbeat_port"],
                    "upload_port": config_data["upload_port"],
                    "download_port": config_data["download_port"]
                })
                
                print("[Client] Connected to server successfully.")
                print(f"[Client] Assigned Device ID: {server_config['device_id']}")
                client_metrics["connection_status"] = "Connected"
                
                # Start heartbeat in separate thread
                threading.Thread(target=send_heartbeat, daemon=True).start()
                break
            else:
                print(f"[Client] Failed to connect. Server responded with status code: {response.status_code}")
                client_metrics["connection_status"] = "Connection Failed"
                retry_count += 1

        except Exception as e:
            print(f"[Client] Connection attempt {retry_count + 1} failed: {e}")
            client_metrics["connection_status"] = "Connection Error"
            retry_count += 1
            if retry_count < max_retries:
                print(f"[Client] Retrying in 2 seconds...")
                time.sleep(2)

    print_client_metrics()

class ClientRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data) if content_length > 0 else {}

            if self.path == '/notify_chunks':
                self.handle_notify_chunks(data)
            elif self.path == '/notify_redistribution':
                self.handle_notify_redistribution(data)
            elif self.path == '/redistribution_failed':
                self.handle_redistribution_failed()
            elif self.path == '/clear_chunks':
                self.handle_clear_chunks()
            elif self.path == '/process_query':           # Make sure these
                self.handle_process_query(data)           # two lines are
            elif self.path == '/query_result':            # present and
                self.handle_query_result(data)            # properly indented
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Invalid endpoint")
                
        except Exception as e:
            print(f"[Client] Error handling request: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def handle_notify_chunks(self, data):
        """Handle notification of new chunk assignments"""
        chunks = data.get("chunks", [])
        if not chunks:
            print("[Client] Received empty chunk list")
            return

        print(f"[Client] Server assigned {len(chunks)} chunks")
        self.download_chunks(chunks)
        
        self.send_response(200)
        self.end_headers()

    def handle_notify_redistribution(self, data):
        """Handle redistribution notification"""
        chunks = data.get("chunks", [])
        print(f"[Client] Received redistribution assignment for {len(chunks)} chunks")
        self.download_chunks(chunks)
        
        self.send_response(200)
        self.end_headers()

    def handle_redistribution_failed(self):
        """Handle redistribution failure notification"""
        print("\n[Client] WARNING: Server redistribution failed due to insufficient space!")
        print("[Client] Some chunks may be temporarily unavailable.")
        client_metrics["connection_status"] = "Redistribution Failed"
        print_client_metrics()
        
        self.send_response(200)
        self.end_headers()

    def handle_clear_chunks(self):
        """Handle request to clear local chunks"""
        print("\n[Client] Clearing all chunks for redistribution...")
        received_folder = "received_files"
        if os.path.exists(received_folder):
            shutil.rmtree(received_folder)
            os.makedirs(received_folder)
        
        client_metrics["current_transfer"] = "Awaiting redistribution"
        update_client_metrics()
        print_client_metrics()
        
        self.send_response(200)
        self.end_headers()

    def download_chunks(self, chunks):
        """Download assigned chunks from server"""
        download_url = f"http://{server_config['server_ip']}:{server_config['download_port']}"
        
        for chunk in tqdm(chunks, desc="Downloading chunks", unit="chunk"):
            try:
                response = requests.get(f"{download_url}/download_chunk?name={chunk}", timeout=30)  # Increased timeout
                if response.status_code == 200:
                    self.save_chunk(chunk, response.json())
                else:
                    print(f"[Client] Failed to download chunk {chunk}")
                    client_metrics["failed_chunks"].append(chunk)
            except Exception as e:
                print(f"[Client] Error downloading chunk {chunk}: {e}")
                client_metrics["failed_chunks"].append(chunk)
                continue  # Continue with next chunk even if one fails

        update_client_metrics()
        print_client_metrics()

    def save_chunk(self, chunk_name, chunk_data):
        """Save received chunk to disk"""
        received_folder = "received_files"
        chunk_path = os.path.join(received_folder, chunk_name)
        
        # Atomic write using temporary file
        temp_path = chunk_path + '.tmp'
        try:
            with open(temp_path, "w") as f:
                json.dump(chunk_data, f, indent=4)
            os.replace(temp_path, chunk_path)
            
            self.update_local_index(chunk_name, chunk_data)
        
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e

    def update_local_index(self, chunk_name, chunk_data):
        """Update local index with new chunk information"""
        received_folder = "received_files"
        index_path = os.path.join(received_folder, "client_local_index.json")
        
        try:
            local_index = []
            if os.path.exists(index_path):
                with open(index_path, "r") as f:
                    local_index = json.load(f)

            local_index.append({
                "chunk": chunk_name,
                "start_id": chunk_data[0]["id"],
                "end_id": chunk_data[-1]["id"],
                "received_time": time.strftime("%Y-%m-%d %H:%M:%S")
            })

            temp_path = index_path + '.tmp'
            with open(temp_path, "w") as f:
                json.dump(local_index, f, indent=4)
            os.replace(temp_path, index_path)

        except Exception as e:
            print(f"[Client] Error updating local index: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    def handle_process_query(self, data):
        """Handle incoming query request from server"""
        query_id = data.get('query_id')
        chunk_name = data.get('chunk_name')
        requester_ip = data.get('requester_ip')
        
        try:
            # Read the chunk file
            chunk_path = os.path.join("received_files", chunk_name)
            if not os.path.exists(chunk_path):
                print(f"[Client] Chunk {chunk_name} not found locally")
                self.send_response(404)
                self.end_headers()
                return
                
            with open(chunk_path, 'r') as f:
                chunk_data = json.load(f)
                
            # Find the specific record
            record = next((item for item in chunk_data if item["id"] == query_id), None)
            
            if record:
                print(f"[Client] Found record for ID {query_id}, sending to requester")
                # Send result back to requesting client
                try:
                    response = requests.post(
                        f"http://{requester_ip}:{CLIENT_NOTIFICATION_PORT}/query_result",
                        json={"record": record},
                        timeout=10
                    )
                    if response.status_code == 200:
                        print(f"[Client] Successfully sent query result to {requester_ip}")
                    
                except Exception as e:
                    print(f"[Client] Error sending query result: {e}")
                    
            self.send_response(200)
            self.end_headers()
            
        except Exception as e:
            print(f"[Client] Error processing query: {e}")
            self.send_response(500)
            self.end_headers()

    def handle_query_result(self, data):
        """Handle incoming query results"""
        record = data.get('record')
        if record:
            print("\n=== Query Result ===")
            print(f"ID: {record['id']}")
            print(f"Name: {record['name']}")
            print(f"Email: {record['email']}")
            print(f"Address: {record['address']}")
            print(f"Phone: {record['phone']}")
            print(f"Registration Date: {record['registration_date']}")
            print(f"Last Login: {record['last_login']}")
            print("\nPreferences:")
            print(f"  Language: {record['preferences']['language']}")
            print(f"  Currency: {record['preferences']['currency']}")
            print("\nActivity Log:")
            for activity in record['activity_log']:
                print(f"  - {activity['action']} at {activity['timestamp']}")
                print(f"    {activity['metadata']}")
            print("==================\n")
        
        self.send_response(200)
        self.end_headers()


    def log_message(self, format, *args):
        return

def send_json_file_to_server(server_ip, folder_path):
    """Send JSON file to server for distribution"""
    upload_url = f"http://{server_config['server_ip']}:{server_config['upload_port']}"
    retry_count = 0
    max_retries = 3

    while retry_count < max_retries:
        try:
            files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
            if not files:
                print("[Client] No JSON file found in the specified folder.")
                return False

            json_file_path = os.path.join(folder_path, files[0])
            file_size = os.path.getsize(json_file_path)

            print("[Client] Preparing to send JSON file...")
            print(f"File size: {file_size / (1024*1024):.2f} MB")

            with open(json_file_path, 'rb') as f, tqdm(total=file_size, desc="Uploading JSON File", 
                                                      unit="B", unit_scale=True) as pbar:
                headers = {"X-Request-Type": "file_transfer", "Content-Length": str(file_size)}
                
                def read_in_chunks(file_object, chunk_size=1024*1024):
                    """Generator to read file in chunks"""
                    while True:
                        data = file_object.read(chunk_size)
                        if not data:
                            break
                        pbar.update(len(data))
                        yield data

                response = requests.post(
                    upload_url,
                    data=read_in_chunks(f),
                    headers=headers,
                    timeout=300
                )

            if response.status_code == 200:
                print("[Client] JSON file sent successfully.")
                return True
            else:
                raise Exception(f"Server responded with status code: {response.status_code}")

        except Exception as e:
            retry_count += 1
            print(f"[Client] Error sending JSON file (attempt {retry_count}): {e}")

            if retry_count < max_retries:
                print(f"[Client] Retrying in 2 seconds...")
                time.sleep(2)
            else:
                print("[Client] Failed to send file after maximum retries.")
                return False

def query_record(server_ip, query_id):
    """Send query request to server"""
    try:
        print(f"[Client] Sending query for ID {query_id} to server...")
        response = requests.post(
            f"http://{server_ip}:8085/query",
            json={"query_id": query_id},
            timeout=30
        )
        
        if response.status_code == 404:
            print(f"[Client] Record with ID {query_id} not found.")
        elif response.status_code != 200:
            print(f"[Client] Query failed with status code: {response.status_code}")
            
    except Exception as e:
        print(f"[Client] Error sending query: {e}")


def clean_start():
    """Clean start function to reset everything"""
    # Clear directories
    if os.path.exists("received_files"):
        shutil.rmtree("received_files")
    os.makedirs("received_files", exist_ok=True)
    
    # Reset client metrics
    client_metrics.update({
        "total_space": 0,
        "used_space": 0,
        "free_space": 0,
        "chunks_stored": 0,
        "connection_status": "Disconnected",
        "last_updated": None,
        "current_transfer": None,
        "failed_chunks": []
    })
    
    # Reset server config
    server_config.update({
        "server_ip": None,
        "device_id": None,
        "heartbeat_port": None,
        "upload_port": None,
        "download_port": None
    })


def main():
    """Main entry point"""
    try:
        # Clean start
        clean_start()

        # Get server information
        server_ip = input("Enter the server IP address: ").strip()
        free_space = int(input("Enter free space in MB (e.g., 100, 200): "))

        # Connect to server
        connect_to_server(server_ip, free_space)

        # Start heartbeat server
        heartbeat_server = HTTPServer(('', CLIENT_PORT), ClientRequestHandler)
        heartbeat_thread = threading.Thread(target=heartbeat_server.serve_forever, daemon=True)
        heartbeat_thread.start()
        print(f"[Client] Starting heartbeat server on port {CLIENT_PORT}.")

        # Start notification server
        notification_server = HTTPServer(('', CLIENT_NOTIFICATION_PORT), ClientRequestHandler)
        notification_thread = threading.Thread(target=notification_server.serve_forever, daemon=True)
        notification_thread.start()
        print(f"[Client] Starting notification server on port {CLIENT_NOTIFICATION_PORT}.")

        print("\nAvailable commands:")
        print("1. distribute - Send JSON file for distribution")
        print("2. metrics    - Display current metrics")
        print("3. query      - Query a specific record")
        print("4. exit       - Exit the client")

        # Main command loop
        while True:
            
            command = input("\nEnter command: ").strip().lower()

            if command == "distribute":
                folder_path = input("Enter the folder path containing the JSON file: ").strip()
                if send_json_file_to_server(server_ip, folder_path):
                    print("[Client] Waiting for server to process and distribute chunks...")
            
            elif command == "metrics":
                update_client_metrics()
                print_client_metrics()
            
            elif command == "query":
                try:
                    query_id = int(input("Enter the ID to query: "))
                    query_record(server_config['server_ip'], query_id)
                except ValueError:
                    print("[Client] Please enter a valid numeric ID")
            
            elif command == "exit":
                print("[Client] Exiting...")
                heartbeat_server.shutdown()
                notification_server.shutdown()
                break
            
            else:
                print("Invalid command. Please try again.")

    except KeyboardInterrupt:
        print("\n[Client] Shutting down...")
        if 'heartbeat_server' in locals():
            heartbeat_server.shutdown()
        if 'notification_server' in locals():
            notification_server.shutdown()
    except Exception as e:
        print(f"[Client] Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
