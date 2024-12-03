import threading
import time
import os
import shutil
import json
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer
from tqdm import tqdm
import sys
from tabulate import tabulate

# Global variables and synchronization
connected_devices = {}
current_id = 1
heartbeat_received = {}
heartbeat_interval = 30
heartbeat_timeout = 60
redistribution_in_progress = False
distribution_completed = False
redistribution_lock = threading.Lock()

# Server ports configuration
MAIN_PORT = 8080        # Command & Control
HEARTBEAT_PORT = 8081   # Heartbeat traffic
UPLOAD_PORT = 8082      # File uploads
DOWNLOAD_PORT = 8083    # Chunk distribution
CLIENT_NOTIFICATION_PORT = 8084  # New port for notifications
QUERY_PORT = 8085  # For querying records

def calculate_chunk_size(chunk_path):
    """Calculate the size of a chunk file in MB"""
    return os.path.getsize(chunk_path) / (1024 * 1024)

def print_server_metrics():
    """Print formatted metrics table for all connected devices"""
    headers = ["Device ID", "IP Address", "Total Space (MB)", "Used Space (MB)", 
              "Free Space (MB)", "Chunks", "Status", "Last Heartbeat"]
    table_data = []
    
    for ip, device in connected_devices.items():
        used_space = 0
        num_chunks = 0
        if os.path.exists("master_index.json"):
            try:
                with open("master_index.json", "r") as f:
                    master_data = json.load(f)
                    device_chunks = [entry for entry in master_data["chunks"] 
                                   if entry["assigned_to"] == ip]
                    num_chunks = len(device_chunks)
                    for chunk in device_chunks:
                        chunk_path = os.path.join("aws_chunks", chunk["chunk"])
                        if os.path.exists(chunk_path):
                            used_space += calculate_chunk_size(chunk_path)
            except Exception as e:
                print(f"[Server] Error reading master index: {e}")

        last_heartbeat = "N/A"
        if ip in heartbeat_received:
            seconds_ago = int(time.time() - heartbeat_received[ip])
            last_heartbeat = f"{seconds_ago}s ago"

        status = "Connected" if ip in heartbeat_received and \
                (time.time() - heartbeat_received[ip]) <= heartbeat_timeout else "Disconnected"
        
        remaining_space = device["free_space"] - used_space

        table_data.append([
            device["id"],
            ip,
            device["free_space"],
            round(used_space, 2),
            round(remaining_space, 2),
            num_chunks,
            status,
            last_heartbeat
        ])

    print("\n=== Server Metrics ===")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    print(f"Total Connected Devices: {len(connected_devices)}")
    print(f"Distribution Status: {'Completed' if distribution_completed else 'Not Started'}")
    print(f"Redistribution Status: {'In Progress' if redistribution_in_progress else 'Not Active'}")
    print("=====================\n")

# Base handler with common functionality
class BaseRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return  # Suppress default logging

# Command & Control handler (Port 8080)
class MainHandler(BaseRequestHandler):
    def do_POST(self):
        global current_id, distribution_completed
        client_ip = self.client_address[0]
        type_header = self.headers.get("X-Request-Type", "unknown")

        try:
            if type_header == "connection":
                self.handle_connection(client_ip)
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid request type")
        except Exception as e:
            print(f"[Server] Error handling main request from {client_ip}: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def handle_connection(self, client_ip):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        received_data = json.loads(post_data)
        free_space = received_data.get("free_space", 0)

        if client_ip not in connected_devices:
            global current_id
            connected_devices[client_ip] = {
                "id": current_id,
                "free_space": free_space,
                "heartbeat_logged": False
            }
            current_id += 1

            if distribution_completed:
                print(f"\n[Server] New device connected after distribution. Initiating redistribution...")
                threading.Thread(target=redistribute_all_chunks, daemon=True).start()
        else:
            connected_devices[client_ip]["free_space"] = free_space

        print(f"\n[Server] New device connected:")
        print(f"  - IP: {client_ip}")
        print(f"  - Free Space: {free_space} MB")
        print(f"  - Assigned ID: {connected_devices[client_ip]['id']}\n")

        print_server_metrics()
        heartbeat_received[client_ip] = time.time()

        self.send_response(200)
        self.end_headers()
        response_data = {
            "status": "connected",
            "device_id": connected_devices[client_ip]["id"],
            "heartbeat_port": HEARTBEAT_PORT,
            "upload_port": UPLOAD_PORT,
            "download_port": DOWNLOAD_PORT
        }
        self.wfile.write(json.dumps(response_data).encode())

# Heartbeat handler (Port 8081)
class HeartbeatHandler(BaseRequestHandler):
    def do_POST(self):
        client_ip = self.client_address[0]
        
        if client_ip in connected_devices:
            heartbeat_received[client_ip] = time.time()
            if not connected_devices[client_ip].get("heartbeat_logged"):
                print(f"[Server] First heartbeat received from device {client_ip} (ID: {connected_devices[client_ip]['id']}).")
                connected_devices[client_ip]["heartbeat_logged"] = True
                print_server_metrics()

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Heartbeat acknowledged")

# File Upload handler (Port 8082)
class UploadHandler(BaseRequestHandler):
    def do_POST(self):
        global distribution_completed
        client_ip = self.client_address[0]

        if client_ip not in connected_devices:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Client not connected")
            return

        try:
            content_length = int(self.headers.get('Content-Length', 0))
            backup_folder = "aws_backup"
            if os.path.exists(backup_folder):
                shutil.rmtree(backup_folder)
            os.makedirs(backup_folder)
            backup_file_path = os.path.join(backup_folder, "backup.json")

            # Handle streaming file upload
            with open(backup_file_path, 'wb') as f, tqdm(total=content_length, 
                    desc="Receiving JSON File", unit="B", unit_scale=True, file=sys.stdout) as pbar:
                received_bytes = 0
                while received_bytes < content_length:
                    chunk = self.rfile.read(min(1024*1024, content_length - received_bytes))
                    if not chunk:
                        break
                    f.write(chunk)
                    received_bytes += len(chunk)
                    pbar.update(len(chunk))

            print(f"\n[Server] JSON file received and backed up at {backup_file_path}.")
            
            distribution_completed = False  # Reset flag before new distribution
            chunk_json_file(backup_file_path)
            create_master_index_and_distribute()
            distribution_completed = True  # Set flag after successful distribution

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"File processed and distributed successfully")

        except Exception as e:
            print(f"[Server] Error in file upload: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

# Chunk Distribution handler (Port 8083)
class DistributionHandler(BaseRequestHandler):
    def do_GET(self):
        client_ip = self.client_address[0]
        
        try:
            if self.path == '/get_chunks':
                self.handle_get_chunks(client_ip)
            elif self.path.startswith('/download_chunk'):
                self.handle_download_chunk(client_ip)
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            print(f"[Server] Error in distribution handler: {e}")
            self.send_response(500)
            self.end_headers()

    def handle_get_chunks(self, client_ip):
        try:
            if not os.path.exists("master_index.json"):
                raise Exception("Master index not found")

            with open("master_index.json", "r") as f:
                master_data = json.load(f)

            assigned_chunks = [chunk["chunk"] for chunk in master_data 
                             if chunk["assigned_to"] == client_ip]

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(assigned_chunks).encode())

        except Exception as e:
            print(f"[Server] Error getting chunks list: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def handle_download_chunk(self, client_ip):
        try:
            chunk_name = self.path.split('=')[1]
            chunk_path = os.path.join("aws_chunks", chunk_name)

            if not os.path.exists(chunk_path):
                raise Exception(f"Chunk {chunk_name} not found")

            # Verify chunk assignment - Fixed to handle new master_index structure
            with open("master_index.json", "r") as f:
                master_data = json.load(f)
                chunk_info = next((chunk for chunk in master_data["chunks"]  # Access the chunks array
                                if chunk["chunk"] == chunk_name), None)
                
                if not chunk_info or chunk_info["assigned_to"] != client_ip:
                    raise Exception("Chunk not assigned to this client")

            # Send chunk data
            with open(chunk_path, 'rb') as f:
                chunk_data = f.read()

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(chunk_data)))
            self.end_headers()
            self.wfile.write(chunk_data)

        except Exception as e:
            print(f"[Server] Error sending chunk: {e}")
            self.send_response(404)
            self.end_headers()
            self.wfile.write(str(e).encode())

def chunk_json_file(file_path):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        total_entries = len(data)
        # Either use max entries per chunk
        MAX_ENTRIES_PER_CHUNK = 1000  # or whatever size you prefer
        entries_per_chunk = min(MAX_ENTRIES_PER_CHUNK, total_entries)

        # Or use desired number of chunks
        # DESIRED_CHUNKS = 10
        # entries_per_chunk = max(1, total_entries // DESIRED_CHUNKS)

        chunks_folder = "aws_chunks"
        if os.path.exists(chunks_folder):
            shutil.rmtree(chunks_folder)
        os.makedirs(chunks_folder)

        chunk_files = []
        for i in range(0, total_entries, entries_per_chunk):
            chunk_data = data[i:i + entries_per_chunk]
            chunk_file_name = f"chunk_{i // entries_per_chunk + 1}.json"
            chunk_file_path = os.path.join(chunks_folder, chunk_file_name)
            with open(chunk_file_path, "w") as chunk_file:
                json.dump(chunk_data, chunk_file, indent=4)
            chunk_files.append(chunk_file_name)

        print(f"[Server] JSON file chunked into {len(chunk_files)} files in {chunks_folder}/.")
        return chunk_files

    except Exception as e:
        print(f"[Server] Error in chunking file: {e}")
        raise

    except Exception as e:
        print(f"[Server] Error in chunking file: {e}")
        raise

def create_master_index_and_distribute():
    """Create and distribute chunks to connected devices"""
    try:
        chunks_folder = "aws_chunks"
        chunk_files = sorted(os.listdir(chunks_folder))
        total_chunks = len(chunk_files)
        total_space = sum(device["free_space"] for device in connected_devices.values())

        if total_space == 0:
            raise Exception("No available free space on connected devices")

        # Always start with empty master index for clean distribution
        master_index = []
        device_assignments = {}

        # Calculate proportional distribution
        for ip, device in connected_devices.items():
            proportion = device["free_space"] / total_space
            assigned_chunks = int(proportion * total_chunks)
            device_assignments[ip] = {"chunks": [], "count": assigned_chunks}

        # Handle remaining chunks
        remaining = total_chunks - sum(d["count"] for d in device_assignments.values())
        sorted_devices = sorted(connected_devices.items(), 
                              key=lambda x: x[1]["free_space"], reverse=True)
        for i in range(remaining):
            device_ip = sorted_devices[i % len(sorted_devices)][0]
            device_assignments[device_ip]["count"] += 1

        # Assign chunks to devices
        current_chunk = 0
        for ip, assignment in device_assignments.items():
            device_chunks = chunk_files[current_chunk:current_chunk + assignment["count"]]
            assignment["chunks"] = device_chunks
            current_chunk += assignment["count"]

            for chunk in device_chunks:
                chunk_path = os.path.join(chunks_folder, chunk)
                with open(chunk_path, "r") as f:
                    data = json.load(f)
                master_index.append({
                    "chunk": chunk,
                    "start_id": data[0]["id"],
                    "end_id": data[-1]["id"],
                    "assigned_to": ip
                })

        # Save new master index
        with open("master_index.json", "w") as f:
            json.dump({
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
                "chunks": master_index
            }, f, indent=4)

        print(f"[Server] Master index created with {len(master_index)} entries")
        notify_clients_distribution(device_assignments)

    except Exception as e:
        print(f"[Server] Error in distribution: {e}")
        raise

def redistribute_chunks_from_device(disconnected_ip):
    """Handle redistribution when a device disconnects"""
    print(f"[Server] Attempting to redistribute chunks from disconnected device {disconnected_ip}...")
    global redistribution_in_progress
    
    with redistribution_lock:
        if redistribution_in_progress:
            print("[Server] Redistribution already in progress. Waiting...")
            return

        redistribution_in_progress = True
        try:
            if not os.path.exists("master_index.json"):
                print("[Server] No master index found. Nothing to redistribute.")
                return

            with open("master_index.json", "r") as f:
                master_data = json.load(f)
                master_index = master_data["chunks"]  

            orphaned_chunks = [entry for entry in master_index 
                             if entry["assigned_to"] == disconnected_ip]
            
            if not orphaned_chunks:
                print(f"[Server] No chunks found for disconnected device {disconnected_ip}")
                return

            total_space_needed = sum(
                calculate_chunk_size(os.path.join("aws_chunks", chunk["chunk"]))
                for chunk in orphaned_chunks
            )

            available_devices = {
                ip: info for ip, info in connected_devices.items()
                if ip != disconnected_ip and
                ip in heartbeat_received and
                (time.time() - heartbeat_received[ip]) <= heartbeat_timeout
            }

            if not available_devices:
                print("[Server] No available devices for redistribution.")
                return

            total_available_space = sum(d["free_space"] for d in available_devices.values())

            if total_available_space < total_space_needed:
                print(f"[Server] Redistribution not feasible - Need {total_space_needed:.2f}MB but only {total_available_space:.2f}MB available")
                notify_clients_redistribution_failed()
                return

            print(f"[Server] Redistributing {len(orphaned_chunks)} chunks among {len(available_devices)} active devices: {', '.join(available_devices.keys())}")

            # Calculate new distribution
            new_assignments = {}
            for ip in available_devices:
                new_assignments[ip] = []

            # Distribute chunks evenly among available devices
            for i, chunk in enumerate(orphaned_chunks):
                target_ip = list(available_devices.keys())[i % len(available_devices)]
                new_assignments[target_ip].append(chunk["chunk"])
                chunk["assigned_to"] = target_ip

            # Update master index with new assignments
            with open("master_index.json", "w") as f:
                json.dump({
                    "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "chunks": master_index
                }, f, indent=4)

            # Notify clients and distribute chunks
            notify_clients_redistribution(new_assignments)
            
            print("[Server] Redistribution completed successfully")
            print_server_metrics()

        except Exception as e:
            print(f"[Server] Error during redistribution: {e}")
        finally:
            redistribution_in_progress = False

def redistribute_all_chunks():
    """Handle complete redistribution when a new device joins"""
    print(f"[Server] Starting full redistribution due to new device connection...")
    global redistribution_in_progress
    
    with redistribution_lock:
        if redistribution_in_progress:
            print("[Server] Redistribution already in progress. Waiting...")
            return

        redistribution_in_progress = True
        try:
            if not os.path.exists("master_index.json"):
                print("[Server] No master index found. Nothing to redistribute.")
                return

            print(f"[Server] Starting redistribution among {len(connected_devices)} connected devices...")  # Modified
            notify_clients_clear_chunks()
            create_master_index_and_distribute()
            print("[Server] Complete redistribution finished successfully")
            print_server_metrics()

        except Exception as e:
            print(f"[Server] Error during redistribution: {e}")
        finally:
            redistribution_in_progress = False

def notify_clients_distribution(assignments):
    """Notify clients about new chunk assignments"""
    for ip, assignment in assignments.items():
        try:
            response = requests.post(
                f"http://{ip}:{CLIENT_NOTIFICATION_PORT}/notify_chunks",
                json={"chunks": assignment["chunks"]},
                timeout=30
            )
        except Exception as e:
            pass

def notify_clients_redistribution(assignments):
    """Notify clients about redistribution"""
    for ip, chunks in assignments.items():
        try:
            response = requests.post(
                f"http://{ip}:{CLIENT_NOTIFICATION_PORT}/notify_redistribution",
                json={"chunks": chunks},
                timeout=30
            )
        except Exception as e:
            pass

def notify_clients_redistribution_failed():
    """Notify all clients that redistribution failed"""
    for ip in connected_devices:
        try:
            requests.post(
                f"http://{ip}:{CLIENT_NOTIFICATION_PORT}/redistribution_failed",
                timeout=5
            )
        except Exception as e:
            pass

def notify_clients_clear_chunks():
    """Notify clients to clear their chunks"""
    for ip in connected_devices:
        try:
            requests.post(
                f"http://{ip}:{CLIENT_NOTIFICATION_PORT}/clear_chunks",
                timeout=5
            )
        except Exception as e:
            pass

def heartbeat_monitor():
    """Monitor client heartbeats"""
    while True:
        current_time = time.time()
        disconnected_devices = []
        
        for ip, last_heartbeat in heartbeat_received.items():
            if current_time - last_heartbeat > heartbeat_timeout:
                if distribution_completed:
                    print(f"\n[Server] Device {ip} has disconnected - Starting redistribution of its chunks...")
                else:
                    print(f"\n[Server] Device {ip} has disconnected - No redistribution needed as initial distribution not complete.")
                disconnected_devices.append(ip)

        for ip in disconnected_devices:
            if distribution_completed:
                threading.Thread(target=redistribute_chunks_from_device, 
                               args=(ip,), daemon=True).start()
            
            connected_devices.pop(ip, None)
            heartbeat_received.pop(ip, None)
            
        time.sleep(heartbeat_interval)

def start_server(handler_class, port):
    """Start a server instance on specified port"""
    server = HTTPServer(('', port), handler_class)
    server_thread = threading.Thread(
        target=server.serve_forever,
        name=f"Server-{port}",
        daemon=True
    )
    server_thread.start()
    return server

class QueryHandler(BaseRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            query_id = data.get('query_id')
            requester_ip = self.client_address[0]
            
            if not query_id:
                self.send_response(400)
                self.end_headers()
                return

            print(f"\n[Server] Received query request for ID {query_id} from {requester_ip}")
            
            try:
                # Look up in master index
                with open("master_index.json", "r") as f:
                    master_data = json.load(f)
                    target_chunk = next(
                        (chunk for chunk in master_data["chunks"] 
                         if chunk["start_id"] <= query_id <= chunk["end_id"]), 
                        None
                    )
                    
                    if not target_chunk:
                        print(f"[Server] ID {query_id} not found in any chunk")
                        self.send_response(404)
                        self.end_headers()
                        return
                        
                    holder_ip = target_chunk["assigned_to"]
                    chunk_name = target_chunk["chunk"]
                    print(f"[Server] Found ID {query_id} in {chunk_name} on device {holder_ip}")
                    
                    try:
                        response = requests.post(
                            f"http://{holder_ip}:{CLIENT_NOTIFICATION_PORT}/process_query",
                            json={
                                "query_id": query_id, 
                                "chunk_name": chunk_name, 
                                "requester_ip": requester_ip
                            },
                            timeout=10
                        )
                        
                        self.send_response(200 if response.status_code == 200 else 500)
                        self.end_headers()
                    except requests.exceptions.ReadTimeout:
                        # Skip printing timeout errors for self-queries
                        self.send_response(200)
                        self.end_headers()
                    except Exception as e:
                        # Only print error if not a self-query
                        if holder_ip != requester_ip:
                            print(f"[Server] Error communicating with client: {e}")
                        self.send_response(500)
                        self.end_headers()
                    
            except Exception as e:
                print(f"[Server] Error processing query: {e}")
                self.send_response(500)
                self.end_headers()
                
        except Exception as e:
            print(f"[Server] Error in query handler: {e}")
            self.send_response(500)
            self.end_headers()


def run_server():
    """Initialize and run all server components"""
    try:
        # Clear and recreate necessary directories
        if os.path.exists("aws_chunks"):
            shutil.rmtree("aws_chunks")
        if os.path.exists("aws_backup"):
            shutil.rmtree("aws_backup")
        if os.path.exists("master_index.json"):
            os.remove("master_index.json")

        # Create necessary directories
        os.makedirs("aws_chunks", exist_ok=True)
        os.makedirs("aws_backup", exist_ok=True)

        # Start all server components
        servers = {
            'main': start_server(MainHandler, MAIN_PORT),
            'heartbeat': start_server(HeartbeatHandler, HEARTBEAT_PORT),
            'upload': start_server(UploadHandler, UPLOAD_PORT),
            'distribution': start_server(DistributionHandler, DOWNLOAD_PORT),
            'query': start_server(QueryHandler, QUERY_PORT)  # Add this line
        }

        print("[Server] Starting AWS distributed file system...")
        print(f"[Server] Main server running on port {MAIN_PORT}")
        print(f"[Server] Heartbeat server running on port {HEARTBEAT_PORT}")
        print(f"[Server] Upload server running on port {UPLOAD_PORT}")
        print(f"[Server] Distribution server running on port {DOWNLOAD_PORT}")
        print(f"[Server] Query server running on port {QUERY_PORT}")

        # Start heartbeat monitoring
        heartbeat_thread = threading.Thread(
            target=heartbeat_monitor,
            name="HeartbeatMonitor",
            daemon=True
        )
        heartbeat_thread.start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Server] Shutting down servers...")
            for server in servers.values():
                server.shutdown()
                server.server_close()
            print("[Server] Shutdown complete.")

    except Exception as e:
        print(f"[Server] Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_server()
