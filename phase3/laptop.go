package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

// Node structure matching the AWS coordinator
type Node struct {
	ID       string    `json:"id"`
	Capacity int       `json:"capacity"` // in MB
	Used     int       `json:"used"`     // in MB
	MacID    string    `json:"mac_id"`
	IP       string    `json:"ip"`
	Port     int       `json:"port"`
	Status   string    `json:"status"`
	LastSeen time.Time `json:"last_seen"`
}

// LocalIndex maintains information about stored chunks
type LocalIndex struct {
	Chunks      map[string]ChunkInfo `json:"chunks"`
	TotalSize   int                  `json:"total_size"` // in KB
	LastUpdated time.Time            `json:"last_updated"`
}

// ChunkInfo stores information about each data chunk
type ChunkInfo struct {
	ChunkID string `json:"chunk_id"`
	IDRange struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"id_range"`
	Size     int    `json:"size"` // in KB
	FilePath string `json:"file_path"`
}

var (
	localFolder = "received_files"
	awsURL      = "http://18.144.165.108:8080" // Change this to your AWS IP
	node        Node
	localIndex  = LocalIndex{
		Chunks: make(map[string]ChunkInfo),
	}
	mu sync.RWMutex
)

func main() {
	// Create necessary directories
	os.MkdirAll(localFolder, 0755)

	// Initialize node with port selection
	if !initializeNode() {
		fmt.Println("[ERROR] No available ports found. Exiting...")
		return
	}

	// Load existing index if any
	loadLocalIndex()

	// Register with AWS
	registerNode()

	// Start background tasks
	go sendHeartbeats()
	go updateLocalMetrics()

	// Setup HTTP server for receiving files
	http.HandleFunc("/receive", receiveFiles)
	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/metrics", getLocalMetrics)

	// Start command interface
	go commandInterface()

	fmt.Printf("[INFO] Laptop node %s started. Listening on port %d...\n", node.ID, node.Port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil)
	if err != nil {
		fmt.Printf("[ERROR] Failed to start server: %v\n", err)
		os.Exit(1)
	}
}

func findAvailablePort() int {
	// Try ports from 8081, then 5000-5010
	ports := append([]int{8081}, generateRange(5000, 5010)...)

	for _, port := range ports {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err == nil {
			listener.Close()
			return port
		}
	}
	return -1
}

func generateRange(start, end int) []int {
	var ports []int
	for i := start; i <= end; i++ {
		ports = append(ports, i)
	}
	return ports
}

func initializeNode() bool {
	// Generate unique ID
	nodeID := fmt.Sprintf("laptop-%d", time.Now().Unix()%1000)

	// Get IP
	ip := getPublicIP()

	// Find available port
	port := findAvailablePort()
	if port == -1 {
		return false
	}

	// Get capacity from environment variable or use default
	capacity := 200 // Default 200MB
	if capEnv := os.Getenv("NODE_CAPACITY"); capEnv != "" {
		fmt.Sscanf(capEnv, "%d", &capacity)
	}

	node = Node{
		ID:       nodeID,
		Capacity: capacity,
		Used:     0,
		MacID:    getMacAddress(),
		IP:       ip,
		Port:     port,
		Status:   "active",
	}

	fmt.Printf("[INFO] Initialized node: %s (MAC: %s, IP: %s, Port: %d, Capacity: %dMB)\n",
		node.ID, node.MacID, node.IP, node.Port, node.Capacity)
	return true
}

func getMacAddress() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return fmt.Sprintf("unknown-%d", time.Now().Unix())
	}

	for _, i := range interfaces {
		if i.Flags&net.FlagUp != 0 && !strings.HasPrefix(i.Name, "lo") {
			return i.HardwareAddr.String()
		}
	}

	return fmt.Sprintf("unknown-%d", time.Now().Unix())
}

func getPublicIP() string {
	resp, err := http.Get("https://api.ipify.org?format=text")
	if err != nil {
		fmt.Println("[ERROR] Unable to fetch public IP:", err)
		return "localhost"
	}
	defer resp.Body.Close()

	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "localhost"
	}
	return string(ip)
}

func loadLocalIndex() {
	data, err := ioutil.ReadFile(filepath.Join(localFolder, "index.json"))
	if err != nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()
	json.Unmarshal(data, &localIndex)
}

func saveLocalIndex() {
	mu.RLock()
	defer mu.RUnlock()

	data, err := json.MarshalIndent(localIndex, "", "  ")
	if err != nil {
		fmt.Println("[ERROR] Failed to save local index:", err)
		return
	}

	err = ioutil.WriteFile(filepath.Join(localFolder, "index.json"), data, 0644)
	if err != nil {
		fmt.Println("[ERROR] Failed to write local index:", err)
	}
}

func updateLocalMetrics() {
	for {
		mu.Lock()
		// Calculate total size of stored chunks
		var totalSize int
		for _, chunk := range localIndex.Chunks {
			totalSize += chunk.Size
		}
		localIndex.TotalSize = totalSize
		localIndex.LastUpdated = time.Now()

		// Update node metrics
		node.Used = totalSize / 1024 // Convert KB to MB
		mu.Unlock()

		saveLocalIndex()
		time.Sleep(5 * time.Second)
	}
}

func registerNode() {
	data, _ := json.Marshal(node)
	resp, err := http.Post(awsURL+"/register", "application/json", bytes.NewBuffer(data))
	if err != nil {
		fmt.Println("[ERROR] Failed to register with AWS:", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("[INFO] Successfully registered with AWS (Port: %d)\n", node.Port)
	} else {
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Printf("[ERROR] Registration failed with status: %d - %s\n", resp.StatusCode, string(body))
	}
}

func sendHeartbeats() {
	for {
		data, _ := json.Marshal(node)
		_, err := http.Post(awsURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
		if err != nil {
			fmt.Println("[ERROR] Failed to send heartbeat:", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func receiveFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	fileName := r.Header.Get("File-Name")
	if fileName == "" {
		http.Error(w, "File name header missing", http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read file data", http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(localFolder, fileName)
	err = ioutil.WriteFile(filePath, body, 0644)
	if err != nil {
		http.Error(w, "Failed to save file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Update local index
	mu.Lock()
	localIndex.Chunks[fileName] = ChunkInfo{
		ChunkID:  fileName,
		Size:     len(body) / 1024, // Convert to KB
		FilePath: filePath,
	}
	mu.Unlock()

	saveLocalIndex()
	fmt.Printf("[INFO] Received file: %s on port %d\n", fileName, node.Port)
	w.WriteHeader(http.StatusOK)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	chunkID := r.URL.Query().Get("chunk")
	queryID := r.URL.Query().Get("id")

	mu.RLock()
	chunk, exists := localIndex.Chunks[chunkID]
	mu.RUnlock()

	if !exists {
		http.Error(w, "Chunk not found", http.StatusNotFound)
		return
	}

	// Read the chunk file
	data, err := ioutil.ReadFile(chunk.FilePath)
	if err != nil {
		http.Error(w, "Failed to read chunk file", http.StatusInternalServerError)
		return
	}

	// Parse JSON and find specific record
	var records []map[string]interface{}
	err = json.Unmarshal(data, &records)
	if err != nil {
		http.Error(w, "Failed to parse chunk data", http.StatusInternalServerError)
		return
	}

	targetID := parseInt(queryID)
	for _, record := range records {
		if int(record["id"].(float64)) == targetID {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(record)
			return
		}
	}

	http.Error(w, "Record not found in chunk", http.StatusNotFound)
}

func getLocalMetrics(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	metrics := struct {
		NodeID      string    `json:"node_id"`
		Capacity    int       `json:"capacity_mb"`
		Used        int       `json:"used_mb"`
		ChunkCount  int       `json:"chunk_count"`
		LastUpdated time.Time `json:"last_updated"`
	}{
		NodeID:      node.ID,
		Capacity:    node.Capacity,
		Used:        node.Used,
		ChunkCount:  len(localIndex.Chunks),
		LastUpdated: localIndex.LastUpdated,
	}
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func commandInterface() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("\nAvailable commands (Port %d):\n", node.Port)
	fmt.Println("1. distribute - Upload and distribute dataset")
	fmt.Println("2. status    - Show node status")
	fmt.Println("3. list      - List received files")
	fmt.Println("4. exit      - Exit the program")

	for {
		fmt.Print("\nEnter command: ")
		command, _ := reader.ReadString('\n')
		command = strings.TrimSpace(command)

		switch command {
		case "distribute":
			handleDistribute()
		case "status":
			showStatus()
		case "list":
			listFiles()
		case "exit":
			fmt.Println("[INFO] Shutting down...")
			os.Exit(0)
		default:
			fmt.Println("[ERROR] Unknown command")
		}
	}
}

func handleDistribute() {
	fmt.Print("Enter the path to the dataset folder: ")
	reader := bufio.NewReader(os.Stdin)
	path, _ := reader.ReadString('\n')
	path = strings.TrimSpace(path)

	files, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Printf("[ERROR] Failed to read directory: %s\n", err)
		return
	}

	fmt.Printf("[INFO] Uploading %d files to AWS from port %d...\n", len(files), node.Port)
	bar := progressbar.Default(int64(len(files)))

	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(path, file.Name())
			err := uploadFile(filePath, file.Name())
			if err != nil {
				fmt.Printf("[ERROR] Failed to upload %s: %s\n", file.Name(), err)
			}
			bar.Add(1)
		}
	}

	// Trigger distribution
	_, err = http.Post(awsURL+"/distribute", "application/json", nil)
	if err != nil {
		fmt.Println("[ERROR] Failed to trigger distribution:", err)
		return
	}

	fmt.Println("[INFO] Distribution initiated successfully")
}

func uploadFile(filePath, fileName string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", awsURL+"/upload", bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("File-Name", fileName)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload failed with status: %d", resp.StatusCode)
	}

	return nil
}

func showStatus() {
	mu.RLock()
	defer mu.RUnlock()

	fmt.Println("\n=== Node Status ===")
	fmt.Printf("Node ID: %s\n", node.ID)
	fmt.Printf("MAC Address: %s\n", node.MacID)
	fmt.Printf("IP Address: %s\n", node.IP)
	fmt.Printf("Port: %d\n", node.Port)
	fmt.Printf("Status: %s\n", node.Status)
	fmt.Printf("Capacity: %d MB\n", node.Capacity)
	fmt.Printf("Used: %d MB\n", node.Used)
	fmt.Printf("Chunks Stored: %d\n", len(localIndex.Chunks))
}

func listFiles() {
	mu.RLock()
	defer mu.RUnlock()

	fmt.Printf("\n=== Stored Files (Port %d) ===\n", node.Port)
	if len(localIndex.Chunks) == 0 {
		fmt.Println("No files stored")
		return
	}

	for _, chunk := range localIndex.Chunks {
		fmt.Printf("%s (%.2f KB)\n", chunk.ChunkID, float64(chunk.Size))
	}
}

func parseInt(s string) int {
	val := 0
	fmt.Sscanf(s, "%d", &val)
	return val
}
