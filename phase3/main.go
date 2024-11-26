package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Node structure
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

// ChunkInfo stores information about each data chunk
type ChunkInfo struct {
	ChunkID string `json:"chunk_id"`
	IDRange struct {
		Start int `json:"start"`
		End   int `json:"end"`
	} `json:"id_range"`
	Size     int    `json:"size"`     // in KB
	Location string `json:"location"` // laptop ID
	Status   string `json:"status"`
}

// SystemMetrics stores system-wide metrics
type SystemMetrics struct {
	TotalData      int            `json:"total_data"` // in MB
	TotalChunks    int            `json:"total_chunks"`
	ActiveNodes    int            `json:"active_nodes"`
	ChunksPerNode  map[string]int `json:"chunks_per_node"`
	DataPerNode    map[string]int `json:"data_per_node"` // in MB
	QueryCount     int            `json:"query_count"`
	LastUpdateTime time.Time      `json:"last_update_time"`
}

var (
	nodes         = make(map[string]Node)      // Node registry
	chunks        = make(map[string]ChunkInfo) // Chunk registry
	lastHeartbeat = make(map[string]int64)     // Last heartbeat time
	dataIndex     = make(map[string][]string)  // Node to chunks mapping
	metrics       = SystemMetrics{
		ChunksPerNode: make(map[string]int),
		DataPerNode:   make(map[string]int),
	}
	mu           sync.RWMutex   // Mutex for thread-safe access
	backupFolder = "aws_backup" // Backup folder for datasets
)

func main() {
	// Delete old nodes.json file to start fresh
	os.Remove("nodes.json")

	loadNodesFromFile()
	ensureBackupFolder()

	fmt.Println("[INFO] AWS Coordinator starting...")
	printSystemMetrics()

	// Setup HTTP handlers
	http.HandleFunc("/register", registerNode)
	http.HandleFunc("/heartbeat", handleHeartbeat)
	http.HandleFunc("/nodes", listNodes)
	http.HandleFunc("/upload", uploadFiles)
	http.HandleFunc("/distribute", distributeData)
	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/metrics", getMetrics)

	// Start background tasks
	go monitorNodes()
	go updateMetrics()

	fmt.Println("[INFO] AWS Coordinator running on port 8080...")
	http.ListenAndServe(":8080", nil)
}

func updateMetrics() {
	for {
		mu.RLock()
		metrics.ActiveNodes = 0
		metrics.TotalChunks = len(chunks)
		metrics.ChunksPerNode = make(map[string]int)
		metrics.DataPerNode = make(map[string]int)

		for _, node := range nodes {
			if node.Status == "active" {
				metrics.ActiveNodes++
				metrics.ChunksPerNode[node.ID] = len(dataIndex[node.MacID])
				metrics.DataPerNode[node.ID] = node.Used
			}
		}
		metrics.LastUpdateTime = time.Now()
		mu.RUnlock()

		time.Sleep(5 * time.Second)
	}
}

func printSystemMetrics() {
	mu.RLock()
	defer mu.RUnlock()

	fmt.Println("\n=== SYSTEM METRICS ===")
	fmt.Printf("Total Connected Devices: %d\n", metrics.ActiveNodes)
	fmt.Printf("Total Data Size: %d MB\n", metrics.TotalData)
	fmt.Printf("Total Chunks: %d\n", metrics.TotalChunks)

	fmt.Println("\nDEVICE METRICS:")
	for _, node := range nodes {
		fmt.Printf("\n%s (Capacity: %dMB):\n", node.ID, node.Capacity)
		fmt.Printf("├── Used: %dMB\n", node.Used)
		fmt.Printf("├── Available: %dMB\n", node.Capacity-node.Used)
		fmt.Printf("├── Chunks: %d\n", metrics.ChunksPerNode[node.ID])
		fmt.Printf("├── Load: %.1f%%\n", float64(node.Used)/float64(node.Capacity)*100)
		fmt.Printf("└── Status: %s\n", node.Status)
	}
	fmt.Println(strings.Repeat("-", 50))
}

func ensureBackupFolder() {
	if _, err := os.Stat(backupFolder); os.IsNotExist(err) {
		err := os.Mkdir(backupFolder, 0755)
		if err != nil {
			fmt.Println("[ERROR] Failed to create backup folder:", err)
		} else {
			fmt.Println("[INFO] Backup folder created.")
		}
	}
}

func loadNodesFromFile() {
	data, err := ioutil.ReadFile("nodes.json")
	if err != nil {
		fmt.Println("[INFO] No saved nodes found, starting fresh.")
		return
	}

	err = json.Unmarshal(data, &nodes)
	if err != nil {
		fmt.Println("[ERROR] Error loading nodes:", err)
	}
}

func saveNodesToFile() {
	data, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		fmt.Println("[ERROR] Error saving nodes:", err)
		return
	}

	err = ioutil.WriteFile("nodes.json", data, 0644)
	if err != nil {
		fmt.Println("[ERROR] Error writing to file:", err)
	}
}

func registerNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var node Node
	err := json.NewDecoder(r.Body).Decode(&node)
	if err != nil {
		http.Error(w, "Invalid JSON data", http.StatusBadRequest)
		return
	}

	mu.Lock()
	node.Status = "active"
	node.LastSeen = time.Now()
	nodes[node.MacID] = node
	lastHeartbeat[node.MacID] = time.Now().Unix()
	saveNodesToFile()
	mu.Unlock()

	fmt.Printf("\n[INFO] New device connected: %s (IP:%s Port:%d Capacity:%dMB)\n",
		node.ID, node.IP, node.Port, node.Capacity)
	printSystemMetrics()
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var node Node
	err := json.NewDecoder(r.Body).Decode(&node)
	if err != nil {
		http.Error(w, "Invalid JSON data", http.StatusBadRequest)
		return
	}

	mu.Lock()
	if existingNode, exists := nodes[node.MacID]; exists {
		existingNode.LastSeen = time.Now()
		existingNode.Status = "active"
		existingNode.IP = node.IP
		existingNode.Port = node.Port
		existingNode.Used = node.Used
		nodes[node.MacID] = existingNode
		lastHeartbeat[node.MacID] = time.Now().Unix()
	}
	mu.Unlock()
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	queryID := r.URL.Query().Get("id")
	if queryID == "" {
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	mu.RLock()
	// Find chunk containing the ID
	var targetNode Node
	var chunkID string
	found := false

	for id, chunk := range chunks {
		if chunk.IDRange.Start <= parseInt(queryID) && parseInt(queryID) <= chunk.IDRange.End {
			for _, node := range nodes {
				if node.ID == chunk.Location && node.Status == "active" {
					targetNode = node
					chunkID = id
					found = true
					break
				}
			}
			break
		}
	}
	mu.RUnlock()

	if !found {
		http.Error(w, "Record not found", http.StatusNotFound)
		return
	}

	// Forward query to target node
	url := fmt.Sprintf("http://%s:%d/query?chunk=%s&id=%s", targetNode.IP, targetNode.Port, chunkID, queryID)
	resp, err := http.Get(url)
	if err != nil {
		http.Error(w, "Failed to retrieve data from node", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	// Copy response to client
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Failed to read response from node", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)

	mu.Lock()
	metrics.QueryCount++
	mu.Unlock()
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	mu.RLock()
	data, err := json.MarshalIndent(metrics, "", "  ")
	mu.RUnlock()

	if err != nil {
		http.Error(w, "Failed to generate metrics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func uploadFiles(w http.ResponseWriter, r *http.Request) {
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

	destPath := backupFolder + "/" + fileName
	err = ioutil.WriteFile(destPath, body, 0644)
	if err != nil {
		http.Error(w, "Failed to save file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("[INFO] File '%s' received and stored in backup.\n", fileName)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "File '%s' received and saved.", fileName)
}

func listNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	mu.RLock()
	defer mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

func monitorNodes() {
	for {
		time.Sleep(10 * time.Second)
		mu.Lock()
		now := time.Now().Unix()
		statusChanged := false

		for mac, last := range lastHeartbeat {
			if now-last > 15 {
				node := nodes[mac]
				if node.Status == "active" {
					node.Status = "inactive"
					nodes[mac] = node
					statusChanged = true
					fmt.Printf("\n[INFO] Node '%s' became inactive\n", node.ID)
				}
			}
		}

		if statusChanged {
			printSystemMetrics()
			triggerRedistribution()
		}
		mu.Unlock()
	}
}

func triggerRedistribution() {
	fmt.Println("[INFO] Triggering redistribution due to node status change.")
	err := distributeFiles()
	if err != nil {
		fmt.Printf("[ERROR] Redistribution failed: %s\n", err)
	}
}

func parseInt(s string) int {
	val := 0
	fmt.Sscanf(s, "%d", &val)
	return val
}

func getNodeByID(id string) *Node {
	for _, node := range nodes {
		if node.ID == id {
			return &node
		}
	}
	return nil
}
