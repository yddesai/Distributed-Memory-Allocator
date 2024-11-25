package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"phase2/config" // Changed from "./config"
	"phase2/types"  // Changed from "./types"
	"sync"
	"time"
	// Changed from "./utils"
)

var (
	localFolder = config.LOCAL_FOLDER
	awsURL      = config.DefaultConfig.AWSURL
	node        types.Node
	localTable  types.LocalTable
	tableLock   sync.RWMutex
)

func main() {
	// Take capacity as command line argument or use default
	capacity := 500 // Default 500MB
	if len(os.Args) > 1 {
		fmt.Sscanf(os.Args[1], "%d", &capacity)
	}

	os.MkdirAll(localFolder, 0755)
	initializeNode(capacity)

	// Start background processes
	go sendHeartbeats()
	go monitorLocalStorage()
	go syncLocalTable()

	// Setup HTTP handlers
	http.HandleFunc("/receive", receiveFiles)
	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/status", handleStatus)

	// Start command interface in a goroutine
	go commandInterface()

	fmt.Printf("[INFO] Laptop node %s started. Listening on port %d...\n", node.ID, node.Port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil)
	if err != nil {
		fmt.Printf("[ERROR] Failed to start server: %v\n", err)
		os.Exit(1)
	}
}

func initializeNode(capacity int) bool {
	// Generate unique ID
	nodeID := fmt.Sprintf("laptop-%d", time.Now().Unix()%1000)
	ip := getPublicIP()

	// Check for existing nodes with same IP
	existingNode := checkExistingNodesWithSameIP(ip)
	port := findAvailablePort(existingNode)

	if port == -1 {
		return false
	}

	node = types.Node{
		ID:       nodeID,
		Capacity: capacity,
		MacID:    getMacAddress(),
		IP:       ip,
		Port:     port,
		Status:   "active",
	}

	// Initialize local table
	localTable = types.LocalTable{
		NodeID:   nodeID,
		Chunks:   make(map[string]types.ChunkInfo),
		Capacity: capacity,
		Used:     0,
		LastSync: time.Now(),
	}

	fmt.Printf("[INFO] Initialized node: %s (MAC: %s, IP: %s, Port: %d, Capacity: %dMB)\n",
		node.ID, node.MacID, node.IP, node.Port, capacity)
	return true
}

func monitorLocalStorage() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		updateLocalStorage()
	}
}

func updateLocalStorage() {
	files, err := ioutil.ReadDir(localFolder)
	if err != nil {
		fmt.Printf("[ERROR] Failed to read local folder: %s\n", err)
		return
	}

	tableLock.Lock()
	defer tableLock.Unlock()

	var totalSize int64
	for _, file := range files {
		totalSize += file.Size()
	}

	localTable.Used = int(totalSize / 1024 / 1024) // Convert to MB
	localTable.LastSync = time.Now()

	// Print storage metrics
	fmt.Printf("\n=== Storage Status ===\n")
	fmt.Printf("Capacity: %d MB\n", localTable.Capacity)
	fmt.Printf("Used: %d MB\n", localTable.Used)
	fmt.Printf("Available: %d MB\n", localTable.Capacity-localTable.Used)
	fmt.Printf("Chunks: %d\n", len(localTable.Chunks))
}

func syncLocalTable() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		sendLocalTableToAWS()
	}
}

func sendLocalTableToAWS() {
	tableLock.RLock()
	data, err := json.Marshal(localTable)
	tableLock.RUnlock()

	if err != nil {
		fmt.Printf("[ERROR] Failed to marshal local table: %s\n", err)
		return
	}

	resp, err := http.Post(fmt.Sprintf("%s/localtable", awsURL),
		"application/json", bytes.NewBuffer(data))
	if err != nil {
		fmt.Printf("[ERROR] Failed to sync local table: %s\n", err)
		return
	}
	defer resp.Body.Close()
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var query types.Query
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		http.Error(w, "Invalid query", http.StatusBadRequest)
		return
	}

	// Process query
	result, err := processQuery(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(result)
}

func processQuery(query types.Query) (interface{}, error) {
	tableLock.RLock()
	defer tableLock.RUnlock()

	// Find the chunk containing the requested ID
	for _, chunk := range localTable.Chunks {
		if query.ID >= chunk.StartID && query.ID <= chunk.EndID {
			// Read and parse the chunk file
			data, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", localFolder, chunk.ChunkID))
			if err != nil {
				return nil, err
			}

			// Parse JSON and find specific record
			var records []map[string]interface{}
			err = json.Unmarshal(data, &records)
			if err != nil {
				return nil, err
			}

			// Find the specific record
			for _, record := range records {
				if int(record["id"].(float64)) == query.ID {
					return record, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("record not found")
}

func receiveFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fileName := r.Header.Get("File-Name")
	if fileName == "" {
		http.Error(w, "File name missing", http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read data", http.StatusInternalServerError)
		return
	}

	// Check capacity
	tableLock.RLock()
	if localTable.Used+len(body)/(1024*1024) > localTable.Capacity {
		tableLock.RUnlock()
		http.Error(w, "Insufficient capacity", http.StatusInsufficientStorage)
		return
	}
	tableLock.RUnlock()

	// Save file
	filePath := fmt.Sprintf("%s/%s", localFolder, fileName)
	err = ioutil.WriteFile(filePath, body, 0644)
	if err != nil {
		http.Error(w, "Failed to save file", http.StatusInternalServerError)
		return
	}

	// Update local table
	updateChunkInfo(fileName, body)

	fmt.Printf("[INFO] Received file: %s (Size: %.2f MB)\n",
		fileName, float64(len(body))/(1024*1024))
	w.WriteHeader(http.StatusOK)
}

func updateChunkInfo(fileName string, data []byte) {
	// Parse the chunk to get ID range
	var records []map[string]interface{}
	err := json.Unmarshal(data, &records)
	if err != nil {
		fmt.Printf("[ERROR] Failed to parse chunk data: %s\n", err)
		return
	}

	if len(records) == 0 {
		return
	}

	startID := int(records[0]["id"].(float64))
	endID := int(records[len(records)-1]["id"].(float64))

	tableLock.Lock()
	localTable.Chunks[fileName] = types.ChunkInfo{
		ChunkID: fileName,
		StartID: startID,
		EndID:   endID,
		Size:    int64(len(data)),
		NodeID:  node.ID,
		Status:  "active",
	}
	tableLock.Unlock()
}

// ... [Previous helper functions remain the same]
