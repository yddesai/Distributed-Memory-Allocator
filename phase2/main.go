package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"phase2/config" // Changed from "./config"
	"phase2/types"  // Changed from "./types"
	"phase2/utils"  // Changed from "./utils"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

type MasterIndex struct {
	Chunks    map[string]types.ChunkInfo `json:"chunks"`
	IDMapping map[int]string             `json:"id_mapping"` // record ID to chunk mapping
}

var (
	nodes         = make(map[string]types.Node) // Node registry
	lastHeartbeat = make(map[string]int64)      // Last heartbeat time for each node
	masterIndex   = MasterIndex{
		Chunks:    make(map[string]types.ChunkInfo),
		IDMapping: make(map[int]string),
	}
	mu           sync.RWMutex                  // Mutex for thread-safe access
	backupFolder = config.BACKUP_FOLDER        // Backup folder for datasets
	queryPool    = make(chan types.Query, 100) // Query pool
	metrics      = types.SystemMetrics{}
)

func main() {
	// Initialize system
	os.Remove("nodes.json") // Start fresh
	os.Remove("master_index.json")

	initializeSystem()
	startWorkers()

	// Setup HTTP handlers
	http.HandleFunc("/register", registerNode)
	http.HandleFunc("/heartbeat", handleHeartbeat)
	http.HandleFunc("/nodes", listNodes)
	http.HandleFunc("/upload", uploadFiles)
	http.HandleFunc("/distribute", distributeData)
	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/metrics", getMetrics)
	http.HandleFunc("/localtable", updateLocalTable)

	fmt.Printf("[INFO] AWS Coordinator starting on port %d...\n", config.AWS_PORT)
	printSystemStatus()

	err := http.ListenAndServe(fmt.Sprintf(":%d", config.AWS_PORT), nil)
	if err != nil {
		fmt.Printf("[ERROR] Server failed: %v\n", err)
		os.Exit(1)
	}
}

func initializeSystem() {
	ensureBackupFolder()
	loadMasterIndex()
	loadNodesFromFile()

	// Initialize metrics
	updateSystemMetrics()
}

func startWorkers() {
	// Start background workers
	go monitorNodes()
	go processQueries()
	go periodicMetricsUpdate()
}

func updateSystemMetrics() {
	mu.RLock()
	defer mu.RUnlock()

	activeCount := 0
	totalSize := int64(0)

	for _, node := range nodes {
		if node.Status == "active" {
			activeCount++
		}
		totalSize += int64(node.Used)
	}

	metrics = types.SystemMetrics{
		TotalNodes:        len(nodes),
		ActiveNodes:       activeCount,
		TotalDataSize:     totalSize,
		TotalChunks:       len(masterIndex.Chunks),
		DistributedChunks: len(masterIndex.IDMapping),
	}
}

func periodicMetricsUpdate() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		updateSystemMetrics()
		printSystemStatus()
	}
}

func processQueries() {
	for query := range queryPool {
		handleQueryExecution(query)
	}
}

func handleQueryExecution(query types.Query) {
	mu.RLock()
	chunkID, exists := masterIndex.IDMapping[query.ID]
	mu.RUnlock()

	if !exists {
		fmt.Printf("[ERROR] Record ID %d not found\n", query.ID)
		return
	}

	mu.RLock()
	chunk := masterIndex.Chunks[chunkID]
	node, exists := nodes[chunk.NodeID]
	mu.RUnlock()

	if !exists || node.Status != "active" {
		fmt.Printf("[ERROR] Node %s not available\n", chunk.NodeID)
		return
	}

	// Forward query to appropriate node
	url := fmt.Sprintf("http://%s:%d/query", node.IP, node.Port)
	sendQueryToNode(url, query)
}

func printSystemStatus() {
	mu.RLock()
	defer mu.RUnlock()

	fmt.Println("\n=== System Status ===")
	fmt.Printf("Active Nodes: %d/%d\n", metrics.ActiveNodes, metrics.TotalNodes)
	fmt.Printf("Total Data: %.2f GB\n", float64(metrics.TotalDataSize)/(1024*1024*1024))
	fmt.Printf("Chunks: %d (Distributed: %d)\n", metrics.TotalChunks, metrics.DistributedChunks)

	fmt.Println("\n=== Node Status ===")
	for _, node := range nodes {
		fmt.Printf("Node %s (%s:%d):\n", node.ID, node.IP, node.Port)
		fmt.Printf("  Capacity: %d MB, Used: %d MB, Status: %s\n",
			node.Capacity, node.Used, node.Status)
	}
}

// ... [continuing in next part]

// ... [continuing from previous part]

func updateLocalTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var localTable types.LocalTable
	if err := json.NewDecoder(r.Body).Decode(&localTable); err != nil {
		http.Error(w, "Invalid data", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Lock()

	// Update master index based on local table
	for chunkID, chunkInfo := range localTable.Chunks {
		masterIndex.Chunks[chunkID] = chunkInfo
		// Update ID mapping
		for id := chunkInfo.StartID; id <= chunkInfo.EndID; id++ {
			masterIndex.IDMapping[id] = chunkID
		}
	}

	// Update node metrics
	if node, exists := nodes[localTable.NodeID]; exists {
		node.Used = localTable.Used
		nodes[localTable.NodeID] = node
	}

	saveMasterIndex()
	updateSystemMetrics()
}

func distributeFiles() error {
	mu.Lock()
	defer mu.Unlock()

	files, err := ioutil.ReadDir(backupFolder)
	if err != nil {
		return fmt.Errorf("failed to read backup folder: %s", err)
	}

	// Get active nodes
	activeNodes := []types.Node{}
	totalCapacity := 0
	for _, node := range nodes {
		if node.Status == "active" {
			activeNodes = append(activeNodes, node)
			totalCapacity += node.Capacity
		}
	}

	if len(activeNodes) == 0 {
		return fmt.Errorf("no active nodes available")
	}

	// Calculate total size of files
	var totalSize int64
	for _, file := range files {
		totalSize += file.Size()
	}

	// Calculate distribution based on capacity
	distribution := utils.CalculateDistribution(totalSize, activeNodes)

	fmt.Printf("\n[INFO] Distribution plan:\n")
	for nodeID, size := range distribution {
		fmt.Printf("Node %s: %.2f MB\n", nodeID, float64(size)/(1024*1024))
	}

	// Distribute files
	bar := progressbar.Default(int64(len(files)))
	currentNode := 0
	currentSize := int64(0)

	for _, file := range files {
		// Select node for this file
		for currentSize >= distribution[activeNodes[currentNode].ID] {
			currentNode++
			currentSize = 0
			if currentNode >= len(activeNodes) {
				return fmt.Errorf("insufficient capacity")
			}
		}

		node := activeNodes[currentNode]
		err := sendFileToNode(node, file.Name())
		if err != nil {
			fmt.Printf("[ERROR] Failed to send %s to %s: %s\n",
				file.Name(), node.ID, err)
			continue
		}

		// Update chunk information
		chunkInfo := createChunkInfo(file, node.ID)
		masterIndex.Chunks[chunkInfo.ChunkID] = chunkInfo

		currentSize += file.Size()
		bar.Add(1)
	}

	saveMasterIndex()
	return nil
}

func createChunkInfo(file os.FileInfo, nodeID string) types.ChunkInfo {
	// Extract ID range from filename or content if needed
	// This is a simplified version
	return types.ChunkInfo{
		ChunkID: file.Name(),
		Size:    file.Size(),
		NodeID:  nodeID,
		Status:  "active",
	}
}

func triggerRedistribution() {
	fmt.Println("[INFO] Starting redistribution...")

	// Collect chunks from inactive nodes
	chunksToRedistribute := []types.ChunkInfo{}
	activeNodes := []types.Node{}

	mu.RLock()
	for _, chunk := range masterIndex.Chunks {
		if nodes[chunk.NodeID].Status != "active" {
			chunksToRedistribute = append(chunksToRedistribute, chunk)
		}
	}

	for _, node := range nodes {
		if node.Status == "active" {
			activeNodes = append(activeNodes, node)
		}
	}
	mu.RUnlock()

	if len(chunksToRedistribute) == 0 {
		return
	}

	// Calculate new distribution
	newDistribution := utils.RecalculateDistribution(chunksToRedistribute, activeNodes)

	// Redistribute chunks
	for nodeID, chunks := range newDistribution {
		node := nodes[nodeID]
		fmt.Printf("[INFO] Redistributing %d chunks to %s\n", len(chunks), node.ID)

		for _, chunk := range chunks {
			// Read from backup and send to new node
			data, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", backupFolder, chunk.ChunkID))
			if err != nil {
				fmt.Printf("[ERROR] Failed to read chunk %s: %s\n", chunk.ChunkID, err)
				continue
			}

			err = sendFileToNode(node, chunk.ChunkID)
			if err != nil {
				fmt.Printf("[ERROR] Failed to redistribute chunk %s: %s\n", chunk.ChunkID, err)
				continue
			}

			mu.Lock()
			chunk.NodeID = node.ID
			masterIndex.Chunks[chunk.ChunkID] = chunk
			mu.Unlock()
		}
	}

	saveMasterIndex()
	updateSystemMetrics()
	fmt.Println("[INFO] Redistribution completed")
}

func saveMasterIndex() {
	data, err := json.MarshalIndent(masterIndex, "", "  ")
	if err != nil {
		fmt.Println("[ERROR] Failed to save master index:", err)
		return
	}

	err = ioutil.WriteFile("master_index.json", data, 0644)
	if err != nil {
		fmt.Println("[ERROR] Failed to write master index:", err)
	}
}

func loadMasterIndex() {
	data, err := ioutil.ReadFile("master_index.json")
	if err != nil {
		fmt.Println("[INFO] No existing master index found")
		return
	}

	err = json.Unmarshal(data, &masterIndex)
	if err != nil {
		fmt.Println("[ERROR] Failed to load master index:", err)
	}
}

// ... [Previous code remains the same]
