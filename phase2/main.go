package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"phase2/config"
	"phase2/types"
	"phase2/utils"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

var (
	nodes         = make(map[string]types.Node) // Node registry
	lastHeartbeat = make(map[string]int64)      // Last heartbeat time for each node
	dataIndex     = make(map[string][]string)   // Tracks which node has which files
	mu            sync.RWMutex                  // Mutex for thread-safe access
	backupFolder  = config.BACKUP_FOLDER        // Backup folder for datasets
)

func main() {
	// Delete old nodes.json file to start fresh
	os.Remove("nodes.json")

	loadNodesFromFile()
	ensureBackupFolder()

	fmt.Println("[INFO] AWS Coordinator starting...")
	printConnectedDevices()

	http.HandleFunc("/register", registerNode)
	http.HandleFunc("/heartbeat", handleHeartbeat)
	http.HandleFunc("/nodes", listNodes)
	http.HandleFunc("/upload", uploadFiles)
	http.HandleFunc("/distribute", distributeData)
	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/metrics", getMetrics)
	http.HandleFunc("/localtable", updateLocalTable)

	go monitorNodes()

	fmt.Println("[INFO] AWS Coordinator running on port 8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Printf("[ERROR] Server failed: %v\n", err)
		os.Exit(1)
	}
}

func registerNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var node types.Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
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

	fmt.Printf("[INFO] New node registered: %s\n", node.ID)
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var node types.Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, "Invalid JSON data", http.StatusBadRequest)
		return
	}

	mu.Lock()
	if existingNode, exists := nodes[node.MacID]; exists {
		existingNode.LastSeen = time.Now()
		existingNode.Status = "active"
		nodes[node.MacID] = existingNode
		lastHeartbeat[node.MacID] = time.Now().Unix()
	}
	mu.Unlock()
}

func listNodes(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()
	json.NewEncoder(w).Encode(nodes)
}

func uploadFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fileName := r.Header.Get("File-Name")
	if fileName == "" {
		http.Error(w, "File name missing", http.StatusBadRequest)
		return
	}

	// Get content length for progress bar
	contentLength := r.ContentLength
	if contentLength <= 0 {
		http.Error(w, "Content-Length header required", http.StatusBadRequest)
		return
	}

	// Create progress bar
	bar := progressbar.NewOptions64(
		contentLength,
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]Receiving %s", fileName)),
		progressbar.OptionSetWidth(30),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Create file
	file, err := os.Create(fmt.Sprintf("%s/%s", backupFolder, fileName))
	if err != nil {
		http.Error(w, "Failed to create file", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Copy data with progress bar
	_, err = io.Copy(io.MultiWriter(file, bar), r.Body)
	if err != nil {
		http.Error(w, "Failed to save file", http.StatusInternalServerError)
		return
	}

	fmt.Printf("\n[INFO] File received: %s\n", fileName)
}

func distributeData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := distributeFiles()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println("[INFO] Distribution completed")
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

	// Process query using utils.QueryHandler
	// Implementation needed
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()

	metrics := types.SystemMetrics{
		TotalNodes:  len(nodes),
		ActiveNodes: countActiveNodes(),
		// Add other metrics
	}

	json.NewEncoder(w).Encode(metrics)
}

func updateLocalTable(w http.ResponseWriter, r *http.Request) {
	// Implementation needed
}

func ensureBackupFolder() {
	if _, err := os.Stat(backupFolder); os.IsNotExist(err) {
		os.MkdirAll(backupFolder, 0755)
	}
}

func loadNodesFromFile() {
	data, err := ioutil.ReadFile("nodes.json")
	if err != nil {
		return
	}
	json.Unmarshal(data, &nodes)
}

func saveNodesToFile() {
	data, _ := json.MarshalIndent(nodes, "", "  ")
	ioutil.WriteFile("nodes.json", data, 0644)
}

func monitorNodes() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		checkNodes()
	}
}

func checkNodes() {
	mu.Lock()
	defer mu.Unlock()

	now := time.Now().Unix()
	for mac, lastSeen := range lastHeartbeat {
		if now-lastSeen > 30 { // 30 seconds timeout
			if node, exists := nodes[mac]; exists && node.Status == "active" {
				node.Status = "inactive"
				nodes[mac] = node
				fmt.Printf("[INFO] Node %s marked as inactive\n", node.ID)
			}
		}
	}
}

func countActiveNodes() int {
	count := 0
	for _, node := range nodes {
		if node.Status == "active" {
			count++
		}
	}
	return count
}

func distributeFiles() error {
	mu.RLock()
	activeNodes := make([]types.Node, 0)
	for _, node := range nodes {
		if node.Status == "active" {
			activeNodes = append(activeNodes, node)
		}
	}
	mu.RUnlock()

	if len(activeNodes) == 0 {
		return fmt.Errorf("no active nodes available for distribution")
	}

	// Get list of files from backup folder
	files, err := ioutil.ReadDir(backupFolder)
	if err != nil {
		return fmt.Errorf("failed to read backup folder: %v", err)
	}

	// Calculate total size
	var totalSize int64
	fileInfos := make([]types.FileInfo, 0)
	for _, file := range files {
		if !file.IsDir() {
			fileInfos = append(fileInfos, types.FileInfo{
				Name: file.Name(),
				Size: file.Size(),
				Path: fmt.Sprintf("%s/%s", backupFolder, file.Name()),
			})
			totalSize += file.Size()
		}
	}

	// Get target distribution
	// Create chunks from files (simple 1:1 mapping for now)
	chunks := make([]types.ChunkInfo, 0)
	for _, file := range fileInfos {
		chunk := types.ChunkInfo{
			FileName:   file.Name,
			Size:       file.Size,
			SourcePath: file.Path,
		}
		chunks = append(chunks, chunk)
	}

	// Get chunk distribution
	chunkDistribution := utils.RecalculateDistribution(chunks, activeNodes)

	// Transfer files to nodes
	for nodeID, nodeChunks := range chunkDistribution {
		// Find node's address
		var nodeAddr string
		for _, node := range activeNodes {
			if node.ID == nodeID {
				nodeAddr = fmt.Sprintf("http://%s:%d", node.IP, node.Port)
				break
			}
		}

		// Transfer each chunk to the node
		for _, chunk := range nodeChunks {
			if err := transferFileToNode(chunk, nodeAddr); err != nil {
				fmt.Printf("[WARNING] Failed to transfer %s to node %s: %v\n",
					chunk.FileName, nodeID, err)
				continue
			}

			// Update data index
			mu.Lock()
			dataIndex[chunk.FileName] = append(dataIndex[chunk.FileName], nodeID)
			mu.Unlock()

			fmt.Printf("[INFO] Transferred %s to node %s\n", chunk.FileName, nodeID)
		}
	}

	return nil
}

func transferFileToNode(chunk types.ChunkInfo, nodeAddr string) error {
	file, err := os.Open(chunk.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create request to node's upload endpoint
	req, err := http.NewRequest("POST", nodeAddr+"/upload", file)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("File-Name", chunk.FileName)
	req.Header.Set("Content-Type", "application/octet-stream")

	// Send request
	client := &http.Client{
		Timeout: 10 * time.Minute, // Longer timeout for large files
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("node returned error: %s", string(body))
	}

	return nil
}

func printConnectedDevices() {
	mu.RLock()
	defer mu.RUnlock()

	fmt.Println("\n=== Connected Devices ===")
	for _, node := range nodes {
		fmt.Printf("Node: %s, Status: %s\n", node.ID, node.Status)
	}
}
