package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"phase2/config"
	"phase2/types"
	"strings"
	"sync"
	"time"
	"regexp"

	"github.com/schollz/progressbar/v3"
)

var (
	nodes         = make(map[string]types.Node) // Node registry
	lastHeartbeat = make(map[string]int64)      // Last heartbeat time for each node
	dataIndex     = make(map[string][]string)   // Tracks which node has which files
	mu            sync.RWMutex                  // Mutex for thread-safe access
	backupFolder  = config.BACKUP_FOLDER        // Backup folder for datasets
)

const (
	CHUNK_SIZE = 8500 // records per chunk
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

	files, err := ioutil.ReadDir(backupFolder)
	if err != nil {
		return fmt.Errorf("failed to read backup folder: %v", err)
	}

	// Process each JSON file
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			err := processJSONFile(file.Name(), activeNodes)
			if err != nil {
				fmt.Printf("[WARNING] Failed to process %s: %v\n", file.Name(), err)
				continue
			}
		}
	}

	return nil
}

func processJSONFile(fileName string, activeNodes []types.Node) error {
	filePath := fmt.Sprintf("%s/%s", backupFolder, fileName)

	// Read file content
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	// Clean and fix JSON data
	cleanData := string(data)

	// Remove any non-printable characters
	re := regexp.MustCompile(`[\x00-\x1F\x7F]`)
	cleanData = re.ReplaceAllString(cleanData, "")

	// Fix common JSON issues
	cleanData = strings.ReplaceAll(cleanData, "-", "")      // Remove hyphens from phone numbers
	cleanData = strings.ReplaceAll(cleanData, "e+", "e")    // Fix scientific notation
	cleanData = strings.ReplaceAll(cleanData, "e-", "e")    // Fix scientific notation
	cleanData = strings.ReplaceAll(cleanData, "NaN", "0")   // Replace NaN with 0
	cleanData = strings.ReplaceAll(cleanData, "Infinity", "0") // Replace Infinity with 0

	// Parse JSON using `json.Number` to handle potential type mismatches
	var rawData interface{}
	decoder := json.NewDecoder(strings.NewReader(cleanData))
	decoder.UseNumber() // Ensure numbers are decoded as `json.Number`
	err = decoder.Decode(&rawData)
	if err != nil {
		return fmt.Errorf("failed to parse JSON: %v", err)
	}

	// Handle different JSON structures
	var records []map[string]interface{}
	switch v := rawData.(type) {
	case []interface{}: // Root element is an array
		for _, item := range v {
			if record, ok := item.(map[string]interface{}); ok {
				records = append(records, record)
			} else {
				return fmt.Errorf("unexpected JSON format: item is not an object")
			}
		}
	case map[string]interface{}: // Root element is an object
		// Flatten the object into a single record
		records = append(records, v)
	default:
		return fmt.Errorf("unexpected JSON format: root element is not an array or object")
	}

	totalRecords := len(records)
	chunks := createChunks(records, totalRecords, len(activeNodes))

	// Create master index
	masterIndex := types.MasterIndex{
		TotalRecords: totalRecords,
		Chunks:       make([]types.ChunkInfo, 0),
	}

	// Distribute chunks to nodes
	for i, chunk := range chunks {
		nodeIndex := i % len(activeNodes)
		node := activeNodes[nodeIndex]

		chunkFileName := fmt.Sprintf("%s_chunk_%d.json", strings.TrimSuffix(fileName, ".json"), i)
		chunkPath := fmt.Sprintf("%s/%s", backupFolder, chunkFileName)

		// Save chunk to file
		chunkData, err := json.MarshalIndent(chunk.Records, "", "  ") // Use MarshalIndent for better formatting
		if err != nil {
			return fmt.Errorf("failed to marshal chunk: %v", err)
		}

		err = ioutil.WriteFile(chunkPath, chunkData, 0644)
		if err != nil {
			return fmt.Errorf("failed to write chunk file: %v", err)
		}

		// Create chunk info
		chunkInfo := types.ChunkInfo{
			ChunkID:    fmt.Sprintf("chunk_%d", i),
			StartID:    chunk.StartID,
			EndID:      chunk.EndID,
			Size:       int64(len(chunkData)),
			NodeID:     node.ID,
			Status:     "active",
			SourcePath: chunkPath,
			FileName:   chunkFileName,
		}

		masterIndex.Chunks = append(masterIndex.Chunks, chunkInfo)

		// Transfer chunk to node
		nodeAddr := fmt.Sprintf("http://%s:%d", node.IP, node.Port)
		err = transferFileToNode(chunkInfo, nodeAddr)
		if err != nil {
			fmt.Printf("[WARNING] Failed to transfer chunk to node %s: %v\n", node.ID, err)
			continue
		}

		// Update data index
		mu.Lock()
		dataIndex[chunkFileName] = append(dataIndex[chunkFileName], node.ID)
		mu.Unlock()
	}

	// Save master index
	indexFileName := strings.TrimSuffix(fileName, ".json") + "_index.json"
	indexPath := fmt.Sprintf("%s/%s", backupFolder, indexFileName)
	indexData, _ := json.MarshalIndent(masterIndex, "", "  ")
	err = ioutil.WriteFile(indexPath, indexData, 0644)
	if err != nil {
		return fmt.Errorf("failed to save master index: %v", err)
	}

	return nil
}



type Chunk struct {
	StartID  int
	EndID    int
	Records  []map[string]interface{}
}

func createChunks(records []map[string]interface{}, totalRecords, nodeCount int) []Chunk {
	chunks := make([]Chunk, 0)
	recordsPerChunk := CHUNK_SIZE
	
	for i := 0; i < totalRecords; i += recordsPerChunk {
		end := i + recordsPerChunk
		if end > totalRecords {
			end = totalRecords
		}

		chunk := Chunk{
			StartID: i,
			EndID:   end - 1,
			Records: records[i:end],
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}

func transferFileToNode(chunk types.ChunkInfo, nodeAddr string) error {
	file, err := os.Open(chunk.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create request to node's upload endpoint
	req, err := http.NewRequest("POST", nodeAddr+"/receive", file)
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
