// main.go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net"
    "net/http"
    "os"
    "sort"
    "strconv"
    "sync"
    "time"

    "github.com/schollz/progressbar/v3"
)

type Node struct {
    ID       string    `json:"id"`
    MacID    string    `json:"mac_id"`
    IP       string    `json:"ip"`
    Port     int       `json:"port"`
    Capacity int       `json:"capacity"` // in MB
    Used     int       `json:"used"`     // in MB
    Status   string    `json:"status"`
    LastSeen time.Time `json:"last_seen"`
}

type ChunkInfo struct {
    ChunkID  string `json:"chunk_id"`
    Size     int    `json:"size"` // in KB
    Status   string `json:"status"`
    FileName string `json:"file_name"`
    Location string `json:"location"`
    IDRange  struct {
        Start int `json:"start"`
        End   int `json:"end"`
    } `json:"id_range"`
}

var (
    nodes          = make(map[string]Node)
    lastHeartbeat  = make(map[string]int64)
    chunks         = make(map[string]ChunkInfo)
    dataIndex      = make(map[string][]string)
    totalFilesSize int64
    mu             sync.RWMutex

    backupFolder     = "aws_backup"
    nodesFile        = "nodes.json"
    masterIndexFile  = "master_index.json"
    heartbeatTimeout = 15 // in seconds
)

func main() {
    fmt.Println("[INFO] AWS Coordinator starting...")

    // Create backup folder if not exists
    if _, err := os.Stat(backupFolder); os.IsNotExist(err) {
        os.Mkdir(backupFolder, 0755)
    }

    // Load existing nodes
    if err := loadNodesFromFile(); err != nil {
        fmt.Println("[INFO] No saved nodes found, starting fresh.")
    }

    // Load existing master index
    if err := loadMasterIndex(); err != nil {
        fmt.Println("[INFO] No saved master index found, starting fresh.")
    }

    // Start node monitor
    go monitorNodes()

    // Set up HTTP handlers
    http.HandleFunc("/register", registerNode)
    http.HandleFunc("/heartbeat", handleHeartbeat)
    http.HandleFunc("/upload", uploadFiles)
    http.HandleFunc("/distribute", distributeData)
    http.HandleFunc("/query", handleQuery)
    http.HandleFunc("/metrics", getMetrics)

    fmt.Println("[INFO] AWS Coordinator running on port 8080...")
    if err := http.ListenAndServe(":8080", nil); err != nil {
        fmt.Printf("[ERROR] Failed to start server: %v\n", err)
    }
}

func loadNodesFromFile() error {
    data, err := ioutil.ReadFile(nodesFile)
    if err != nil {
        return err
    }

    var savedNodes map[string]Node
    err = json.Unmarshal(data, &savedNodes)
    if err != nil {
        return err
    }

    mu.Lock()
    nodes = savedNodes
    mu.Unlock()

    return nil
}

func saveNodesToFile() {
    mu.RLock()
    data, err := json.MarshalIndent(nodes, "", "  ")
    mu.RUnlock()
    if err != nil {
        fmt.Printf("[ERROR] Failed to marshal nodes data: %v\n", err)
        return
    }

    err = ioutil.WriteFile(nodesFile, data, 0644)
    if err != nil {
        fmt.Printf("[ERROR] Failed to write nodes data to file: %v\n", err)
    }
}

func loadMasterIndex() error {
    data, err := ioutil.ReadFile(masterIndexFile)
    if err != nil {
        return err
    }

    var savedChunks map[string]ChunkInfo
    err = json.Unmarshal(data, &savedChunks)
    if err != nil {
        return err
    }

    mu.Lock()
    chunks = savedChunks
    mu.Unlock()

    return nil
}

func saveMasterIndex() {
    mu.RLock()
    data, err := json.MarshalIndent(chunks, "", "  ")
    mu.RUnlock()
    if err != nil {
        fmt.Printf("[ERROR] Failed to marshal master index data: %v\n", err)
        return
    }

    err = ioutil.WriteFile(masterIndexFile, data, 0644)
    if err != nil {
        fmt.Printf("[ERROR] Failed to write master index to file: %v\n", err)
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

    // Extract the public IP from the request
    ip, _, err := net.SplitHostPort(r.RemoteAddr)
    if err == nil {
        node.IP = ip
    }

    mu.Lock()
    defer mu.Unlock()

    node.Status = "active"
    node.LastSeen = time.Now()
    nodes[node.MacID] = node
    lastHeartbeat[node.MacID] = time.Now().Unix()
    saveNodesToFile()

    fmt.Printf("[INFO] New node registered: %s (IP: %s, Port: %d, Capacity: %dMB)\n",
        node.ID, node.IP, node.Port, node.Capacity)
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

    // Extract the public IP from the request
    ip, _, err := net.SplitHostPort(r.RemoteAddr)
    if err == nil {
        node.IP = ip
    }

    mu.Lock()
    defer mu.Unlock()

    if existingNode, exists := nodes[node.MacID]; exists {
        existingNode.LastSeen = time.Now()
        existingNode.Status = "active"
        existingNode.IP = node.IP
        existingNode.Port = node.Port
        existingNode.Used = node.Used
        nodes[node.MacID] = existingNode
        lastHeartbeat[node.MacID] = time.Now().Unix()
        saveNodesToFile()
    } else {
        // If node is not registered, register it
        node.Status = "active"
        node.LastSeen = time.Now()
        nodes[node.MacID] = node
        lastHeartbeat[node.MacID] = time.Now().Unix()
        saveNodesToFile()
        fmt.Printf("[INFO] New node registered via heartbeat: %s (IP: %s, Port: %d, Capacity: %dMB)\n",
            node.ID, node.IP, node.Port, node.Capacity)
    }
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

    mu.Lock()
    totalFilesSize += int64(len(body))
    mu.Unlock()

    fmt.Printf("[INFO] File '%s' received. Total files received size: %d bytes\n", fileName, totalFilesSize)
    w.WriteHeader(http.StatusOK)
}

func distributeData(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
        return
    }

    fmt.Println("[INFO] Starting data distribution...")
    err := processAndDistributeFiles()
    if err != nil {
        http.Error(w, "Failed to distribute dataset: "+err.Error(), http.StatusInternalServerError)
        fmt.Printf("[ERROR] Failed to distribute dataset: %v\n", err)
        return
    }

    fmt.Println("[INFO] Dataset distribution completed.")
    fmt.Printf("[INFO] Total chunks distributed: %d\n", len(chunks))
    fmt.Println("[INFO] Distribution details:")
    mu.RLock()
    for nodeID, chunkIDs := range dataIndex {
        fmt.Printf("Node '%s' received %d chunks.\n", nodeID, len(chunkIDs))
    }
    mu.RUnlock()

    saveMasterIndex()
    fmt.Fprintf(w, "Dataset distribution completed.")
}

func processAndDistributeFiles() error {
    mu.Lock()
    defer mu.Unlock()

    // Clear previous data
    chunks = make(map[string]ChunkInfo)
    dataIndex = make(map[string][]string)

    files, err := ioutil.ReadDir(backupFolder)
    if err != nil {
        return fmt.Errorf("failed to read backup folder: %s", err)
    }

    // Read all records from files
    var allRecords []map[string]interface{}
    for _, file := range files {
        data, err := ioutil.ReadFile(backupFolder + "/" + file.Name())
        if err != nil {
            return fmt.Errorf("failed to read file '%s': %s", file.Name(), err)
        }

        var records []map[string]interface{}
        err = json.Unmarshal(data, &records)
        if err != nil {
            return fmt.Errorf("failed to parse JSON in file '%s': %s", file.Name(), err)
        }

        allRecords = append(allRecords, records...)
    }

    // Sort records by ID
    sort.Slice(allRecords, func(i, j int) bool {
        return parseInt(fmt.Sprintf("%v", allRecords[i]["id"])) < parseInt(fmt.Sprintf("%v", allRecords[j]["id"]))
    })

    fmt.Printf("[INFO] Total records to process: %d\n", len(allRecords))

    // Chunk records into ~8.5KB chunks
    var chunkSizeLimit = 8.5 * 1024 // 8.5KB
    var currentChunk []map[string]interface{}
    var currentSize float64
    var chunkIDCounter int

    for _, record := range allRecords {
        recordData, err := json.Marshal(record)
        if err != nil {
            return fmt.Errorf("failed to marshal record: %s", err)
        }

        recordSize := float64(len(recordData))
        if currentSize+recordSize > chunkSizeLimit && len(currentChunk) > 0 {
            // Save current chunk
            chunkID := fmt.Sprintf("chunk_%d.json", chunkIDCounter)
            chunkPath := backupFolder + "/" + chunkID
            chunkData, _ := json.Marshal(currentChunk)
            err = ioutil.WriteFile(chunkPath, chunkData, 0644)
            if err != nil {
                return fmt.Errorf("failed to write chunk '%s': %s", chunkID, err)
            }

            // Update chunk info
            startID := parseInt(fmt.Sprintf("%v", currentChunk[0]["id"]))
            endID := parseInt(fmt.Sprintf("%v", currentChunk[len(currentChunk)-1]["id"]))
            chunkInfo := ChunkInfo{
                ChunkID:  chunkID,
                Size:     int(currentSize / 1024), // in KB
                Status:   "available",
                FileName: chunkID,
            }
            chunkInfo.IDRange.Start = startID
            chunkInfo.IDRange.End = endID
            chunks[chunkID] = chunkInfo

            fmt.Printf("[INFO] Created chunk '%s' with ID range %d-%d, size: %d KB\n",
                chunkID, startID, endID, chunkInfo.Size)

            // Reset for next chunk
            currentChunk = []map[string]interface{}{record}
            currentSize = recordSize
            chunkIDCounter++
        } else {
            currentChunk = append(currentChunk, record)
            currentSize += recordSize
        }
    }

    // Save any remaining chunk
    if len(currentChunk) > 0 {
        chunkID := fmt.Sprintf("chunk_%d.json", chunkIDCounter)
        chunkPath := backupFolder + "/" + chunkID
        chunkData, _ := json.Marshal(currentChunk)
        err = ioutil.WriteFile(chunkPath, chunkData, 0644)
        if err != nil {
            return fmt.Errorf("failed to write chunk '%s': %s", chunkID, err)
        }

        // Update chunk info
        startID := parseInt(fmt.Sprintf("%v", currentChunk[0]["id"]))
        endID := parseInt(fmt.Sprintf("%v", currentChunk[len(currentChunk)-1]["id"]))
        chunkInfo := ChunkInfo{
            ChunkID:  chunkID,
            Size:     int(currentSize / 1024), // in KB
            Status:   "available",
            FileName: chunkID,
        }
        chunkInfo.IDRange.Start = startID
        chunkInfo.IDRange.End = endID
        chunks[chunkID] = chunkInfo

        fmt.Printf("[INFO] Created chunk '%s' with ID range %d-%d, size: %d KB\n",
            chunkID, startID, endID, chunkInfo.Size)
    }

    // Now distribute chunks among active nodes
    return distributeChunks()
}

func distributeChunks() error {
    // Get active nodes
    activeNodes := []Node{}
    mu.RLock()
    for _, node := range nodes {
        if node.Status == "active" {
            activeNodes = append(activeNodes, node)
        }
    }
    mu.RUnlock()

    if len(activeNodes) == 0 {
        return fmt.Errorf("no active nodes available")
    }

    // Prepare list of chunks
    chunkList := make([]ChunkInfo, 0, len(chunks))
    for _, chunk := range chunks {
        if chunk.Status == "available" {
            chunkList = append(chunkList, chunk)
        }
    }

    totalChunks := len(chunkList)
    bar := progressbar.Default(int64(totalChunks))

    // Distribute chunks in a round-robin fashion
    chunksAssigned := 0
    nodeIndex := 0
    for chunksAssigned < totalChunks {
        node := activeNodes[nodeIndex%len(activeNodes)]
        chunk := chunkList[chunksAssigned]

        err := sendChunkToNode(node, chunk)
        if err != nil {
            fmt.Printf("[ERROR] Failed to send chunk '%s' to node '%s': %v\n",
                chunk.ChunkID, node.ID, err)
            // Optionally, mark the node as inactive or skip to next node
            nodeIndex++
            continue
        }

        // Update chunk location
        mu.Lock()
        chunk.Location = node.ID
        chunk.Status = "assigned"
        chunks[chunk.ChunkID] = chunk
        mu.Unlock()

        // Update dataIndex
        mu.Lock()
        dataIndex[node.ID] = append(dataIndex[node.ID], chunk.ChunkID)
        mu.Unlock()

        bar.Add(1)
        chunksAssigned++
        nodeIndex++
    }

    fmt.Printf("[INFO] Distributed %d chunks among %d nodes\n", chunksAssigned, len(activeNodes))
    return nil
}

func sendChunkToNode(node Node, chunk ChunkInfo) error {
    data, err := ioutil.ReadFile(backupFolder + "/" + chunk.FileName)
    if err != nil {
        return fmt.Errorf("failed to read chunk '%s': %s", chunk.FileName, err)
    }

    url := fmt.Sprintf("http://%s:%d/receive", node.IP, node.Port)
    req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
    if err != nil {
        return err
    }

    req.Header.Set("File-Name", chunk.FileName)
    client := &http.Client{Timeout: 30 * time.Second}

    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("node returned status code %d", resp.StatusCode)
    }

    // Update node's used space
    mu.Lock()
    node.Used += len(data) / (1024 * 1024) // Convert bytes to MB
    nodes[node.MacID] = node
    mu.Unlock()

    return nil
}

func monitorNodes() {
    for {
        time.Sleep(10 * time.Second)
        mu.Lock()
        now := time.Now().Unix()
        statusChanged := false

        for mac, last := range lastHeartbeat {
            if now-last > int64(heartbeatTimeout) {
                node := nodes[mac]
                if node.Status == "active" {
                    node.Status = "inactive"
                    nodes[mac] = node
                    statusChanged = true
                    fmt.Printf("[WARNING] Node '%s' became inactive\n", node.ID)
                }
            }
        }

        if statusChanged {
            fmt.Println("[INFO] Initiating redistribution due to node inactivity.")
            triggerRedistribution()
        }
        mu.Unlock()
    }
}

func triggerRedistribution() {
    fmt.Println("[INFO] Triggering redistribution of data chunks.")
    err := redistributeChunks()
    if err != nil {
        fmt.Printf("[ERROR] Redistribution failed: %s\n", err)
    }
}

func redistributeChunks() error {
    mu.Lock()
    defer mu.Unlock()

    // Identify chunks from inactive nodes
    inactiveChunks := []ChunkInfo{}
    for _, chunk := range chunks {
        nodeID := chunk.Location
        if node, exists := getNodeByID(nodeID); exists {
            if node.Status != "active" {
                inactiveChunks = append(inactiveChunks, chunk)
                chunk.Status = "available"
                chunks[chunk.ChunkID] = chunk
            }
        } else {
            // Node not found, mark chunk as available
            inactiveChunks = append(inactiveChunks, chunk)
            chunk.Status = "available"
            chunks[chunk.ChunkID] = chunk
        }
    }

    // Remove chunks from dataIndex
    for _, chunk := range inactiveChunks {
        dataIndex[chunk.Location] = removeFromSlice(dataIndex[chunk.Location], chunk.ChunkID)
    }

    // Distribute available chunks
    return distributeChunks()
}

func removeFromSlice(slice []string, s string) []string {
    for i, v := range slice {
        if v == s {
            return append(slice[:i], slice[i+1:]...)
        }
    }
    return slice
}

func getNodeByID(id string) (Node, bool) {
    for _, node := range nodes {
        if node.ID == id {
            return node, true
        }
    }
    return Node{}, false
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
        startID := chunk.IDRange.Start
        endID := chunk.IDRange.End
        qID := parseInt(queryID)
        if startID <= qID && qID <= endID {
            // Get the node where this chunk is located
            nodeID := chunk.Location
            if node, exists := getNodeByID(nodeID); exists && node.Status == "active" {
                targetNode = node
                chunkID = id
                found = true
                break
            }
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
        fmt.Printf("[ERROR] Failed to retrieve data from node '%s': %v\n", targetNode.ID, err)
        return
    }
    defer resp.Body.Close()

    // Copy response to client
    data, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        http.Error(w, "Failed to read response from node", http.StatusInternalServerError)
        fmt.Printf("[ERROR] Failed to read response from node '%s': %v\n", targetNode.ID, err)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.Write(data)
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
        return
    }

    mu.RLock()
    defer mu.RUnlock()

    metrics := struct {
        TotalDataSize int             `json:"total_data_size"`
        TotalChunks   int             `json:"total_chunks"`
        ActiveNodes   int             `json:"active_nodes"`
        Nodes         map[string]Node `json:"nodes"`
    }{
        TotalDataSize: int(totalFilesSize / (1024 * 1024)), // Convert bytes to MB
        TotalChunks:   len(chunks),
        Nodes:         nodes,
    }

    // Count active nodes
    for _, node := range nodes {
        if node.Status == "active" {
            metrics.ActiveNodes++
        }
    }

    data, err := json.MarshalIndent(metrics, "", "  ")
    if err != nil {
        http.Error(w, "Failed to generate metrics", http.StatusInternalServerError)
        fmt.Printf("[ERROR] Failed to generate metrics: %v\n", err)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.Write(data)
}

func parseInt(s string) int {
    val, _ := strconv.Atoi(s)
    return val
}
