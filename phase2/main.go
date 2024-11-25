package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "os"
    "sync"
    "time"
    "phase2/types"
    "phase2/config"
)

var (
    nodes         = make(map[string]types.Node)       // Node registry
    lastHeartbeat = make(map[string]int64)           // Last heartbeat time for each node
    dataIndex     = make(map[string][]string)        // Tracks which node has which files
    mu            sync.RWMutex                       // Mutex for thread-safe access
    backupFolder  = config.BACKUP_FOLDER             // Backup folder for datasets
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

    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Failed to read data", http.StatusInternalServerError)
        return
    }

    err = ioutil.WriteFile(fmt.Sprintf("%s/%s", backupFolder, fileName), body, 0644)
    if err != nil {
        http.Error(w, "Failed to save file", http.StatusInternalServerError)
        return
    }

    fmt.Printf("[INFO] File received: %s\n", fileName)
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
        TotalNodes: len(nodes),
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
    // Implementation needed
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
