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
    "strconv"
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
    ChunkID  string `json:"chunk_id"`
    IDRange  struct {
        Start int `json:"start"`
        End   int `json:"end"`
    } `json:"id_range"`
    Size     int    `json:"size"` // in KB
    FilePath string `json:"file_path"`
}

var (
    localFolder = "received_files"
    awsURL      = "http://18.144.165.108:8080" // Replace <AWS_PUBLIC_IP> with your AWS public IP
    node        Node
    localIndex  = LocalIndex{
        Chunks: make(map[string]ChunkInfo),
    }
    mu sync.RWMutex
)

func main() {
    // Create necessary directories
    os.MkdirAll(localFolder, 0755)

    // Initialize node
    initializeNode()

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

    fmt.Printf("[INFO] Laptop node '%s' started. Listening on port %d...\n", node.ID, node.Port)
    err := http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil)
    if err != nil {
        fmt.Printf("[ERROR] Failed to start server: %v\n", err)
        os.Exit(1)
    }
}

func initializeNode() {
    // Generate unique ID
    nodeID := fmt.Sprintf("laptop-%d", time.Now().Unix()%1000)

    // Get IP
    ip := getLocalIP()

    // Use port 8081
    port := 8081

    // Set capacity
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
}

func getMacAddress() string {
    interfaces, err := net.Interfaces()
    if err != nil {
        return fmt.Sprintf("unknown-%d", time.Now().Unix())
    }

    for _, i := range interfaces {
        if i.Flags&net.FlagUp != 0 && !strings.HasPrefix(i.Name, "lo") && len(i.HardwareAddr.String()) > 0 {
            return i.HardwareAddr.String()
        }
    }

    return fmt.Sprintf("unknown-%d", time.Now().Unix())
}

func getLocalIP() string {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        return "localhost"
    }

    for _, addr := range addrs {
        if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
            if ipNet.IP.To4() != nil {
                return ipNet.IP.String()
            }
        }
    }
    return "localhost"
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
        fmt.Printf("[INFO] Updated local metrics. Total chunks stored: %d, Total size used: %d KB\n",
            len(localIndex.Chunks), localIndex.TotalSize)

        time.Sleep(10 * time.Second)
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
        fmt.Printf("[INFO] Successfully registered with AWS (Node ID: %s)\n", node.ID)
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
            fmt.Printf("[ERROR] Failed to send heartbeat: %v\n", err)
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

    // Parse chunk to get IDRange and Size
    var records []map[string]interface{}
    err = json.Unmarshal(body, &records)
    if err != nil {
        http.Error(w, "Failed to parse chunk data", http.StatusInternalServerError)
        return
    }

    startID := parseInt(fmt.Sprintf("%v", records[0]["id"]))
    endID := parseInt(fmt.Sprintf("%v", records[len(records)-1]["id"]))
    sizeKB := len(body) / 1024

    // Update local index
    mu.Lock()
    chunkInfo := ChunkInfo{
        ChunkID:  fileName,
        Size:     sizeKB,
        FilePath: filePath,
    }
    chunkInfo.IDRange.Start = startID
    chunkInfo.IDRange.End = endID
    localIndex.Chunks[fileName] = chunkInfo
    mu.Unlock()

    saveLocalIndex()
    fmt.Printf("[INFO] Received chunk '%s' of size %d KB\n", fileName, sizeKB)
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
        if parseInt(fmt.Sprintf("%v", record["id"])) == targetID {
            w.Header().Set("Content-Type", "application/json")
            json.NewEncoder(w).Encode(record)
            fmt.Printf("[INFO] Record with ID '%s' found and returned.\n", queryID)
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
    fmt.Printf("\nWelcome to the Distributed Memory Allocator Node Interface\n")
    fmt.Printf("Type 'help' to see available commands.\n")

    for {
        fmt.Print("\n> ")
        command, _ := reader.ReadString('\n')
        command = strings.TrimSpace(command)

        switch strings.ToLower(command) {
        case "distribute":
            handleDistribute()
        case "stats":
            showStatus()
        case "list":
            listChunks()
        case "query":
            handleLocalQuery()
        case "exit":
            fmt.Print("Are you sure you want to exit? (yes/no): ")
            confirmation, _ := reader.ReadString('\n')
            if strings.TrimSpace(strings.ToLower(confirmation)) == "yes" {
                fmt.Println("[INFO] Shutting down...")
                os.Exit(0)
            }
        case "help":
            showHelp()
        default:
            fmt.Printf("[WARNING] Unknown command: '%s'\n", command)
            showHelp()
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

    fmt.Printf("[INFO] Uploading %d files to AWS Coordinator...\n", len(files))
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

    fmt.Println("[INFO] Data distribution initiated successfully.")
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
    fmt.Printf("IP Address: %s\n", node.IP)
    fmt.Printf("Port: %d\n", node.Port)
    fmt.Printf("Status: %s\n", node.Status)
    fmt.Printf("Capacity: %d MB\n", node.Capacity)
    fmt.Printf("Used Storage: %d MB\n", node.Used)
    fmt.Printf("Available Storage: %d MB\n", node.Capacity-node.Used)
    fmt.Printf("Chunks Stored: %d\n", len(localIndex.Chunks))
    fmt.Printf("Last Updated: %s\n", localIndex.LastUpdated.Format(time.RFC1123))
}

func listChunks() {
    mu.RLock()
    defer mu.RUnlock()

    fmt.Printf("\n=== Stored Chunks ===\n")
    if len(localIndex.Chunks) == 0 {
        fmt.Println("No chunks stored.")
        return
    }

    for _, chunk := range localIndex.Chunks {
        fmt.Printf("Chunk ID: %s\n", chunk.ChunkID)
        fmt.Printf("ID Range: %d - %d\n", chunk.IDRange.Start, chunk.IDRange.End)
        fmt.Printf("Size: %d KB\n", chunk.Size)
        fmt.Printf("File Path: %s\n", chunk.FilePath)
        fmt.Println()
    }
    fmt.Printf("Total Chunks Stored: %d\n", len(localIndex.Chunks))
}

func handleLocalQuery() {
    fmt.Print("Enter the record ID to query: ")
    reader := bufio.NewReader(os.Stdin)
    queryIDStr, _ := reader.ReadString('\n')
    queryIDStr = strings.TrimSpace(queryIDStr)
    queryID := parseInt(queryIDStr)

    if queryID == 0 {
        fmt.Println("[ERROR] Invalid record ID.")
        return
    }

    fmt.Printf("Fetching record with ID %d...\n", queryID)

    // Send query to AWS Coordinator
    url := fmt.Sprintf("%s/query?id=%d", awsURL, queryID)
    resp, err := http.Get(url)
    if err != nil {
        fmt.Printf("[ERROR] Failed to fetch record: %v\n", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := ioutil.ReadAll(resp.Body)
        fmt.Printf("[ERROR] Failed to fetch record: %s\n", string(body))
        return
    }

    data, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Printf("[ERROR] Failed to read response: %v\n", err)
        return
    }

    fmt.Println("Record found:")
    fmt.Println(string(data))
}

func showHelp() {
    fmt.Println("\nAvailable commands:")
    fmt.Println("- stats      : Display node status and metrics.")
    fmt.Println("- distribute : Upload dataset and initiate distribution.")
    fmt.Println("- query      : Query a specific record by ID.")
    fmt.Println("- list       : List stored chunks and their details.")
    fmt.Println("- exit       : Exit the program.")
    fmt.Println("- help       : Show this help message.")
}

func parseInt(s string) int {
    val, _ := strconv.Atoi(s)
    return val
}
