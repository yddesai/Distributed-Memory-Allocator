// laptop.go
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
    "strconv"
    "strings"
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
    IDRange  struct {
        Start int `json:"start"`
        End   int `json:"end"`
    } `json:"id_range"`
}

var (
    node          Node
    awsIP         = "18.144.165.108" // Replace with your AWS Coordinator's public IP
    awsPort       = 8080
    indexFile     = "index.json"
    dataFolder    = "data"
    metricsFile   = "metrics.json"
    mu            sync.Mutex
    storedChunks  = make(map[string]ChunkInfo)
    totalUsedSize int // in KB
)

func main() {
    initializeNode()

    // Create data folder if not exists
    if _, err := os.Stat(dataFolder); os.IsNotExist(err) {
        os.Mkdir(dataFolder, 0755)
    }

    // Load stored chunks from index file
    loadIndex()

    // Start heartbeat
    go sendHeartbeat()

    // Start HTTP server
    http.HandleFunc("/receive", receiveChunk)
    http.HandleFunc("/query", handleQuery)
    http.HandleFunc("/metrics", getMetrics)

    fmt.Printf("[INFO] Laptop node '%s' started. Listening on port %d...\n", node.ID, node.Port)

    // Start CLI
    go startCLI()

    if err := http.ListenAndServe(":"+strconv.Itoa(node.Port), nil); err != nil {
        fmt.Printf("[ERROR] Failed to start server: %v\n", err)
    }
}

func initializeNode() {
    macID := getMacAddr()
    macIDClean := strings.ReplaceAll(macID, ":", "") // Remove colons

    // Get public IP address
    publicIP := getPublicIP()
    if publicIP == "" {
        fmt.Println("[ERROR] Could not retrieve public IP address.")
        os.Exit(1)
    }

    node = Node{
        ID:       "laptop-" + macIDClean[len(macIDClean)-4:], // Use the last 4 characters
        MacID:    macID,
        IP:       publicIP,
        Port:     8081,
        Capacity: 200, // 200MB capacity
    }

    fmt.Printf("[INFO] Initialized node: %s (MAC: %s, IP: %s, Port: %d, Capacity: %dMB)\n",
        node.ID, node.MacID, node.IP, node.Port, node.Capacity)

    registerWithAWS()
}

func getPublicIP() string {
    resp, err := http.Get("https://api.ipify.org?format=text")
    if err != nil {
        fmt.Printf("[ERROR] Failed to get public IP address: %v\n", err)
        return ""
    }
    defer resp.Body.Close()
    ip, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Printf("[ERROR] Failed to read public IP response: %v\n", err)
        return ""
    }
    return strings.TrimSpace(string(ip))
}

func registerWithAWS() {
    url := fmt.Sprintf("http://%s:%d/register", awsIP, awsPort)
    data, _ := json.Marshal(node)
    client := &http.Client{Timeout: 10 * time.Second} // Add a timeout
    resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
    if err != nil {
        fmt.Printf("[ERROR] Failed to register with AWS Coordinator: %v\n", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusOK {
        fmt.Printf("[INFO] Successfully registered with AWS (Node ID: %s)\n", node.ID)
    } else {
        fmt.Printf("[ERROR] Failed to register with AWS Coordinator. Status code: %d\n", resp.StatusCode)
    }
}

func sendHeartbeat() {
    for {
        time.Sleep(5 * time.Second)
        url := fmt.Sprintf("http://%s:%d/heartbeat", awsIP, awsPort)
        mu.Lock()
        node.Used = totalUsedSize / 1024 // Convert KB to MB
        data, _ := json.Marshal(node)
        mu.Unlock()

        client := &http.Client{Timeout: 5 * time.Second}
        resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
        if err != nil {
            fmt.Printf("[ERROR] Failed to send heartbeat: %v\n", err)
            continue
        }
        resp.Body.Close()
    }
}

func receiveChunk(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
        return
    }

    fileName := r.Header.Get("File-Name")
    if fileName == "" {
        http.Error(w, "File name header missing", http.StatusBadRequest)
        return
    }

    data, err := ioutil.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Failed to read chunk data", http.StatusInternalServerError)
        return
    }

    filePath := dataFolder + "/" + fileName
    err = ioutil.WriteFile(filePath, data, 0644)
    if err != nil {
        http.Error(w, "Failed to save chunk: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Update stored chunks
    var records []map[string]interface{}
    err = json.Unmarshal(data, &records)
    if err != nil {
        // Handle error if necessary
    }

    chunkInfo := ChunkInfo{
        ChunkID:  fileName,
        Size:     len(data) / 1024, // Convert bytes to KB
        Status:   "stored",
        FileName: fileName,
    }
    if len(records) > 0 {
        startID := parseInt(fmt.Sprintf("%v", records[0]["id"]))
        endID := parseInt(fmt.Sprintf("%v", records[len(records)-1]["id"]))
        chunkInfo.IDRange.Start = startID
        chunkInfo.IDRange.End = endID
    }

    mu.Lock()
    storedChunks[fileName] = chunkInfo
    totalUsedSize += len(data) / 1024 // Convert bytes to KB
    saveIndex()
    mu.Unlock()

    fmt.Printf("[INFO] Received chunk '%s'. Total stored chunks: %d\n", fileName, len(storedChunks))
    w.WriteHeader(http.StatusOK)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
        return
    }

    chunkID := r.URL.Query().Get("chunk")
    queryID := r.URL.Query().Get("id")
    if chunkID == "" || queryID == "" {
        http.Error(w, "Missing chunk or id parameter", http.StatusBadRequest)
        return
    }

    filePath := dataFolder + "/" + chunkID
    data, err := ioutil.ReadFile(filePath)
    if err != nil {
        http.Error(w, "Chunk not found", http.StatusNotFound)
        return
    }

    var records []map[string]interface{}
    err = json.Unmarshal(data, &records)
    if err != nil {
        http.Error(w, "Failed to parse chunk data", http.StatusInternalServerError)
        return
    }

    // Search for the record
    qID := parseInt(queryID)
    for _, record := range records {
        id := parseInt(fmt.Sprintf("%v", record["id"]))
        if id == qID {
            result, _ := json.Marshal(record)
            w.Header().Set("Content-Type", "application/json")
            w.Write(result)
            return
        }
    }

    http.Error(w, "Record not found in chunk", http.StatusNotFound)
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
        return
    }

    mu.Lock()
    defer mu.Unlock()

    metrics := struct {
        TotalChunks int `json:"total_chunks"`
        TotalUsed   int `json:"total_used"` // in KB
    }{
        TotalChunks: len(storedChunks),
        TotalUsed:   totalUsedSize,
    }

    data, err := json.MarshalIndent(metrics, "", "  ")
    if err != nil {
        http.Error(w, "Failed to generate metrics", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.Write(data)
}

func loadIndex() {
    data, err := ioutil.ReadFile(indexFile)
    if err != nil {
        return
    }

    var index map[string]ChunkInfo
    err = json.Unmarshal(data, &index)
    if err != nil {
        return
    }

    mu.Lock()
    storedChunks = index
    mu.Unlock()
}

func saveIndex() {
    data, _ := json.MarshalIndent(storedChunks, "", "  ")
    ioutil.WriteFile(indexFile, data, 0644)
}

func startCLI() {
    fmt.Println("\nWelcome to the Distributed Memory Allocator Node Interface")
    fmt.Println("Type 'help' to see available commands.\n")

    reader := bufio.NewReader(os.Stdin)

    for {
        fmt.Print("> ")
        input, _ := reader.ReadString('\n')
        input = strings.TrimSpace(input)

        switch strings.ToLower(input) {
        case "help":
            fmt.Println("Available commands:")
            fmt.Println("  distribute - Upload dataset and initiate distribution")
            fmt.Println("  query      - Query a record by ID")
            fmt.Println("  metrics    - Show local metrics")
            fmt.Println("  exit       - Exit the application")
        case "distribute":
            handleDistribute()
        case "query":
            handleQueryCLI()
        case "metrics":
            showMetrics()
        case "exit":
            fmt.Println("Exiting...")
            os.Exit(0)
        default:
            fmt.Println("Unknown command. Type 'help' to see available commands.")
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
        fmt.Printf("[ERROR] Failed to read directory: %v\n", err)
        return
    }

    fmt.Printf("[INFO] Uploading %d files to AWS Coordinator...\n", len(files))
    bar := progressbar.Default(int64(len(files)))
    for _, file := range files {
        data, err := ioutil.ReadFile(path + "/" + file.Name())
        if err != nil {
            fmt.Printf("[ERROR] Failed to read file '%s': %v\n", file.Name(), err)
            continue
        }

        url := fmt.Sprintf("http://%s:%d/upload", awsIP, awsPort)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
        if err != nil {
            fmt.Printf("[ERROR] Failed to create request for file '%s': %v\n", file.Name(), err)
            continue
        }

        req.Header.Set("File-Name", file.Name())
        client := &http.Client{Timeout: 30 * time.Second}

        resp, err := client.Do(req)
        if err != nil {
            fmt.Printf("[ERROR] Failed to upload file '%s': %v\n", file.Name(), err)
            continue
        }
        resp.Body.Close()
        bar.Add(1)
    }

    // Initiate distribution
    url := fmt.Sprintf("http://%s:%d/distribute", awsIP, awsPort)
    client := &http.Client{Timeout: 30 * time.Second}
    resp, err := client.Post(url, "application/json", nil)
    if err != nil {
        fmt.Printf("[ERROR] Failed to initiate distribution: %v\n", err)
        return
    }
    resp.Body.Close()
    fmt.Println("\n[INFO] Distribution initiated.")
}

func handleQueryCLI() {
    fmt.Print("Enter the ID to query: ")
    reader := bufio.NewReader(os.Stdin)
    id, _ := reader.ReadString('\n')
    id = strings.TrimSpace(id)

    url := fmt.Sprintf("http://%s:%d/query?id=%s", awsIP, awsPort, id)
    client := &http.Client{Timeout: 10 * time.Second}
    resp, err := client.Get(url)
    if err != nil {
        fmt.Printf("[ERROR] Failed to query AWS Coordinator: %v\n", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        fmt.Printf("[ERROR] Record not found. Status code: %d\n", resp.StatusCode)
        return
    }

    data, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Printf("[ERROR] Failed to read response: %v\n", err)
        return
    }

    fmt.Printf("[INFO] Query Result: %s\n", string(data))
}

func showMetrics() {
    mu.Lock()
    fmt.Printf("[INFO] Total chunks stored: %d, Total size used: %d KB\n", len(storedChunks), totalUsedSize)
    mu.Unlock()
}

func getMacAddr() string {
    interfaces, err := net.Interfaces()
    if err != nil {
        return ""
    }
    for _, i := range interfaces {
        if i.Flags&net.FlagUp != 0 && !strings.HasPrefix(i.Name, "lo") {
            mac := i.HardwareAddr.String()
            if mac != "" {
                return mac
            }
        }
    }
    return ""
}

func parseInt(s string) int {
    val, _ := strconv.Atoi(s)
    return val
}
