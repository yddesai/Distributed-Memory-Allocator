package main

import (
        "bytes"
        "encoding/json"
        "fmt"
        "io/ioutil"
        "net/http"
        "os"
        "sync"
        "time"
        "strings"

        "github.com/schollz/progressbar/v3"
)

// Node structure
type Node struct {
        ID       string    `json:"id"`
        Capacity int       `json:"capacity"`
        MacID    string    `json:"mac_id"`
        IP       string    `json:"ip"`
        Port     int       `json:"port"`
        Status   string    `json:"status"`
        LastSeen time.Time `json:"last_seen"`
}

var (
        nodes         = make(map[string]Node)       // Node registry
        lastHeartbeat = make(map[string]int64)      // Last heartbeat time for each node
        dataIndex     = make(map[string][]string)   // Tracks which node has which files
        mu            sync.Mutex                    // Mutex for thread-safe access
        backupFolder  = "aws_backup"                // Backup folder for datasets
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

        go monitorNodes()

        fmt.Println("[INFO] AWS Coordinator running on port 8080...")
        http.ListenAndServe(":8080", nil)
}

func printConnectedDevices() {
        mu.Lock()
        defer mu.Unlock()
        
        fmt.Println("\n=== Connected Devices ===")
        fmt.Printf("%-15s %-25s %-15s %-10s\n", "Device ID", "IP:Port", "Status", "Last Seen")
        fmt.Println(strings.Repeat("-", 70))
        
        for _, node := range nodes {
                lastSeenStr := node.LastSeen.Format("15:04:05")
                ipPort := fmt.Sprintf("%s:%d", node.IP, node.Port)
                fmt.Printf("%-15s %-25s %-15s %-10s\n", 
                        node.ID, 
                        ipPort,
                        node.Status,
                        lastSeenStr)
        }
        fmt.Println(strings.Repeat("-", 70))
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
        // Check for IP:Port conflict
        for _, existingNode := range nodes {
                if existingNode.IP == node.IP && existingNode.Port == node.Port && existingNode.Status == "active" {
                        mu.Unlock()
                        http.Error(w, fmt.Sprintf("IP:Port combination %s:%d is already in use", node.IP, node.Port), http.StatusConflict)
                        return
                }
        }

        node.Status = "active"
        node.LastSeen = time.Now()
        nodes[node.MacID] = node
        lastHeartbeat[node.MacID] = time.Now().Unix()
        saveNodesToFile()
        mu.Unlock()

        fmt.Printf("\n[INFO] New device connected: %s (IP:%s Port:%d)\n", 
                node.ID, node.IP, node.Port)
        printConnectedDevices()
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
                nodes[node.MacID] = existingNode
                lastHeartbeat[node.MacID] = time.Now().Unix()
        }
        mu.Unlock()
}

func listNodes(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
                http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
                return
        }

        mu.Lock()
        defer mu.Unlock()
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
                        printConnectedDevices()
                        triggerRedistribution()
                }
                mu.Unlock()
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

        fmt.Printf("[INFO] File '%s' received and stored in backup.\n", fileName)
        w.WriteHeader(http.StatusOK)
        fmt.Fprintf(w, "File '%s' received and saved.", fileName)
}

func distributeData(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
                http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
                return
        }

        fmt.Println("[INFO] Starting data distribution...")
        printConnectedDevices()

        err := distributeFiles()
        if err != nil {
                http.Error(w, "Failed to distribute dataset: "+err.Error(), http.StatusInternalServerError)
                return
        }

        fmt.Println("[INFO] Dataset distribution completed.")
        printConnectedDevices()
        fmt.Fprintf(w, "Dataset distribution completed.")
}

func triggerRedistribution() {
        fmt.Println("[INFO] Triggering redistribution due to node status change.")
        err := distributeFiles()
        if err != nil {
                fmt.Printf("[ERROR] Redistribution failed: %s\n", err)
        }
}

func distributeFiles() error {
        mu.Lock()
        defer mu.Unlock()

        files, err := ioutil.ReadDir(backupFolder)
        if err != nil {
                return fmt.Errorf("failed to read backup folder: %s", err)
        }

        activeNodes := []Node{}
        for _, node := range nodes {
                if node.Status == "active" {
                        activeNodes = append(activeNodes, node)
                }
        }

        fmt.Printf("\n[INFO] Distributing files among %d active nodes\n", len(activeNodes))
        if len(activeNodes) == 0 {
                return fmt.Errorf("no active nodes available")
        }

        filesPerNode := len(files) / len(activeNodes)
        remainder := len(files) % len(activeNodes)

        bar := progressbar.Default(int64(len(files)))
        
        // Clear previous distribution
        dataIndex = make(map[string][]string)

        for i, node := range activeNodes {
                startIdx := i * filesPerNode
                endIdx := startIdx + filesPerNode
                if i == len(activeNodes)-1 {
                        endIdx += remainder
                }

                nodeFiles := files[startIdx:endIdx]
                fmt.Printf("[INFO] Sending %d files to %s (IP:%s Port:%d)\n", 
                        len(nodeFiles), node.ID, node.IP, node.Port)

                fileNames := []string{}
                for _, file := range nodeFiles {
                        err := sendFileToNode(node, file.Name())
                        if err != nil {
                                fmt.Printf("[ERROR] Failed to send %s to %s: %s\n", 
                                        file.Name(), node.ID, err)
                                continue
                        }
                        fileNames = append(fileNames, file.Name())
                        bar.Add(1)
                }
                dataIndex[node.MacID] = fileNames
        }

        return nil
}

func sendFileToNode(node Node, fileName string) error {
        data, err := ioutil.ReadFile(backupFolder + "/" + fileName)
        if err != nil {
                return fmt.Errorf("failed to read file '%s': %s", fileName, err)
        }

        url := fmt.Sprintf("http://%s:%d/receive", node.IP, node.Port)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
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
                return fmt.Errorf("node returned status code %d", resp.StatusCode)
        }

        return nil
}