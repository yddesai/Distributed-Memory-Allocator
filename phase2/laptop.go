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
	"phase2/config"
	"phase2/types"
	"strings"
	"time"
)

var (
	localFolder = config.LOCAL_FOLDER
	awsURL      = config.DefaultConfig.AWSURL
	node        types.Node
)

func main() {
	os.MkdirAll(localFolder, 0755)

	// Initialize node with port selection
	if !initializeNode() {
		fmt.Println("[ERROR] No available ports found. Exiting...")
		return
	}

	// Register with AWS
	registerNode()

	// Start heartbeat
	go sendHeartbeats()

	// Setup HTTP server for receiving files
	http.HandleFunc("/receive", receiveFiles)
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/query", handleQuery)

	// Start command interface
	go commandInterface()

	fmt.Printf("[INFO] Laptop node %s started. Listening on port %d...\n", node.ID, node.Port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", node.Port), nil)
	if err != nil {
		fmt.Printf("[ERROR] Failed to start server: %v\n", err)
		os.Exit(1)
	}
}

func initializeNode() bool {
	nodeID := fmt.Sprintf("laptop-%d", time.Now().Unix()%1000)
	ip := getPublicIP()
	existingNode := checkExistingNodesWithSameIP(ip)
	port := findAvailablePort(existingNode)

	if port == -1 {
		return false
	}

	node = types.Node{
		ID:       nodeID,
		Capacity: 500, // Default capacity in MB
		MacID:    getMacAddress(),
		IP:       ip,
		Port:     port,
		Status:   "active",
	}

	fmt.Printf("[INFO] Initialized node: %s (MAC: %s, IP: %s, Port: %d)\n",
		node.ID, node.MacID, node.IP, node.Port)
	return true
}

func getMacAddress() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return fmt.Sprintf("unknown-%d", time.Now().Unix())
	}

	for _, i := range interfaces {
		if i.Flags&net.FlagUp != 0 && !strings.HasPrefix(i.Name, "lo") {
			return i.HardwareAddr.String()
		}
	}

	return fmt.Sprintf("unknown-%d", time.Now().Unix())
}

func getPublicIP() string {
	resp, err := http.Get("https://api.ipify.org?format=text")
	if err != nil {
		fmt.Println("[ERROR] Unable to fetch public IP:", err)
		return "localhost"
	}
	defer resp.Body.Close()

	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "localhost"
	}
	return string(ip)
}

func checkExistingNodesWithSameIP(ip string) bool {
	resp, err := http.Get(fmt.Sprintf("%s/nodes", awsURL))
	if err != nil {
		fmt.Printf("[WARNING] Could not check existing nodes: %v\n", err)
		return false
	}
	defer resp.Body.Close()

	var existingNodes map[string]types.Node
	if err := json.NewDecoder(resp.Body).Decode(&existingNodes); err != nil {
		fmt.Printf("[WARNING] Could not decode existing nodes: %v\n", err)
		return false
	}

	for _, n := range existingNodes {
		if n.IP == ip && n.Status == "active" {
			return true
		}
	}
	return false
}

func findAvailablePort(existingPort bool) int {
	if !existingPort {
		listener, err := net.Listen("tcp", ":8081")
		if err == nil {
			listener.Close()
			return 8081
		}
	}

	for port := 5000; port < 5010; port++ {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err == nil {
			listener.Close()
			return port
		}
	}
	return -1
}

func registerNode() {
	data, _ := json.Marshal(node)
	resp, err := http.Post(awsURL+"/register", "application/json", bytes.NewBuffer(data))
	if err != nil {
		fmt.Println("[ERROR] Failed to register with AWS:", err)
		return
	}
	defer resp.Body.Close()
}

func sendHeartbeats() {
	for {
		data, _ := json.Marshal(node)
		_, err := http.Post(awsURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
		if err != nil {
			fmt.Println("[ERROR] Failed to send heartbeat:", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       node.ID,
		"status":   "active",
		"capacity": node.Capacity,
		"port":     node.Port,
	})
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
	// Implementation needed based on your requirements
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

	filePath := fmt.Sprintf("%s/%s", localFolder, fileName)
	err = ioutil.WriteFile(filePath, body, 0644)
	if err != nil {
		http.Error(w, "Failed to save file", http.StatusInternalServerError)
		return
	}

	fmt.Printf("[INFO] Received file: %s\n", fileName)
	w.WriteHeader(http.StatusOK)
}

func commandInterface() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("\nAvailable commands (Port %d):\n", node.Port)
	fmt.Println("1. distribute - Upload and distribute dataset")
	fmt.Println("2. status    - Show node status")
	fmt.Println("3. list      - List received files")
	fmt.Println("4. exit      - Exit the program")

	for {
		fmt.Print("\nEnter command: ")
		command, _ := reader.ReadString('\n')
		command = strings.TrimSpace(command)

		switch command {
		case "distribute":
			handleDistribute()
		case "status":
			showStatus()
		case "list":
			listFiles()
		case "exit":
			fmt.Println("[INFO] Shutting down...")
			os.Exit(0)
		default:
			fmt.Println("[ERROR] Unknown command")
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

	fmt.Printf("[INFO] Uploading %d files to AWS...\n", len(files))

	for _, file := range files {
		if !file.IsDir() {
			filePath := fmt.Sprintf("%s/%s", path, file.Name())
			err := uploadFile(filePath, file.Name())
			if err != nil {
				fmt.Printf("[ERROR] Failed to upload %s: %s\n", file.Name(), err)
			}
		}
	}

	// Trigger distribution
	_, err = http.Post(awsURL+"/distribute", "application/json", nil)
	if err != nil {
		fmt.Println("[ERROR] Failed to trigger distribution:", err)
		return
	}

	fmt.Println("[INFO] Distribution initiated successfully")
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
	fmt.Println("\n=== Node Status ===")
	fmt.Printf("Node ID: %s\n", node.ID)
	fmt.Printf("MAC Address: %s\n", node.MacID)
	fmt.Printf("IP Address: %s\n", node.IP)
	fmt.Printf("Port: %d\n", node.Port)
	fmt.Printf("Status: %s\n", node.Status)
}

func listFiles() {
	files, err := ioutil.ReadDir(localFolder)
	if err != nil {
		fmt.Printf("[ERROR] Failed to read directory: %s\n", err)
		return
	}

	fmt.Printf("\n=== Received Files (Port %d) ===\n", node.Port)
	if len(files) == 0 {
		fmt.Println("No files received yet")
		return
	}

	for _, file := range files {
		size := float64(file.Size()) / 1024 // Size in KB
		fmt.Printf("%s (%.2f KB)\n", file.Name(), size)
	}
}
