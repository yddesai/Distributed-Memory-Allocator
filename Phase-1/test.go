package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func main() {
	ip := getPublicIP()
	if ip == "unknown" {
		fmt.Println("[ERROR] Failed to fetch public IP.")
		return
	}

	fmt.Printf("[INFO] Server running on %s:8085\n", ip)

	http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Connection successful. Public IP: %s\n", ip)
	})

	err := http.ListenAndServe(":8085", nil)
	if err != nil {
		fmt.Printf("[ERROR] Failed to start server: %s\n", err)
	}
}

func getPublicIP() string {
	resp, err := http.Get("https://api.ipify.org?format=text")
	if err != nil {
		return "unknown"
	}
	defer resp.Body.Close()

	ip, _ := ioutil.ReadAll(resp.Body)
	return string(ip)
}
