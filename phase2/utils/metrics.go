package utils

import (
	"fmt"
	"sync"

	"phase2/types"
)

var (
	metricsLock   sync.RWMutex
	systemMetrics types.SystemMetrics
	nodeMetrics   map[string]types.NodeMetrics
)

func UpdateNodeMetrics(node types.Node, chunks []types.ChunkInfo) {
	metricsLock.Lock()
	defer metricsLock.Unlock()

	used := 0
	for _, chunk := range chunks {
		used += int(chunk.Size / 1024 / 1024) // Convert to MB
	}

	nodeMetrics[node.ID] = types.NodeMetrics{
		Capacity:    node.Capacity,
		Used:        used,
		Available:   node.Capacity - used,
		ChunkCount:  len(chunks),
		LoadPercent: float64(used) / float64(node.Capacity) * 100,
	}
}

func PrintSystemMetrics() {
	metricsLock.RLock()
	defer metricsLock.RUnlock()

	fmt.Println("\n=== SYSTEM METRICS ===")
	fmt.Printf("Total Nodes: %d\n", systemMetrics.TotalNodes)
	fmt.Printf("Active Nodes: %d\n", systemMetrics.ActiveNodes)
	fmt.Printf("Total Data Size: %.2f GB\n", float64(systemMetrics.TotalDataSize)/(1024*1024*1024))
	fmt.Printf("Total Chunks: %d\n", systemMetrics.TotalChunks)
	fmt.Printf("Distributed Chunks: %d\n", systemMetrics.DistributedChunks)
}

func PrintNodeMetrics(nodeID string) {
	metricsLock.RLock()
	defer metricsLock.RUnlock()

	if metrics, exists := nodeMetrics[nodeID]; exists {
		fmt.Printf("\n=== NODE METRICS: %s ===\n", nodeID)
		fmt.Printf("Capacity: %d MB\n", metrics.Capacity)
		fmt.Printf("Used: %d MB\n", metrics.Used)
		fmt.Printf("Available: %d MB\n", metrics.Available)
		fmt.Printf("Chunks: %d\n", metrics.ChunkCount)
		fmt.Printf("Load: %.2f%%\n", metrics.LoadPercent)
	}
}
