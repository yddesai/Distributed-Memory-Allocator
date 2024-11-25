package utils

import (
	"math"

	"phase2/types"

)

func CalculateDistribution(totalSize int64, nodes []types.Node) map[string]int64 {
	distribution := make(map[string]int64)

	// Calculate total capacity
	var totalCapacity int64
	for _, node := range nodes {
		totalCapacity += int64(node.Capacity)
	}

	// Calculate proportional distribution
	for _, node := range nodes {
		proportion := float64(node.Capacity) / float64(totalCapacity)
		nodeSize := int64(math.Floor(float64(totalSize) * proportion))
		distribution[node.ID] = nodeSize
	}

	return distribution
}

func RecalculateDistribution(chunks []types.ChunkInfo, activeNodes []types.Node) map[string][]types.ChunkInfo {
	newDistribution := make(map[string][]types.ChunkInfo)

	if len(activeNodes) == 0 {
		return newDistribution
	}

	// Calculate total capacity of active nodes
	var totalCapacity int64
	for _, node := range activeNodes {
		totalCapacity += int64(node.Capacity)
	}

	// Sort chunks by size for better distribution
	// Note: Implement sorting logic if needed

	// Distribute chunks proportionally
	currentNodeIndex := 0
	for _, chunk := range chunks {
		node := activeNodes[currentNodeIndex]
		newDistribution[node.ID] = append(newDistribution[node.ID], chunk)

		// Move to next node using capacity-weighted round-robin
		currentNodeIndex = (currentNodeIndex + 1) % len(activeNodes)
	}

	return newDistribution
}
