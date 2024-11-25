package utils

import (
	"phase2/types" // Changed from "../types"
	"sync"
)

type MetricsCollector struct {
	mu            sync.RWMutex
	metrics       map[string]*types.NodeMetrics
	systemMetrics *types.SystemMetrics
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics:       make(map[string]*types.NodeMetrics),
		systemMetrics: &types.SystemMetrics{},
	}
}

func (mc *MetricsCollector) UpdateNodeMetrics(nodeID string, metrics *types.NodeMetrics) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics[nodeID] = metrics
	mc.updateSystemMetrics()
}

func (mc *MetricsCollector) updateSystemMetrics() {
	totalNodes := len(mc.metrics)
	activeNodes := 0
	totalSize := int64(0)

	for _, m := range mc.metrics {
		if m.LoadPercent > 0 {
			activeNodes++
		}
		totalSize += int64(m.Used)
	}

	mc.systemMetrics.TotalNodes = totalNodes
	mc.systemMetrics.ActiveNodes = activeNodes
	mc.systemMetrics.TotalDataSize = totalSize
}
