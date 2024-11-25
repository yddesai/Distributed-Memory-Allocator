package types

import (
	"time"
)

// Node structure
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

// ChunkInfo stores information about data chunks
type ChunkInfo struct {
	ChunkID    string `json:"chunk_id"`
	StartID    int    `json:"start_id"`
	EndID      int    `json:"end_id"`
	Size       int64  `json:"size"` // in bytes
	NodeID     string `json:"node_id"`
	Status     string `json:"status"`
	SourcePath string `json:"source_path"`
	FileName   string `json:"file_name"`
}

// SystemMetrics stores system-wide metrics
type SystemMetrics struct {
	TotalNodes        int   `json:"total_nodes"`
	ActiveNodes       int   `json:"active_nodes"`
	TotalDataSize     int64 `json:"total_data_size"`
	TotalChunks       int   `json:"total_chunks"`
	DistributedChunks int   `json:"distributed_chunks"`
}

// NodeMetrics stores per-node metrics
type NodeMetrics struct {
	Capacity    int     `json:"capacity"`
	Used        int     `json:"used"`
	Available   int     `json:"available"`
	ChunkCount  int     `json:"chunk_count"`
	LoadPercent float64 `json:"load_percent"`
}

// LocalTable stores node's local data information
type LocalTable struct {
	NodeID   string               `json:"node_id"`
	Chunks   map[string]ChunkInfo `json:"chunks"`
	Capacity int                  `json:"capacity"`
	Used     int                  `json:"used"`
	LastSync time.Time            `json:"last_sync"`
}

// Query represents a data query
type Query struct {
	ID        int       `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

// QueryResponse represents the response to a query
type QueryResponse struct {
	Data   interface{} `json:"data"`
	Status string      `json:"status"`
	Time   int64       `json:"time"`    // processing time in ms
	NodeID string      `json:"node_id"` // responding node
}

type FileInfo struct {
	Name string
	Size int64
	Path string
}

// Add this new struct
type MasterIndex struct {
    TotalRecords int           `json:"total_records"`
    Chunks       []ChunkInfo   `json:"chunks"`
}
