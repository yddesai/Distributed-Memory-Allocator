package utils

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"phase2/types" // Changed from "../types"
	"time"
)

type ChunkManager struct {
	chunkSize int64
	basePath  string
}

func NewChunkManager(basePath string, chunkSize int64) *ChunkManager {
	return &ChunkManager{
		chunkSize: chunkSize,
		basePath:  basePath,
	}
}

func (cm *ChunkManager) CreateChunk(data []byte) (types.ChunkInfo, error) {
	chunkID := generateChunkID()
	chunkPath := filepath.Join(cm.basePath, chunkID)

	chunk := types.ChunkInfo{
		ChunkID: chunkID,
		Size:    int64(len(data)),
		Status:  "active",
	}

	err := ioutil.WriteFile(chunkPath, data, 0644)
	if err != nil {
		return chunk, err
	}

	return chunk, nil
}

func (cm *ChunkManager) ReadChunk(chunkID string) ([]byte, error) {
	chunkPath := filepath.Join(cm.basePath, chunkID)
	return ioutil.ReadFile(chunkPath)
}

func generateChunkID() string {
	return fmt.Sprintf("chunk_%d", time.Now().UnixNano())
}
