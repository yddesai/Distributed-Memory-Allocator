package utils

import (
	"phase2/types" // Changed from "../types"
	"sync"
)

type QueryHandler struct {
	queriesInProgress sync.Map
	resultCache       sync.Map
	cacheTimeout      int64
}

func NewQueryHandler() *QueryHandler {
	return &QueryHandler{
		cacheTimeout: 300, // 5 minutes cache
	}
}

func (qh *QueryHandler) ProcessQuery(query types.Query) (*types.QueryResponse, error) {
	// Check cache first
	if result, ok := qh.checkCache(query); ok {
		return result, nil
	}

	// Track query
	qh.queriesInProgress.Store(query.ID, query)
	defer qh.queriesInProgress.Delete(query.ID)

	return nil, nil // Actual implementation in node-specific code
}

func (qh *QueryHandler) checkCache(query types.Query) (*types.QueryResponse, bool) {
	if result, ok := qh.resultCache.Load(query.ID); ok {
		response := result.(*types.QueryResponse)
		return response, true
	}
	return nil, false
}
