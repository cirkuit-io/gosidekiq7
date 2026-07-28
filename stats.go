package gosidekiq7

import (
	"runtime"
	"sync/atomic"

	workers "github.com/digitalocean/go-workers2"
)

// Stats represents current manager statistics.
type Stats struct {
	BusyWorkers int32
	Concurrency int32
	Queues      []string
	ProcessID   string
	Sentinel    bool
	Memory      MemoryStats
}

// MemoryStats represents memory usage statistics.
type MemoryStats struct {
	RSS        uint64 // Resident set size in KB
	Alloc      uint64 // Allocated memory in bytes
	TotalAlloc uint64 // Total allocated memory in bytes
	Sys        uint64 // System memory in bytes
	NumGC      uint32 // Number of GC cycles
}

// GetStats returns current statistics.
func (m *Manager) GetStats() Stats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return Stats{
		BusyWorkers: atomic.LoadInt32(&m.busyWorkers),
		Concurrency: atomic.LoadInt32(&m.concurrency),
		Queues:      m.queues,
		ProcessID:   m.processID,
		Sentinel:    m.sentinel,
		Memory: MemoryStats{
			RSS:        getRSS(),
			Alloc:      memStats.Alloc,
			TotalAlloc: memStats.TotalAlloc,
			Sys:        memStats.Sys,
			NumGC:      memStats.NumGC,
		},
	}
}

// GetWorkerStats returns the underlying go-workers2 stats.
func (m *Manager) GetWorkerStats() (workers.Stats, error) {
	return m.Manager.GetStats()
}

