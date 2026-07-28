package gosidekiq7

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func (m *Manager) newHeartbeatClient() redis.UniversalClient {
	if m.sentinel {
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    m.redisMasterName,
			SentinelAddrs: m.sentinelAddrs,
			Password:      m.redisPassword,
			DB:            m.redisDB,
			TLSConfig:     m.redisTLSConfig,
		})
	}
	return redis.NewClient(&redis.Options{
		Addr:      m.redisAddr,
		Password:  m.redisPassword,
		DB:        m.redisDB,
		TLSConfig: m.redisTLSConfig,
	})
}

// prefixed returns key with this manager's namespace prepended, matching
// how Rails' redis-client-namespace middleware will rewrite the same
// literal key when Sidekiq::Web reads it.
func (m *Manager) prefixed(key string) string {
	if m.namespace != "" {
		return m.namespace + ":" + key
	}
	return key
}

func (m *Manager) startHeartbeat() {
	rdb := m.newHeartbeatClient()
	ctx := context.Background()
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	startedAt := time.Now().Unix()

	processesKey := m.prefixed("processes")
	identityKey := m.prefixed(m.processID)

	go func() {
		defer rdb.Close()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-m.stopCh:
				rdb.SRem(ctx, processesKey, m.processID)
				rdb.Del(ctx, identityKey)
				return
			case <-ticker.C:
				now := time.Now()
				nowUnix := now.Unix()
				currentBusy := atomic.LoadInt32(&m.busyWorkers)
				currentConcurrency := atomic.LoadInt32(&m.concurrency)
				rssKB := getRSS()
				processInfo := map[string]interface{}{
					"hostname":    hostname,
					"started_at":  float64(startedAt),
					"pid":         pid,
					"tag":         m.tag,
					"concurrency": currentConcurrency,
					"queues":      m.queues,
					"weights":     buildWeights(m.queues),
					"labels":      []string{},
					"identity":    m.processID,
					"version":     "7.3.10",
					"embedded":    false,
				}
				infoJSON, err := json.Marshal(processInfo)
				if err != nil {
					continue
				}
				pipe := rdb.Pipeline()
				pipe.SAdd(ctx, processesKey, m.processID)
				pipe.HSet(ctx, identityKey, map[string]interface{}{
					"busy":   fmt.Sprintf("%d", currentBusy),
					"quiet":  "false",
					"rss":    fmt.Sprintf("%d", rssKB),
					"rtt_us": "0",
					"info":   string(infoJSON),
					"beat":   fmt.Sprintf("%f", float64(nowUnix)),
				})
				pipe.Expire(ctx, identityKey, 60*time.Second)
				if _, err := pipe.Exec(ctx); err != nil {
					continue
				}
			}
		}
	}()
}

func buildWeights(queues []string) []map[string]int {
	weights := make([]map[string]int, len(queues))
	for i, q := range queues {
		weights[i] = map[string]int{q: 0}
	}
	return weights
}

func getRSS() uint64 {
	var rusage syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	if err == nil {
		if runtime.GOOS == "darwin" {
			return uint64(rusage.Maxrss) / 1024
		}
		return uint64(rusage.Maxrss)
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys / 1024
}
