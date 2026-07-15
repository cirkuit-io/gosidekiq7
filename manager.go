package gosidekiq7

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	workers "github.com/digitalocean/go-workers2"
)

// Manager wraps go-workers2 Manager with Sidekiq 7.3 Web UI compatibility
type Manager struct {
	*workers.Manager
	producer    *workers.Producer
	busyWorkers int32
	processID   string
	redisAddr   string
	sentinel    bool
	redisDB     int
	concurrency int32
	queues      []string
	tag         string
	namespace   string
	stopCh      chan struct{}

	sentinelAddrs   []string
	redisMasterName string
	redisPassword   string
	redisTLSConfig  *tls.Config
}

// Options for creating a new Sidekiq 7.3 compatible manager.
//
// Provide either RedisAddr (direct connection) OR SentinelAddrs +
// RedisMasterName (Sentinel-managed failover) — not both. Sentinel takes
// precedence if both happen to be set.
type Options struct {
	RedisAddr       string
	SentinelAddrs   string // comma-separated, e.g. "sentinel1:26379,sentinel2:26379"
	RedisMasterName string
	RedisPassword   string
	RedisTLSConfig  *tls.Config
	RedisDB         int
	PoolSize        int
	Namespace       string   // prefixes all Redis keys this manager creates/reads directly
	Tag             string   // Optional tag for process identification
	Queues          []string // List of queues this process handles
}

// NewManager creates a new Sidekiq 7.3 compatible manager.
func NewManager(opts Options) (*Manager, error) {
	if opts.PoolSize == 0 {
		opts.PoolSize = 10
	}
	if opts.Tag == "" {
		opts.Tag = "golang"
	}
	if len(opts.Queues) == 0 {
		opts.Queues = []string{"default"}
	}

	usingSentinel := opts.SentinelAddrs != ""

	if usingSentinel && opts.RedisAddr != "" {
		fmt.Fprintf(os.Stderr, "gosidekiq7: both RedisAddr and SentinelAddrs set — using Sentinel, ignoring RedisAddr\n")
	}
	if !usingSentinel && opts.RedisAddr == "" {
		return nil, fmt.Errorf("gosidekiq7: must provide either RedisAddr or SentinelAddrs+RedisMasterName")
	}

	hostname, _ := os.Hostname()
	pid := os.Getpid()
	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	processID := fmt.Sprintf("%s:%d:%s", hostname, pid, nonce[:8])

	wOpts := workers.Options{
		Database:  opts.RedisDB,
		PoolSize:  opts.PoolSize,
		ProcessID: processID,
		Password:  opts.RedisPassword,
		Namespace: opts.Namespace,
	}

	if usingSentinel {
		wOpts.SentinelAddrs = opts.SentinelAddrs
		wOpts.RedisMasterName = opts.RedisMasterName
	} else {
		wOpts.ServerAddr = opts.RedisAddr
	}
	if opts.RedisTLSConfig != nil {
		wOpts.RedisTLSConfig = opts.RedisTLSConfig
	}

	wm, err := workers.NewManager(wOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create workers manager: %w", err)
	}

	var sentinelAddrList []string
	if usingSentinel {
		sentinelAddrList = strings.Split(opts.SentinelAddrs, ",")
	}

	m := &Manager{
		Manager:         wm,
		producer:        wm.Producer(),
		processID:       processID,
		redisAddr:       opts.RedisAddr,
		sentinel:        usingSentinel,
		sentinelAddrs:   sentinelAddrList,
		redisMasterName: opts.RedisMasterName,
		redisPassword:   opts.RedisPassword,
		redisTLSConfig:  opts.RedisTLSConfig,
		redisDB:         opts.RedisDB,
		queues:          opts.Queues,
		tag:             opts.Tag,
		namespace:       opts.Namespace,
		stopCh:          make(chan struct{}),
	}

	m.startHeartbeat()

	return m, nil
}

// AddWorker adds a worker with automatic busy tracking for Sidekiq Web UI.
func (m *Manager) AddWorker(queue string, concurrency int, fn func(*workers.Msg) error) {
	m.trackQueue(queue)
	atomic.AddInt32(&m.concurrency, int32(concurrency))

	m.Manager.AddWorker(queue, concurrency, func(msg *workers.Msg) error {
		atomic.AddInt32(&m.busyWorkers, 1)
		defer atomic.AddInt32(&m.busyWorkers, -1)
		return fn(msg)
	})
}

// AddWorkerWithMiddleware adds a worker with custom middleware.
func (m *Manager) AddWorkerWithMiddleware(queue string, concurrency int, fn func(*workers.Msg) error, middlewares ...workers.MiddlewareFunc) {
	m.trackQueue(queue)
	atomic.AddInt32(&m.concurrency, int32(concurrency))

	wrappedFn := func(msg *workers.Msg) error {
		atomic.AddInt32(&m.busyWorkers, 1)
		defer atomic.AddInt32(&m.busyWorkers, -1)
		return fn(msg)
	}

	m.Manager.AddWorker(queue, concurrency, wrappedFn, middlewares...)
}

func (m *Manager) trackQueue(queue string) {
	for _, q := range m.queues {
		if q == queue {
			return
		}
	}
	m.queues = append(m.queues, queue)
	m.registerQueueInRedis(queue)
}

// registerQueueInRedis adds this queue's name to Sidekiq's "queues" set,
// which is what Sidekiq::Web reads to know which queues exist at all.
// go-workers2 doesn't populate this set on its own, so we do it explicitly
// here. Uses a bare, unprefixed key — matching startHeartbeat's existing
// pattern for "processes", which is confirmed working.
func (m *Manager) registerQueueInRedis(queue string) {
	rdb := m.newHeartbeatClient()
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.SAdd(ctx, m.prefixed("queues"), queue).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "gosidekiq7: failed to register queue %s: %v\n", queue, err)
	}
}
// Producer returns the producer for enqueueing jobs.
func (m *Manager) Producer() *workers.Producer {
	return m.producer
}

// Enqueue is a convenience method for enqueueing jobs.
func (m *Manager) Enqueue(queue, class string, args interface{}) (string, error) {
	return m.producer.Enqueue(queue, class, args)
}

// EnqueueAt enqueues a job to run at a specific time.
func (m *Manager) EnqueueAt(queue, class string, runAt time.Time, args interface{}) (string, error) {
	return m.producer.EnqueueAt(queue, class, runAt, args)
}

// EnqueueIn enqueues a job to run after a duration.
func (m *Manager) EnqueueIn(queue, class string, delay time.Duration, args interface{}) (string, error) {
	runAt := time.Now().Add(delay)
	return m.producer.EnqueueAt(queue, class, runAt, args)
}

// BusyWorkers returns the current number of busy workers.
func (m *Manager) BusyWorkers() int32 {
	return atomic.LoadInt32(&m.busyWorkers)
}

// Concurrency returns the live total concurrency across all registered queues.
func (m *Manager) Concurrency() int32 {
	return atomic.LoadInt32(&m.concurrency)
}

// Stop gracefully stops the manager.
func (m *Manager) Stop() {
	close(m.stopCh)
	m.Manager.Stop()
}

// Run starts processing jobs (blocks until stopped).
func (m *Manager) Run() {
	m.Manager.Run()
}
