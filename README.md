# gosidekiq7

A thin wrapper around [`go-workers2`](https://github.com/digitalocean/go-workers2)
that makes a Go background-job process visible in Sidekiq 7.3's Web UI —
process heartbeat, queue registration, busy-worker tracking, and Redis
Sentinel support, out of the box.

## Why this exists

`go-workers2` is wire-compatible with Sidekiq's Redis data format for
queues and jobs, but it doesn't implement everything Sidekiq's dashboard
expects to see: the process heartbeat, the `queues` registry set, or
per-process busy tracking. `gosidekiq7` fills in those gaps, so a Go
worker process shows up in Sidekiq::Web (Processes, Queues, Busy tabs)
alongside your Ruby/Sidekiq workers, if you point them at the same Redis.

## Installation

```bash
go get github.com/cirkuit-io/gosidekiq7
```

## Quick start

```go
package main

import (
	"fmt"
	"time"

	"github.com/cirkuit-io/gosidekiq7"
	workers "github.com/digitalocean/go-workers2"
)

func main() {
	manager, err := gosidekiq7.NewManager(gosidekiq7.Options{
		RedisAddr: "localhost:6379",
		RedisDB:   0,
		Tag:       "my-app",
		Queues:    []string{"default"},
	})
	if err != nil {
		panic(err)
	}

	manager.AddWorker("default", 5, func(msg *workers.Msg) error {
		fmt.Printf("Processing job: %v\n", msg.Args())
		return nil
	})

	manager.Enqueue("default", "SomeJob", map[string]interface{}{
		"id": 1,
	})

	manager.Run() // blocks
}
```

## Configuration

`Options` fields:

| Field | Description |
|---|---|
| `RedisAddr` | Direct Redis connection (`host:port`). Use this OR `SentinelAddrs`. |
| `SentinelAddrs` | Comma-separated Sentinel addresses, e.g. `"sentinel1:26379,sentinel2:26379"`. |
| `RedisMasterName` | Required if using Sentinel — the master name your Sentinel config monitors. |
| `RedisPassword` | Redis auth password, if required. |
| `RedisTLSConfig` | Optional `*tls.Config` for TLS connections. |
| `RedisDB` | Redis DB index (0-15). |
| `PoolSize` | Connection pool size. Defaults to 10. |
| `Namespace` | Prefixes every key this manager creates/reads. See "Namespacing" below. |
| `Tag` | Display label shown in Sidekiq::Web's Processes tab. Cosmetic only. |
| `Queues` | Informational list of queues this process handles. Populated automatically as you call `AddWorker`. |
markdown
## Configuration

`Options` fields:

| Field | Description |
|---|---|
| `RedisAddr` | Direct Redis connection (`host:port`). Use this OR `SentinelAddrs`. |
| `SentinelAddrs` | Comma-separated Sentinel addresses, e.g. `"sentinel1:26379,sentinel2:26379"`. |
| `RedisMasterName` | Required if using Sentinel — the master name your Sentinel config monitors. |
| `RedisPassword` | Redis auth password, if required. |
| `RedisTLSConfig` | Optional `*tls.Config` for TLS connections. |
| `RedisDB` | Redis DB index (0-15). |
| `PoolSize` | Connection pool size. Defaults to 10. |
| `Namespace` | Prefixes every key this manager creates/reads. See "Namespacing" below. |
| `Tag` | Display label shown in Sidekiq::Web's Processes tab. Cosmetic only. |
| `Queues` | Informational list of queues this process handles. Populated automatically as you call `AddWorker`. |
| `DrainTimeout` | Bounds how long `Stop()` will wait for in-flight jobs to finish before giving up. Defaults to 25 seconds if unset. go-workers2's own `Stop()` documents that it "returns immediately" without waiting for in-flight jobs — this option registers a drain hook so `Stop()` actually blocks until busy workers reach zero or this timeout elapses, whichever comes first. Set it slightly shorter than whatever grace period your deploy environment (Docker/Kubernetes) gives you before escalating to `SIGKILL`. |

## Sentinel / high-availability setup

```go
manager, err := gosidekiq7.NewManager(gosidekiq7.Options{
	SentinelAddrs:   "sentinel1:26379,sentinel2:26379,sentinel3:26379",
	RedisMasterName: "mymaster",
	RedisPassword:   "your-password",
	RedisDB:         0,
})
```

Both the job-processing connection and the heartbeat connection
automatically use Sentinel-aware failover clients when `SentinelAddrs` is
set — no separate configuration needed.

## Namespacing (sharing Redis with a Ruby Sidekiq app)

If you're running this alongside a real Ruby Sidekiq application on the
**same Redis DB**, set `Namespace` to avoid key collisions:

```go
manager, err := gosidekiq7.NewManager(gosidekiq7.Options{
	RedisAddr: "localhost:6379",
	RedisDB:   0,
	Namespace: "my-go-app",
})
```

**Important — the Ruby side needs matching configuration too.** you'll need the[`redis-client-namespace`]
gem on the Ruby side.

## Patterns: building recurring/self-rescheduling jobs

This library doesn't include a scheduler, but a common pattern is having
each job reschedule its own next occurrence:

```go
manager.AddWorker("recurring", 1, func(msg *workers.Msg) error {
	// Reschedule FIRST, so a crash during the real work below
	// doesn't kill the recurring chain.
	manager.EnqueueIn("recurring", "my-job", time.Minute, nil)

	doTheActualWork()
	return nil
})

// Seed the first occurrence once, at startup — guard against
// duplicate seeding across restarts/multiple instances with your
// own SETNX-based check against Redis.
manager.EnqueueIn("recurring", "my-job", time.Minute, nil)
```

**Known limitation of this pattern:** if the process crashes *after* the
job starts but *before* the reschedule call completes, that job's chain
stops permanently with no automatic recovery. Consider adding your own
periodic health check (comparing "last seen" timestamps against an
expected interval) as a safety net — this library doesn't include one out
of the box.

## Testing

```bash
go build ./...
go vet ./...
```

If you're testing against a real Redis locally:
```bash
docker run -d -p 6379:6379 redis:7
```
## License

MIT
