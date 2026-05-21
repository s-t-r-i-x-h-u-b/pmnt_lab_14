// Package coordinator handles distributed shard assignment across multiple collector
// instances using etcd leader election and lease-based registration.
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	collectorPrefix  = "/collectors/"
	assignmentPrefix = "/assignments/"
	electionKey      = "/election/shard-coordinator"
	leaseTTL         = 15 // seconds
)

// Info is the metadata stored in etcd when a collector registers.
type Info struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at"`
	Host      string    `json:"host"`
}

// Coordinator manages etcd-based registration, leader election, and shard assignment.
type Coordinator struct {
	client   *clientv3.Client
	id       string
	info     Info
	session  *concurrency.Session
	election *concurrency.Election
	logger   *zap.Logger

	mu             sync.RWMutex
	assignedShards map[string]context.CancelFunc

	onAssigned   func(ctx context.Context, shard string)
	onUnassigned func(shard string)
}

func New(client *clientv3.Client, id string, info Info, logger *zap.Logger) *Coordinator {
	return &Coordinator{
		client:         client,
		id:             id,
		info:           info,
		logger:         logger,
		assignedShards: make(map[string]context.CancelFunc),
	}
}

// SetCallbacks registers callbacks fired when shards are assigned or removed.
func (c *Coordinator) SetCallbacks(onAssigned func(ctx context.Context, shard string), onUnassigned func(shard string)) {
	c.onAssigned = onAssigned
	c.onUnassigned = onUnassigned
}

// Start registers this instance in etcd, starts a leader-election goroutine, and watches
// for shard assignments. allShards is the complete universe of shards to distribute.
func (c *Coordinator) Start(ctx context.Context, allShards []string) error {
	var err error
	c.session, err = concurrency.NewSession(c.client, concurrency.WithTTL(leaseTTL))
	if err != nil {
		return fmt.Errorf("create etcd session: %w", err)
	}

	infoBytes, _ := json.Marshal(c.info)
	if _, err = c.client.Put(ctx, collectorPrefix+c.id, string(infoBytes),
		clientv3.WithLease(c.session.Lease())); err != nil {
		return fmt.Errorf("register in etcd: %w", err)
	}
	c.logger.Info("Registered in etcd", zap.String("id", c.id), zap.Int64("leaseID", int64(c.session.Lease())))

	c.election = concurrency.NewElection(c.session, electionKey)

	go c.watchAssignments(ctx)
	go c.runLeader(ctx, allShards)
	return nil
}

// Stop resigns from leadership, cancels all shard goroutines, and closes the etcd session.
func (c *Coordinator) Stop() {
	c.mu.Lock()
	for _, cancel := range c.assignedShards {
		cancel()
	}
	c.assignedShards = make(map[string]context.CancelFunc)
	c.mu.Unlock()

	if c.election != nil {
		_ = c.election.Resign(context.Background())
	}
	if c.session != nil {
		_ = c.session.Close()
	}
}

// AssignedShards returns the current list of shards owned by this instance.
func (c *Coordinator) AssignedShards() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.assignedShards))
	for s := range c.assignedShards {
		out = append(out, s)
	}
	return out
}

// runLeader campaigns for leadership in a loop, re-campaigning on loss.
func (c *Coordinator) runLeader(ctx context.Context, allShards []string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		c.logger.Info("Campaigning for shard-coordinator leadership")
		if err := c.election.Campaign(ctx, c.id); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("Campaign error", zap.Error(err))
			time.Sleep(5 * time.Second)
			continue
		}
		c.logger.Info("Became shard-coordinator leader")
		c.doLeaderWork(ctx, allShards)
		if ctx.Err() != nil {
			return
		}
		c.logger.Info("Lost leadership, re-campaigning")
	}
}

// doLeaderWork distributes shards and re-distributes whenever the collector set changes.
func (c *Coordinator) doLeaderWork(ctx context.Context, allShards []string) {
	c.distributeShards(ctx, allShards)

	watchCh := c.client.Watch(ctx, collectorPrefix, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-watchCh:
			if !ok {
				return
			}
			if resp.Err() != nil {
				c.logger.Error("Collector watch error", zap.Error(resp.Err()))
				return
			}
			c.logger.Info("Collector set changed, redistributing shards")
			c.distributeShards(ctx, allShards)
		}
	}
}

// distributeShards assigns shards round-robin among active collectors.
// Assignment keys are stored with the leader's lease so they expire when the leader dies.
func (c *Coordinator) distributeShards(ctx context.Context, allShards []string) {
	resp, err := c.client.Get(ctx, collectorPrefix, clientv3.WithPrefix())
	if err != nil {
		c.logger.Error("List collectors failed", zap.Error(err))
		return
	}
	collectors := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		collectors = append(collectors, string(kv.Key)[len(collectorPrefix):])
	}
	if len(collectors) == 0 {
		return
	}
	sort.Strings(collectors)
	c.logger.Info("Distributing shards",
		zap.Int("collectors", len(collectors)),
		zap.Int("shards", len(allShards)),
		zap.Strings("collector_ids", collectors),
	)

	if _, err = c.client.Delete(ctx, assignmentPrefix, clientv3.WithPrefix()); err != nil {
		c.logger.Error("Clear assignments failed", zap.Error(err))
		return
	}
	for i, shard := range allShards {
		owner := collectors[i%len(collectors)]
		key := assignmentPrefix + owner + "/" + shard
		if _, err := c.client.Put(ctx, key, c.id, clientv3.WithLease(c.session.Lease())); err != nil {
			c.logger.Error("Assign shard failed", zap.String("shard", shard), zap.Error(err))
		}
	}
}

// watchAssignments loads initial assignments and watches for changes to this collector's prefix.
func (c *Coordinator) watchAssignments(ctx context.Context) {
	myPrefix := assignmentPrefix + c.id + "/"

	resp, err := c.client.Get(ctx, myPrefix, clientv3.WithPrefix())
	if err == nil {
		for _, kv := range resp.Kvs {
			shard := string(kv.Key)[len(myPrefix):]
			c.startShard(ctx, shard)
		}
	}

	watchCh := c.client.Watch(ctx, myPrefix, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-watchCh:
			if !ok {
				return
			}
			for _, ev := range resp.Events {
				shard := string(ev.Kv.Key)[len(myPrefix):]
				switch ev.Type {
				case clientv3.EventTypePut:
					c.startShard(ctx, shard)
				case clientv3.EventTypeDelete:
					c.stopShard(shard)
				}
			}
		}
	}
}

func (c *Coordinator) startShard(ctx context.Context, shard string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.assignedShards[shard]; ok {
		return
	}
	shardCtx, cancel := context.WithCancel(ctx)
	c.assignedShards[shard] = cancel
	c.logger.Info("Shard assigned", zap.String("shard", shard))
	if c.onAssigned != nil {
		go c.onAssigned(shardCtx, shard)
	}
}

func (c *Coordinator) stopShard(shard string) {
	c.mu.Lock()
	cancel, ok := c.assignedShards[shard]
	if ok {
		delete(c.assignedShards, shard)
	}
	c.mu.Unlock()

	if ok {
		cancel()
		c.logger.Info("Shard unassigned", zap.String("shard", shard))
	}
	if c.onUnassigned != nil {
		c.onUnassigned(shard)
	}
}
