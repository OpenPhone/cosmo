package redis

import (
	"context"
	"hash/fnv"
	"strings"
	"sync"
	"unsafe"

	"github.com/redis/go-redis/v9"
	"github.com/wundergraph/cosmo/router/pkg/metric"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"go.uber.org/zap"
)

const (
	// Matches the Redis default pubsub client-output-buffer soft limit.
	subscriberQueueMaxBytes = 8 * 1024 * 1024
	// Bounds memory when a burst hits many slow subscribers at once.
	providerQueueMaxBytes = 64 * 1024 * 1024
)

var messageOverhead = int64(unsafe.Sizeof(redis.Message{}))

// A shard is one PubSub connection. A key always maps to the same shard, so the
// shard's subscriber count for a key decides when to SUBSCRIBE and UNSUBSCRIBE.
type shard struct {
	mu     sync.Mutex
	pubsub *redis.PubSub
	subs   map[string]map[*subscriber]struct{}
}

type subscriber struct {
	providerID string
	notify     chan struct{}

	mu      sync.Mutex
	queue   []*redis.Message
	bytes   int64
	dropped int
}

func isPattern(channel string) bool {
	return strings.ContainsAny(channel, `*?[\`)
}

func (p *ProviderAdapter) startShards() {
	if p.subscriptionConnections <= 0 {
		return
	}
	p.shards = make([]*shard, p.subscriptionConnections)
	for i := range p.shards {
		sh := &shard{
			pubsub: p.conn.Subscribe(p.ctx),
			subs:   make(map[string]map[*subscriber]struct{}),
		}
		p.shards[i] = sh
		msgs := sh.pubsub.Channel()
		p.closeWg.Go(func() { p.dispatch(sh, msgs) })
	}
}

func (p *ProviderAdapter) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return p.shards[h.Sum32()%uint32(len(p.shards))]
}

// dispatch never blocks on a subscriber: go-redis drops messages when its
// channel stays full, so a slow subscriber must not hold up the shard.
func (p *ProviderAdapter) dispatch(sh *shard, msgs <-chan *redis.Message) {
	for msg := range msgs {
		key := msg.Channel
		if msg.Pattern != "" {
			key = msg.Pattern
		}
		sh.mu.Lock()
		for s := range sh.subs[key] {
			if !p.enqueue(s, msg) {
				p.streamMetricStore.Consume(p.ctx, metric.StreamsEvent{
					ProviderId:          s.providerID,
					StreamOperationName: redisReceive,
					ProviderType:        metric.ProviderTypeRedis,
					ErrorType:           "dropped",
					DestinationName:     msg.Channel,
				})
			}
		}
		sh.mu.Unlock()
	}
}

func messageSize(msg *redis.Message) int64 {
	return int64(len(msg.Payload)+len(msg.Channel)+len(msg.Pattern)) + messageOverhead
}

func (p *ProviderAdapter) enqueue(s *subscriber, msg *redis.Message) bool {
	size := messageSize(msg)

	s.mu.Lock()
	defer s.mu.Unlock()

	ok := s.bytes+size <= subscriberQueueMaxBytes
	if ok && p.queuedBytes.Add(size) > providerQueueMaxBytes {
		p.queuedBytes.Add(-size)
		ok = false
	}
	if ok {
		s.queue = append(s.queue, msg)
		s.bytes += size
	} else {
		s.dropped++
	}

	select {
	case s.notify <- struct{}{}:
	default:
	}
	return ok
}

func (p *ProviderAdapter) take(s *subscriber) ([]*redis.Message, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs, dropped := s.queue, s.dropped
	// Keep the batch charged until each message has been delivered or discarded.
	s.queue, s.dropped = nil, 0
	return msgs, dropped
}

func (p *ProviderAdapter) subscribeShared(ctx context.Context, providerID string, keys []string, updater datasource.SubscriptionEventUpdater, log *zap.Logger) {
	s := &subscriber{providerID: providerID, notify: make(chan struct{}, 1)}
	for _, key := range keys {
		p.addSubscriber(key, s, log)
	}

	p.closeWg.Go(func() {
		defer p.removeSubscriber(keys, s, log)

		for {
			select {
			case <-s.notify:
				msgs, dropped := p.take(s)
				if dropped > 0 {
					log.Warn("redis subscriber queue full, dropped messages", zap.Int("dropped", dropped))
				}
				for i, msg := range msgs {
					if ctx.Err() != nil || p.ctx.Err() != nil {
						return
					}
					p.streamMetricStore.Consume(ctx, metric.StreamsEvent{
						ProviderId:          providerID,
						StreamOperationName: redisReceive,
						ProviderType:        metric.ProviderTypeRedis,
						DestinationName:     msg.Channel,
					})
					updater.Update([]datasource.StreamEvent{
						&Event{evt: &MutableEvent{
							Data: []byte(msg.Payload),
						}},
					})
					size := messageSize(msg)
					msgs[i] = nil // Do not retain delivered payloads in the batch.
					s.mu.Lock()
					s.bytes -= size
					p.queuedBytes.Add(-size)
					s.mu.Unlock()
				}
			case <-p.ctx.Done():
				log.Debug("application context done, stopping subscription")
				return
			case <-ctx.Done():
				log.Debug("subscription context done, stopping subscription")
				return
			}
		}
	})
}

// SUBSCRIBE and UNSUBSCRIBE go out under the shard lock so an UNSUBSCRIBE cannot
// overtake a SUBSCRIBE for the same key.
func (p *ProviderAdapter) addSubscriber(key string, s *subscriber, log *zap.Logger) {
	sh := p.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	subs, ok := sh.subs[key]
	if !ok {
		subs = make(map[*subscriber]struct{})
		sh.subs[key] = subs
	}
	subs[s] = struct{}{}
	if ok {
		return
	}

	err := sh.subscribe(p.ctx, key)
	// A failed write reconnects and resubscribes before go-redis records the new
	// key, so the fresh connection would miss it. The retry runs after the key is
	// recorded: it writes on the new connection, or the next reconnect carries it.
	if err != nil && p.ctx.Err() == nil {
		err = sh.subscribe(p.ctx, key)
	}
	if err != nil {
		log.Warn("redis subscribe failed, will retry on reconnect", zap.String("key", key), zap.Error(err))
	}
}

func (sh *shard) subscribe(ctx context.Context, key string) error {
	if isPattern(key) {
		return sh.pubsub.PSubscribe(ctx, key)
	}
	return sh.pubsub.Subscribe(ctx, key)
}

func (p *ProviderAdapter) removeSubscriber(keys []string, s *subscriber, log *zap.Logger) {
	for _, key := range keys {
		sh := p.shardFor(key)
		sh.mu.Lock()
		subs := sh.subs[key]
		if _, ok := subs[s]; ok {
			delete(subs, s)
			if len(subs) == 0 {
				delete(sh.subs, key)
				// Shutdown closes the connections; nothing to unsubscribe from.
				if p.ctx.Err() == nil {
					var err error
					if isPattern(key) {
						err = sh.pubsub.PUnsubscribe(p.ctx, key)
					} else {
						err = sh.pubsub.Unsubscribe(p.ctx, key)
					}
					if err != nil {
						log.Warn("redis unsubscribe failed", zap.String("key", key), zap.Error(err))
					}
				}
			}
		}
		sh.mu.Unlock()
	}
	// No dispatcher can enqueue now. Release queued and unfinished batch bytes.
	s.mu.Lock()
	dropped := s.dropped
	p.queuedBytes.Add(-s.bytes)
	s.queue, s.bytes, s.dropped = nil, 0, 0
	s.mu.Unlock()
	if dropped > 0 {
		log.Warn("redis subscriber queue full, dropped messages", zap.Int("dropped", dropped))
	}
}
