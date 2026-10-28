package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	rd "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/wundergraph/cosmo/router/pkg/metric"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"
)

type blockingUpdater struct {
	recordingUpdater
	unblock chan struct{}
	once    sync.Once
	blocked atomic.Bool
}

// newBlockingUpdater releases in cleanup, so a failed assertion cannot leave
// Shutdown waiting on a blocked Update.
func newBlockingUpdater(t *testing.T) *blockingUpdater {
	b := &blockingUpdater{unblock: make(chan struct{})}
	t.Cleanup(b.release)
	return b
}

func (b *blockingUpdater) release() {
	b.once.Do(func() { close(b.unblock) })
}

func (b *blockingUpdater) Update(events []datasource.StreamEvent) {
	b.blocked.Store(true)
	<-b.unblock
	b.recordingUpdater.Update(events)
}

type droppedCounter struct {
	mu      sync.Mutex
	dropped int
}

func (d *droppedCounter) Produce(context.Context, metric.StreamsEvent) {}

func (d *droppedCounter) Consume(_ context.Context, event metric.StreamsEvent) {
	if event.ErrorType == "dropped" {
		d.mu.Lock()
		d.dropped++
		d.mu.Unlock()
	}
}

func (d *droppedCounter) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dropped
}

func newSharedAdapter(t *testing.T, redis *miniredis.Miniredis, connections int, store metric.StreamMetricStore) *ProviderAdapter {
	t.Helper()

	ctx := context.Background()
	adapter := NewProviderAdapter(ctx, zaptest.NewLogger(t), []string{fmt.Sprintf("redis://%s", redis.Addr())}, false, connections, datasource.ProviderOpts{StreamMetricStore: store}).(*ProviderAdapter)
	require.NoError(t, adapter.Startup(ctx))
	t.Cleanup(func() {
		_ = adapter.Shutdown(ctx)
	})
	return adapter
}

func subscribe(t *testing.T, ctx context.Context, adapter *ProviderAdapter, updater datasource.SubscriptionEventUpdater, channels ...string) {
	t.Helper()
	require.NoError(t, adapter.Subscribe(ctx, &SubscriptionEventConfiguration{
		Provider:  "test-provider",
		Channels:  channels,
		FieldName: "testField",
	}, updater))
}

func TestSharedConnections_ManySubscriptionsUseFixedConnections(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	adapter := newSharedAdapter(t, redis, 4, nil)

	require.Eventually(t, func() bool {
		return redis.CurrentConnectionCount() == 5
	}, 2*time.Second, 10*time.Millisecond, "expected the client connection plus 4 shared connections")

	updaters := make([]*recordingUpdater, 1000)
	for i := range updaters {
		updaters[i] = &recordingUpdater{}
		subscribe(t, context.Background(), adapter, updaters[i], fmt.Sprintf("orders:org-%d", i))
	}

	require.Eventually(t, func() bool {
		return len(redis.PubSubChannels("")) == 1000
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, 5, redis.CurrentConnectionCount())

	redis.Publish("orders:org-42", "for-42")
	require.Eventually(t, func() bool {
		return len(updaters[42].received()) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Empty(t, updaters[41].received())
}

func TestSharedConnections_LiteralsAndPatterns(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	adapter := newSharedAdapter(t, redis, 4, nil)

	updater := &recordingUpdater{}
	subscribe(t, context.Background(), adapter, updater, "orders:org-1", "invoices:*")

	require.Eventually(t, func() bool {
		return redis.PubSubNumSub("orders:org-1")["orders:org-1"] == 1 && redis.PubSubNumPat() == 1
	}, 2*time.Second, 10*time.Millisecond)

	redis.Publish("orders:org-2", "other-org")
	redis.Publish("orders:org-1", "literal")
	redis.Publish("invoices:org-1", "glob")

	require.Eventually(t, func() bool {
		return len(updater.received()) == 2
	}, 2*time.Second, 10*time.Millisecond)
	require.ElementsMatch(t, []string{"literal", "glob"}, updater.received())
}

func TestSharedConnections_SharedChannelUnsubscribesWithLastSubscriber(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	adapter := newSharedAdapter(t, redis, 4, nil)

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelA()
	defer cancelB()
	a, b := &recordingUpdater{}, &recordingUpdater{}
	subscribe(t, ctxA, adapter, a, "orders:org-1")
	subscribe(t, ctxB, adapter, b, "orders:org-1")

	require.Eventually(t, func() bool {
		return redis.PubSubNumSub("orders:org-1")["orders:org-1"] == 1
	}, 2*time.Second, 10*time.Millisecond, "expected one Redis subscription for both subscribers")

	cancelA()
	time.Sleep(50 * time.Millisecond)
	redis.Publish("orders:org-1", "after-a-left")
	require.Eventually(t, func() bool {
		return len(b.received()) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Empty(t, a.received())

	cancelB()
	require.Eventually(t, func() bool {
		return len(redis.PubSubChannels("")) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedConnections_SlowSubscriberDoesNotDelayOthers(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	adapter := newSharedAdapter(t, redis, 1, nil)

	slow := newBlockingUpdater(t)
	fast := &recordingUpdater{}
	subscribe(t, context.Background(), adapter, slow, "orders:org-1")
	subscribe(t, context.Background(), adapter, fast, "orders:org-1")
	require.Eventually(t, func() bool {
		return redis.PubSubNumSub("orders:org-1")["orders:org-1"] == 1
	}, 2*time.Second, 10*time.Millisecond)

	want := make([]string, 3000)
	for i := range want {
		want[i] = fmt.Sprintf("event-%d", i)
		redis.Publish("orders:org-1", want[i])
	}

	require.Eventually(t, func() bool {
		return len(fast.received()) == len(want)
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, want, fast.received())
	require.Empty(t, slow.received())

	slow.release()
	require.Eventually(t, func() bool {
		return len(slow.received()) == len(want)
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, want, slow.received())
}

func TestSharedConnections_DropsOverSubscriberCap(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	store := &droppedCounter{}
	adapter := newSharedAdapter(t, redis, 1, store)

	slow := newBlockingUpdater(t)
	other := &recordingUpdater{}
	subscribe(t, context.Background(), adapter, slow, "orders:org-1")
	subscribe(t, context.Background(), adapter, other, "orders:org-2")
	require.Eventually(t, func() bool {
		return len(redis.PubSubChannels("")) == 2
	}, 2*time.Second, 10*time.Millisecond)

	payload := strings.Repeat("x", 1024*1024)
	const published = 12
	for range published {
		redis.Publish("orders:org-1", payload)
	}
	redis.Publish("orders:org-2", "unaffected")

	require.Eventually(t, func() bool {
		return len(other.received()) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return store.count() >= published-subscriberQueueMaxBytes/len(payload)
	}, 5*time.Second, 10*time.Millisecond)

	slow.release()
	require.Eventually(t, func() bool {
		return len(slow.received())+store.count() == published
	}, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, adapter.queuedBytes.Load())
}

func TestSharedConnections_DropsOverProviderCap(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	store := &droppedCounter{}
	adapter := newSharedAdapter(t, redis, 4, store)

	const subscribers = 10
	slow := make([]*blockingUpdater, subscribers)
	for i := range slow {
		slow[i] = newBlockingUpdater(t)
		channel := fmt.Sprintf("orders:org-%d", i)
		subscribe(t, context.Background(), adapter, slow[i], channel)
		require.Eventually(t, func() bool {
			return len(redis.PubSubChannels(channel)) == 1
		}, 2*time.Second, 10*time.Millisecond)
		redis.Publish(channel, "in-flight")
		require.Eventually(t, slow[i].blocked.Load, 2*time.Second, 10*time.Millisecond)
	}

	// 10 subscribers x 7 MiB stays under each subscriber cap but passes the provider cap.
	payload := strings.Repeat("x", 1024*1024)
	for i := range subscribers {
		for range 7 {
			redis.Publish(fmt.Sprintf("orders:org-%d", i), payload)
		}
	}

	require.Eventually(t, func() bool {
		return store.count() > 0
	}, 5*time.Second, 10*time.Millisecond)
	require.LessOrEqual(t, adapter.queuedBytes.Load(), int64(providerQueueMaxBytes))
}

func TestSharedConnections_RemovedSubscriberReleasesQueuedBytes(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	adapter := newSharedAdapter(t, redis, 1, nil)

	ctx, cancel := context.WithCancel(context.Background())
	slow := newBlockingUpdater(t)
	subscribe(t, ctx, adapter, slow, "orders:org-1")
	require.Eventually(t, func() bool {
		return len(redis.PubSubChannels("")) == 1
	}, 2*time.Second, 10*time.Millisecond)

	redis.Publish("orders:org-1", "in-flight")
	require.Eventually(t, slow.blocked.Load, 2*time.Second, 10*time.Millisecond)
	for range 5 {
		redis.Publish("orders:org-1", "queued")
	}
	require.Eventually(t, func() bool {
		return adapter.queuedBytes.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	slow.release()
	require.Eventually(t, func() bool {
		return adapter.queuedBytes.Load() == 0 && len(redis.PubSubChannels("")) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSharedConnections_RecoversAfterRedisRestart(t *testing.T) {
	t.Parallel()

	redis := miniredis.RunT(t)
	adapter := newSharedAdapter(t, redis, 4, nil)

	redis.Close()

	updater := &recordingUpdater{}
	subscribe(t, context.Background(), adapter, updater, "orders:org-1", "invoices:*")

	require.NoError(t, redis.Restart())

	require.Eventually(t, func() bool {
		return redis.PubSubNumSub("orders:org-1")["orders:org-1"] == 1 && redis.PubSubNumPat() == 1
	}, 10*time.Second, 50*time.Millisecond)

	redis.Publish("orders:org-1", "literal")
	redis.Publish("invoices:org-1", "glob")

	require.Eventually(t, func() bool {
		return len(updater.received()) == 2
	}, 2*time.Second, 10*time.Millisecond)
	require.ElementsMatch(t, []string{"literal", "glob"}, updater.received())
}

func TestSharedConnections_ShutdownStopsAllGoroutines(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	redis := miniredis.NewMiniRedis()
	require.NoError(t, redis.Start())
	defer redis.Close()

	ctx := context.Background()
	adapter := NewProviderAdapter(ctx, zaptest.NewLogger(t), []string{fmt.Sprintf("redis://%s", redis.Addr())}, false, 4, datasource.ProviderOpts{})
	require.NoError(t, adapter.Startup(ctx))

	slow := newBlockingUpdater(t)
	require.NoError(t, adapter.Subscribe(ctx, &SubscriptionEventConfiguration{Provider: "test-provider", Channels: []string{"orders:org-1"}}, slow))
	require.NoError(t, adapter.Subscribe(ctx, &SubscriptionEventConfiguration{Provider: "test-provider", Channels: []string{"orders:org-2"}}, &recordingUpdater{}))
	redis.Publish("orders:org-1", "queued")
	slow.release()

	done := make(chan error, 1)
	go func() { done <- adapter.Shutdown(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return")
	}
}

type steppedUpdater struct {
	entered chan struct{}
	permit  chan struct{}
}

func (u *steppedUpdater) Update([]datasource.StreamEvent) {
	u.entered <- struct{}{}
	<-u.permit
}
func (u *steppedUpdater) Complete()                 {}
func (u *steppedUpdater) Done()                     {}
func (u *steppedUpdater) SetHooks(datasource.Hooks) {}

func waitForUpdater(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("updater not reached")
	}
}

func newSteppedUpdater(t *testing.T) *steppedUpdater {
	u := &steppedUpdater{entered: make(chan struct{}, 100), permit: make(chan struct{})}
	t.Cleanup(func() { close(u.permit) })
	return u
}

func TestSharedConnections_ShutdownDiscardsPendingBatch(t *testing.T) {
	r := miniredis.RunT(t)
	p := newSharedAdapter(t, r, 1, nil)
	u := newSteppedUpdater(t)
	subscribe(t, context.Background(), p, u, "review")
	require.Eventually(t, func() bool { return len(r.PubSubChannels("")) == 1 }, time.Second, time.Millisecond)
	r.Publish("review", "first")
	waitForUpdater(t, u.entered)
	r.Publish("review", "second")
	r.Publish("review", "third")
	require.Eventually(t, func() bool {
		sh := p.shardFor("review")
		sh.mu.Lock()
		defer sh.mu.Unlock()
		for s := range sh.subs["review"] {
			s.mu.Lock()
			n := len(s.queue)
			s.mu.Unlock()
			return n == 2
		}
		return false
	}, time.Second, time.Millisecond)
	u.permit <- struct{}{}
	waitForUpdater(t, u.entered)
	done := make(chan struct{})
	go func() {
		_ = p.Shutdown(context.Background())
		close(done)
	}()
	waitForUpdater(t, p.ctx.Done())
	u.permit <- struct{}{}
	select {
	case <-u.entered:
		t.Fatal("Shutdown cancelled provider, but updater received third event from pending batch")
	case <-done:
		require.Zero(t, p.queuedBytes.Load())
	case <-time.After(time.Second):
		t.Fatal("shutdown stuck")
	}
}

func TestSharedConnections_InFlightBatchesCountTowardLimits(t *testing.T) {
	for _, tc := range []struct {
		name        string
		subscribers int
		batches     int
	}{
		{name: "subscriber", subscribers: 1, batches: 2},
		{name: "provider", subscribers: 12, batches: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &ProviderAdapter{}
			log := zaptest.NewLogger(t)
			var subscribers []*subscriber
			var retained [][]*rd.Message
			var acceptedBytes int64
			var rejected int
			for range tc.subscribers {
				s := &subscriber{notify: make(chan struct{}, 1)}
				subscribers = append(subscribers, s)
				for range tc.batches {
					for range 7 {
						msg := &rd.Message{Channel: "orders", Payload: strings.Repeat("x", 1024*1024)}
						if p.enqueue(s, msg) {
							acceptedBytes += int64(len(msg.Payload)+len(msg.Channel)+len(msg.Pattern)) + messageOverhead
						} else {
							rejected++
						}
					}
					batch, _ := p.take(s)
					retained = append(retained, batch)
				}
			}
			require.Positive(t, rejected, "undelivered batches must prevent accepting messages over the cap")
			require.Equal(t, acceptedBytes, p.queuedBytes.Load())
			require.LessOrEqual(t, acceptedBytes, int64(providerQueueMaxBytes))
			var retainedBytes int64
			for _, batch := range retained {
				for _, msg := range batch {
					retainedBytes += int64(len(msg.Payload)+len(msg.Channel)+len(msg.Pattern)) + messageOverhead
				}
			}
			require.Equal(t, acceptedBytes, retainedBytes)
			for _, s := range subscribers {
				require.LessOrEqual(t, s.bytes, int64(subscriberQueueMaxBytes))
				p.removeSubscriber(nil, s, log)
				require.Zero(t, s.bytes)
			}
			require.Zero(t, p.queuedBytes.Load(), "cancellation must release unfinished batches")
		})
	}
}
