package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wundergraph/cosmo/router/pkg/pubsub"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/datasource/pubsub_datasource"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"go.uber.org/zap"
)

// RabbitMQConnector is the interface for creating RabbitMQ pubsub instances
type RabbitMQConnector interface {
	// New creates a new RabbitMQ pubsub instance
	New(ctx context.Context) RabbitMQPubSub
}

// RabbitMQPubSub is the interface for RabbitMQ pubsub operations
type RabbitMQPubSub interface {
	// Subscribe subscribes to the given queues and updates the subscription updater
	Subscribe(ctx context.Context, event pubsub_datasource.RabbitMQSubscriptionEventConfiguration, updater resolve.SubscriptionUpdater) error
	// Publish publishes the given event to the RabbitMQ queue
	Publish(ctx context.Context, event pubsub_datasource.RabbitMQPublishEventConfiguration) error
}

var (
	_ RabbitMQConnector = (*connector)(nil)
	_ RabbitMQPubSub    = (*rabbitMQPubSub)(nil)
	_ pubsub.Lifecycle  = (*rabbitMQPubSub)(nil)

	errChannelClosed = errors.New("channel closed")
)

type connector struct {
	conn   *amqp.Connection
	logger *zap.Logger
}

func NewConnector(logger *zap.Logger, url string) (RabbitMQConnector, error) {

	conn, err := amqp.Dial(url)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	return &connector{
		conn:   conn,
		logger: logger,
	}, nil
}

func (c *connector) New(ctx context.Context) RabbitMQPubSub {
	ctx, cancel := context.WithCancel(ctx)

	ps := &rabbitMQPubSub{
		ctx:      ctx,
		logger:   c.logger.With(zap.String("pubsub", "rabbitmq")),
		conn:     c.conn,
		closeWg:  sync.WaitGroup{},
		cancel:   cancel,
		channels: make(map[string]*amqp.Channel),
		mu:       sync.Mutex{},
	}

	return ps
}

// rabbitMQPubSub is a RabbitMQ pubsub implementation.
// It uses the amqp091-go RabbitMQ client to consume and produce messages.
// The pubsub is stateless and does not store any messages.
// It uses a channel per queue to consume messages and a channel for publishing messages.
type rabbitMQPubSub struct {
	ctx      context.Context
	logger   *zap.Logger
	conn     *amqp.Connection
	closeWg  sync.WaitGroup
	cancel   context.CancelFunc
	channels map[string]*amqp.Channel
	mu       sync.Mutex
}

// getChannel returns a channel for the given queue name, creating it if it doesn't exist
func (p *rabbitMQPubSub) getChannel(queueName string) (*amqp.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ch, ok := p.channels[queueName]; ok {
		return ch, nil
	}

	ch, err := p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	p.channels[queueName] = ch
	return ch, nil
}

// queueConsumer consumes messages from a RabbitMQ queue and calls the updateTriggers function
func (p *rabbitMQPubSub) queueConsumer(ctx context.Context, queueName string, updater resolve.SubscriptionUpdater) error {
	ch, err := p.getChannel(queueName)
	if err != nil {
		return err
	}

	// Declare the queue to ensure it exists
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Set up the consumer
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	for {
		select {
		case <-p.ctx.Done(): // Close the consumer if the application context was canceled
			return p.ctx.Err()
		case <-ctx.Done(): // Close the consumer if the subscription context was canceled
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return errChannelClosed
			}
			p.logger.Debug("subscription update", zap.String("queue", queueName), zap.ByteString("data", msg.Body))
			updater.Update(msg.Body)
		}
	}
}

// Subscribe subscribes to the given queues and updates the subscription updater
func (p *rabbitMQPubSub) Subscribe(ctx context.Context, event pubsub_datasource.RabbitMQSubscriptionEventConfiguration, updater resolve.SubscriptionUpdater) error {
	log := p.logger.With(
		zap.String("provider_id", event.ProviderID),
		zap.String("method", "subscribe"),
		zap.Strings("queues", event.Queues),
	)

	log.Debug("subscribe")

	for _, queueName := range event.Queues {
		queueName := queueName // Create a new variable for the goroutine

		p.closeWg.Add(1)

		go func() {
			defer p.closeWg.Done()

			err := p.queueConsumer(ctx, queueName, updater)
			if err != nil {
				if errors.Is(err, errChannelClosed) || errors.Is(err, context.Canceled) {
					log.Debug("consumer canceled", zap.Error(err))
				} else {
					log.Error("consumer error", zap.Error(err))
				}
				return
			}
		}()
	}

	return nil
}

// Publish publishes the given event to the RabbitMQ queue in a non-blocking way
func (p *rabbitMQPubSub) Publish(ctx context.Context, event pubsub_datasource.RabbitMQPublishEventConfiguration) error {
	log := p.logger.With(
		zap.String("provider_id", event.ProviderID),
		zap.String("method", "publish"),
		zap.String("queue", event.Queue),
	)

	log.Debug("publish", zap.ByteString("data", event.Data))

	ch, err := p.getChannel(event.Queue)
	if err != nil {
		log.Error("failed to get channel", zap.Error(err))
		return pubsub.NewError(fmt.Sprintf("error getting channel for queue %s", event.Queue), err)
	}

	// Declare the queue to ensure it exists
	q, err := ch.QueueDeclare(
		event.Queue, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Error("failed to declare queue", zap.Error(err))
		return pubsub.NewError(fmt.Sprintf("error declaring queue %s", event.Queue), err)
	}

	// Set a timeout for the publish operation
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Publish the message
	err = ch.PublishWithContext(
		publishCtx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        event.Data,
		},
	)
	if err != nil {
		log.Error("publish error", zap.Error(err))
		return pubsub.NewError(fmt.Sprintf("error publishing to RabbitMQ queue %s", event.Queue), err)
	}

	return nil
}

// Shutdown closes all channels and the connection
func (p *rabbitMQPubSub) Shutdown(ctx context.Context) error {
	// Cancel the context to stop all consumers
	p.cancel()

	// Wait until all consumers are closed
	p.closeWg.Wait()

	// Close all channels
	p.mu.Lock()
	for queueName, ch := range p.channels {
		if err := ch.Close(); err != nil {
			p.logger.Error("error closing channel", zap.Error(err), zap.String("queue", queueName))
		}
	}
	p.mu.Unlock()

	// Close the connection
	if err := p.conn.Close(); err != nil {
		p.logger.Error("error closing connection", zap.Error(err))
		return err
	}

	return nil
}
