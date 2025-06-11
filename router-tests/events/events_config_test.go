package events_test

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/wundergraph/cosmo/router-tests/testenv"
	"github.com/wundergraph/cosmo/router/pkg/config"
	"testing"
)

func TestEventsConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Run("kafka provider not specified in the router configuration", func(t *testing.T) {
		err := testenv.RunWithError(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsJSONTemplate,
			EnableNats:               true,
			EnableKafka:              false,
			ModifyEventsConfiguration: func(eventsConfiguration *config.EventsConfiguration) {
				eventsConfiguration.Providers.Kafka = nil
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			assert.Fail(t, "should not be called")
		})
		assert.ErrorContains(t, err, "failed to find Kafka provider with ID")
	})

	t.Run("nats provider not specified in the router configuration", func(t *testing.T) {
		err := testenv.RunWithError(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsJSONTemplate,
			EnableNats:               false,
			EnableKafka:              true,
			ModifyEventsConfiguration: func(eventsConfiguration *config.EventsConfiguration) {
				eventsConfiguration.Providers.Nats = nil
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			assert.Fail(t, "should not be called")
		})
		assert.ErrorContains(t, err, "failed to find Nats provider with ID")
	})

	t.Run("rabbitmq provider not specified in the router configuration", func(t *testing.T) {
		err := testenv.RunWithError(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsJSONTemplate,
			EnableNats:               true,
			EnableKafka:              true,
			ModifyEventsConfiguration: func(eventsConfiguration *config.EventsConfiguration) {
				eventsConfiguration.Providers.RabbitMQ = nil
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			assert.Fail(t, "should not be called")
		})
		assert.ErrorContains(t, err, "failed to find RabbitMQ provider with ID")
	})

	t.Run("rabbitmq provider correctly specified in the router configuration", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfiguration *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				assert.NotEmpty(t, eventsConfiguration.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfiguration.Providers.RabbitMQ {
					eventsConfiguration.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			// Verify that the RabbitMQ connection is established
			assert.NotNil(t, xEnv.RabbitMQConnection, "RabbitMQ connection should not be nil")

			// Test publishing a message to verify the connection works
			ch, err := xEnv.RabbitMQConnection.Channel()
			assert.NoError(t, err, "Should be able to create a channel")
			defer ch.Close()

			queueName := xEnv.GetPubSubName("test-queue")
			q, err := ch.QueueDeclare(
				queueName, // name
				true,      // durable
				false,     // delete when unused
				false,     // exclusive
				false,     // no-wait
				nil,       // arguments
			)
			assert.NoError(t, err, "Should be able to declare a queue")

			// Publish a message
			err = ch.PublishWithContext(
				context.Background(),
				"",     // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte("test message"),
				},
			)
			assert.NoError(t, err, "Should be able to publish a message")
		})
	})

	t.Run("rabbitmq provider with invalid URL", func(t *testing.T) {
		err := testenv.RunWithError(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfiguration *config.EventsConfiguration) {
				// Set an invalid URL for the RabbitMQ provider
				for i := range eventsConfiguration.Providers.RabbitMQ {
					eventsConfiguration.Providers.RabbitMQ[i].URL = "amqp://invalid:5672/"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			assert.Fail(t, "should not be called")
		})
		assert.ErrorContains(t, err, "failed to connect to RabbitMQ")
	})
}
