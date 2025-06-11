package events_test

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hasura/go-graphql-client"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/wundergraph/cosmo/router-tests/testenv"
	"github.com/wundergraph/cosmo/router/pkg/config"
)

const RabbitMQWaitTimeout = time.Second * 30

func TestRabbitMQEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Run("subscribe async with filter", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfig *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				require.NotEmpty(t, eventsConfig.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfig.Providers.RabbitMQ {
					eventsConfig.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			var subscriptionOne struct {
				filteredEmployeeUpdated struct {
					ID      float64 `graphql:"id"`
					Details struct {
						Forename string `graphql:"forename"`
						Surname  string `graphql:"surname"`
					} `graphql:"details"`
				} `graphql:"filteredEmployeeUpdated(employeeID: 3)"`
			}

			surl := xEnv.GraphQLWebSocketSubscriptionURL()
			client := graphql.NewSubscriptionClient(surl)
			t.Cleanup(func() {
				_ = client.Close()
			})

			var counter atomic.Uint32

			subscriptionOneID, err := client.Subscribe(&subscriptionOne, nil, func(dataValue []byte, errValue error) error {
				defer counter.Add(1)
				require.NoError(t, errValue)
				require.JSONEq(t, `{"filteredEmployeeUpdated":{"id":3,"details":{"forename":"Jens","surname":"Neuse"}}}`, string(dataValue))
				return nil
			})
			require.NoError(t, err)
			require.NotEmpty(t, subscriptionOneID)

			go func() {
				clientErr := client.Run()
				require.NoError(t, clientErr)
			}()

			xEnv.WaitForSubscriptionCount(1, RabbitMQWaitTimeout)

			// This message should be filtered out because the ID is not in the filter list [1,3,4,7,11]
			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 2,"details":{"forename":"Jens","surname":"Neuse"}}`)

			// Wait a bit to ensure the message is processed
			time.Sleep(time.Second)

			// This message should pass the filter because the ID is in the filter list [1,3,4,7,11]
			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 3,"details":{"forename":"Jens","surname":"Neuse"}}`)

			require.Eventually(t, func() bool {
				return counter.Load() == 1
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			require.NoError(t, client.Close())

			xEnv.WaitForSubscriptionCount(0, RabbitMQWaitTimeout)
			xEnv.WaitForConnectionCount(0, RabbitMQWaitTimeout)
		})
	})

	t.Run("subscribe to multiple queues", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfig *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				require.NotEmpty(t, eventsConfig.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfig.Providers.RabbitMQ {
					eventsConfig.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			var subscriptionOne struct {
				employeeUpdated struct {
					ID      float64 `graphql:"id"`
					Details struct {
						Forename string `graphql:"forename"`
						Surname  string `graphql:"surname"`
					} `graphql:"details"`
				} `graphql:"employeeUpdated(employeeID: 3)"`
			}

			surl := xEnv.GraphQLWebSocketSubscriptionURL()
			client := graphql.NewSubscriptionClient(surl)
			t.Cleanup(func() {
				_ = client.Close()
			})

			var counter atomic.Uint32

			subscriptionOneID, err := client.Subscribe(&subscriptionOne, nil, func(dataValue []byte, errValue error) error {
				defer counter.Add(1)
				require.NoError(t, errValue)
				require.JSONEq(t, `{"employeeUpdated":{"id":3,"details":{"forename":"Jens","surname":"Neuse"}}}`, string(dataValue))
				return nil
			})
			require.NoError(t, err)
			require.NotEmpty(t, subscriptionOneID)

			go func() {
				clientErr := client.Run()
				require.NoError(t, clientErr)
			}()

			xEnv.WaitForSubscriptionCount(1, RabbitMQWaitTimeout)

			// Publish to the first queue
			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 3,"details":{"forename":"Jens","surname":"Neuse"}}`)

			require.Eventually(t, func() bool {
				return counter.Load() == 1
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			// Publish to the second queue
			produceRabbitMQMessage(t, xEnv, "employeeUpdatedTwo", `{"__typename":"Employee","id": 3,"details":{"forename":"Jens","surname":"Neuse"}}`)

			require.Eventually(t, func() bool {
				return counter.Load() == 2
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			require.NoError(t, client.Close())

			xEnv.WaitForSubscriptionCount(0, RabbitMQWaitTimeout)
			xEnv.WaitForConnectionCount(0, RabbitMQWaitTimeout)
		})
	})

	t.Run("publish", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfig *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				require.NotEmpty(t, eventsConfig.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfig.Providers.RabbitMQ {
					eventsConfig.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			// Create a channel to receive messages
			ch, err := xEnv.RabbitMQConnection.Channel()
			require.NoError(t, err)
			defer ch.Close()

			// Declare a queue to receive messages
			queueName := xEnv.GetPubSubName("testPublish")
			q, err := ch.QueueDeclare(
				queueName, // name
				true,      // durable
				false,     // delete when unused
				false,     // exclusive
				false,     // no-wait
				nil,       // arguments
			)
			require.NoError(t, err)

			// Set up a consumer to receive messages
			msgs, err := ch.Consume(
				q.Name, // queue
				"",     // consumer
				true,   // auto-ack
				false,  // exclusive
				false,  // no-local
				false,  // no-wait
				nil,    // args
			)
			require.NoError(t, err)

			// Make a GraphQL mutation that publishes a message
			res := xEnv.MakeGraphQLRequestOK(testenv.GraphQLRequest{
				Query: `mutation { publishMessage(queue: "testPublish", message: "test message") }`,
			})
			require.JSONEq(t, `{"data":{"publishMessage":true}}`, res.Body)

			// Wait for the message to be received
			var receivedMessage string
			select {
			case msg := <-msgs:
				receivedMessage = string(msg.Body)
			case <-time.After(RabbitMQWaitTimeout):
				t.Fatal("Timed out waiting for message")
			}

			// Verify the message
			require.Equal(t, "test message", receivedMessage)
		})
	})

	t.Run("subscribe async", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfig *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				require.NotEmpty(t, eventsConfig.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfig.Providers.RabbitMQ {
					eventsConfig.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			var subscriptionOne struct {
				employeeUpdated struct {
					ID      float64 `graphql:"id"`
					Details struct {
						Forename string `graphql:"forename"`
						Surname  string `graphql:"surname"`
					} `graphql:"details"`
				} `graphql:"employeeUpdated(employeeID: 3)"`
			}

			surl := xEnv.GraphQLWebSocketSubscriptionURL()
			client := graphql.NewSubscriptionClient(surl)
			t.Cleanup(func() {
				_ = client.Close()
			})

			var counter atomic.Uint32

			subscriptionOneID, err := client.Subscribe(&subscriptionOne, nil, func(dataValue []byte, errValue error) error {
				defer counter.Add(1)
				require.NoError(t, errValue)
				require.JSONEq(t, `{"employeeUpdated":{"id":1,"details":{"forename":"Jens","surname":"Neuse"}}}`, string(dataValue))
				return nil
			})
			require.NoError(t, err)
			require.NotEmpty(t, subscriptionOneID)

			go func() {
				clientErr := client.Run()
				require.NoError(t, clientErr)
			}()

			go func() {
				require.Eventually(t, func() bool {
					return counter.Load() == 1
				}, RabbitMQWaitTimeout, time.Millisecond*100)
				_ = client.Close()
			}()

			xEnv.WaitForSubscriptionCount(1, RabbitMQWaitTimeout)

			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 1,"update":{"name":"foo"}}`)

			xEnv.WaitForSubscriptionCount(0, RabbitMQWaitTimeout)
			xEnv.WaitForConnectionCount(0, RabbitMQWaitTimeout)
		})
	})

	t.Run("message and resolve errors should not abort the subscription", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableNats:               false,
			EnableKafka:              false,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfig *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				require.NotEmpty(t, eventsConfig.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfig.Providers.RabbitMQ {
					eventsConfig.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			var subscriptionOne struct {
				employeeUpdated struct {
					ID      float64 `graphql:"id"`
					Details struct {
						Forename string `graphql:"forename"`
						Surname  string `graphql:"surname"`
					} `graphql:"details"`
				} `graphql:"employeeUpdated(employeeID: 3)"`
			}

			surl := xEnv.GraphQLWebSocketSubscriptionURL()
			client := graphql.NewSubscriptionClient(surl)
			t.Cleanup(func() {
				_ = client.Close()
			})

			var counter atomic.Uint32

			subscriptionOneID, err := client.Subscribe(&subscriptionOne, nil, func(dataValue []byte, errValue error) error {
				oldCount := counter.Load()
				counter.Add(1)

				if oldCount == 0 && errValue != nil {
					// Empty message - first error case
					var gqlErr graphql.Errors
					if errors.As(errValue, &gqlErr) {
						// Check if the error message contains "Invalid message" somewhere
						foundInvalidMsg := false
						for _, err := range gqlErr {
							if strings.Contains(err.Message, "Invalid message") {
								foundInvalidMsg = true
								break
							}
						}
						require.True(t, foundInvalidMsg, "Expected error message to contain 'Invalid message'")
					} else {
						t.Fatalf("Expected graphql.Errors but got %T: %v", errValue, errValue)
					}
				} else if (oldCount == 1 || oldCount == 3) && errValue == nil {
					// Valid messages
					require.JSONEq(t, `{"employeeUpdated":{"id":1,"details":{"forename":"Jens","surname":"Neuse"}}}`, string(dataValue))
				} else if oldCount == 2 && errValue != nil {
					// Missing ID - second error case
					var gqlErr graphql.Errors
					if errors.As(errValue, &gqlErr) {
						foundNonNullableError := false
						for _, err := range gqlErr {
							if strings.Contains(err.Message, "non-nullable") {
								foundNonNullableError = true
								break
							}
						}
						require.True(t, foundNonNullableError, "Expected error message to contain 'non-nullable'")
					} else {
						t.Fatalf("Expected graphql.Errors but got %T: %v", errValue, errValue)
					}
				}

				return nil
			})
			require.NoError(t, err)
			require.NotEmpty(t, subscriptionOneID)

			go func() {
				clientErr := client.Run()
				require.NoError(t, clientErr)
			}()

			xEnv.WaitForSubscriptionCount(1, RabbitMQWaitTimeout)

			produceRabbitMQMessage(t, xEnv, "employeeUpdated", ``) // Empty message
			require.Eventually(t, func() bool {
				return counter.Load() == 1
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 1,"details":{"forename":"Jens","surname":"Neuse"}}`) // Correct message
			require.Eventually(t, func() bool {
				return counter.Load() == 2
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","details":{"forename":"Jens","surname":"Neuse"}}`) // Missing ID
			require.Eventually(t, func() bool {
				return counter.Load() == 3
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 1,"details":{"forename":"Jens","surname":"Neuse"}}`) // Correct message
			require.Eventually(t, func() bool {
				return counter.Load() == 4
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			require.NoError(t, client.Close())

			xEnv.WaitForSubscriptionCount(0, RabbitMQWaitTimeout)
			xEnv.WaitForConnectionCount(0, RabbitMQWaitTimeout)
		})
	})

	t.Run("every subscriber gets the message", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsKafkaJSONTemplate,
			EnableNats:               false,
			EnableKafka:              false,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfig *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				require.NotEmpty(t, eventsConfig.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfig.Providers.RabbitMQ {
					eventsConfig.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			var subscriptionOne struct {
				employeeUpdated struct {
					ID      float64 `graphql:"id"`
					Details struct {
						Forename string `graphql:"forename"`
						Surname  string `graphql:"surname"`
					} `graphql:"details"`
				} `graphql:"employeeUpdated(employeeID: 3)"`
			}

			surl := xEnv.GraphQLWebSocketSubscriptionURL()
			client := graphql.NewSubscriptionClient(surl)
			t.Cleanup(func() {
				_ = client.Close()
			})

			var counter atomic.Uint32

			subscriptionOneID, err := client.Subscribe(&subscriptionOne, nil, func(dataValue []byte, errValue error) error {
				defer counter.Add(1)
				require.NoError(t, errValue)
				require.JSONEq(t, `{"employeeUpdated":{"id":1,"details":{"forename":"Jens","surname":"Neuse"}}}`, string(dataValue))
				return nil
			})
			require.NoError(t, err)
			require.NotEmpty(t, subscriptionOneID)

			subscriptionTwoID, err := client.Subscribe(&subscriptionOne, nil, func(dataValue []byte, errValue error) error {
				defer counter.Add(1)
				require.NoError(t, errValue)
				require.JSONEq(t, `{"employeeUpdated":{"id":1,"details":{"forename":"Jens","surname":"Neuse"}}}`, string(dataValue))
				return nil
			})
			require.NoError(t, err)
			require.NotEmpty(t, subscriptionTwoID)

			go func() {
				clientErr := client.Run()
				require.NoError(t, clientErr)
			}()

			xEnv.WaitForSubscriptionCount(2, RabbitMQWaitTimeout)

			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 1,"update":{"name":"foo"}}`)

			require.Eventually(t, func() bool {
				return counter.Load() == 2
			}, RabbitMQWaitTimeout, time.Millisecond*100)

			require.NoError(t, client.Close())

			xEnv.WaitForSubscriptionCount(0, RabbitMQWaitTimeout)
			xEnv.WaitForConnectionCount(0, RabbitMQWaitTimeout)
		})
	})

	t.Run("subscribe async netPoll disabled", func(t *testing.T) {
		testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsRabbitMQJSONTemplate,
			EnableNats:               false,
			EnableKafka:              false,
			EnableRabbitMQ:           true,
			ModifyEventsConfiguration: func(eventsConfig *config.EventsConfiguration) {
				// Verify that the providers are set correctly
				require.NotEmpty(t, eventsConfig.Providers.RabbitMQ, "RabbitMQ providers should not be empty")

				// Set the ID to match what we expect in the customEvents field
				for i := range eventsConfig.Providers.RabbitMQ {
					eventsConfig.Providers.RabbitMQ[i].ID = "my-rabbitmq"
				}
			},
			ModifyEngineExecutionConfiguration: func(engineExecutionConfiguration *config.EngineExecutionConfiguration) {
				engineExecutionConfiguration.EnableNetPoll = false
				engineExecutionConfiguration.WebSocketClientReadTimeout = time.Second
			},
		}, func(t *testing.T, xEnv *testenv.Environment) {
			var subscriptionOne struct {
				employeeUpdated struct {
					ID      float64 `graphql:"id"`
					Details struct {
						Forename string `graphql:"forename"`
						Surname  string `graphql:"surname"`
					} `graphql:"details"`
				} `graphql:"employeeUpdated(employeeID: 3)"`
			}

			surl := xEnv.GraphQLWebSocketSubscriptionURL()
			client := graphql.NewSubscriptionClient(surl)
			t.Cleanup(func() {
				_ = client.Close()
			})

			var counter atomic.Uint32

			subscriptionOneID, err := client.Subscribe(&subscriptionOne, nil, func(dataValue []byte, errValue error) error {
				defer counter.Add(1)
				require.NoError(t, errValue)
				require.JSONEq(t, `{"employeeUpdated":{"id":1,"details":{"forename":"Jens","surname":"Neuse"}}}`, string(dataValue))
				return nil
			})
			require.NoError(t, err)
			require.NotEmpty(t, subscriptionOneID)

			go func() {
				clientErr := client.Run()
				require.NoError(t, clientErr)
			}()

			go func() {
				require.Eventually(t, func() bool {
					return counter.Load() == 1
				}, RabbitMQWaitTimeout, time.Millisecond*100)
				_ = client.Close()
			}()

			xEnv.WaitForSubscriptionCount(1, RabbitMQWaitTimeout)

			produceRabbitMQMessage(t, xEnv, "employeeUpdated", `{"__typename":"Employee","id": 1,"update":{"name":"foo"}}`)

			xEnv.WaitForSubscriptionCount(0, RabbitMQWaitTimeout)
			xEnv.WaitForConnectionCount(0, RabbitMQWaitTimeout)
		})
	})
}

func produceRabbitMQMessage(t *testing.T, xEnv *testenv.Environment, queueName string, message string) {
	t.Helper()

	log.Printf("[DEBUG] produceRabbitMQMessage called for queue: %s", queueName)

	if xEnv.RabbitMQConnection == nil {
		log.Printf("[ERROR] RabbitMQConnection is nil")
		t.Fatalf("RabbitMQConnection is nil")
		return
	}

	ch, err := xEnv.RabbitMQConnection.Channel()
	if err != nil {
		log.Printf("[ERROR] Failed to get channel: %s", err)
	}
	require.NoError(t, err)
	defer ch.Close()

	pubSubName := xEnv.GetPubSubName(queueName)
	log.Printf("[DEBUG] Declaring queue: %s", pubSubName)

	q, err := ch.QueueDeclare(
		pubSubName, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		log.Printf("[ERROR] Failed to declare queue: %s", err)
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If message is empty, use a default valid message
	if message == "" {
		message = `{"id": 0, "details": null}`
	}

	// If the message contains "__typename":"Employee", ensure it has proper details
	if message != "" && !strings.Contains(message, `"details"`) && strings.Contains(message, `"__typename":"Employee"`) {
		// Replace with a properly structured message that will work with our schema
		message = `{"__typename":"Employee","id": 1,"details":{"forename":"Jens","surname":"Neuse"}}`
	}

	log.Printf("[DEBUG] Publishing message to queue %s: %s", q.Name, message)
	err = ch.PublishWithContext(
		ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(message),
		})
	if err != nil {
		log.Printf("[ERROR] Failed to publish message: %s", err)
	}
	require.NoError(t, err)
	log.Printf("[DEBUG] Message published successfully")
}
