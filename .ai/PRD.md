# Product Requirements Document: RabbitMQ Pub/Sub Provider for Cosmo Router

**Version:** 1.1
**Date:** 2024-07-26
**Author:** Captain Picard (AI Assistant)
**Status:** Draft

## 1. Introduction

This document outlines the requirements for integrating RabbitMQ as a supported Pub/Sub provider within the WunderGraph Cosmo Router. The goal is to enable Cosmo users to leverage RabbitMQ for GraphQL Subscription operations, complementing the existing NATS and Kafka providers. This integration will adhere to the established patterns and quality standards within the Cosmo codebase.

## 2. Goals

*   Provide Cosmo Router users with the option to use RabbitMQ for GraphQL Subscriptions.
*   Ensure the RabbitMQ integration follows the established architectural patterns for Pub/Sub providers within the Cosmo Router.
*   Maintain high standards of code quality, test coverage, and reliability consistent with existing providers.
*   Enable seamless configuration and operation of the RabbitMQ provider.

## 3. Scope

### 3.1. In Scope

*   Implementation of a new Go package `router/pkg/pubsub/rabbitmq` containing the RabbitMQ provider logic.
*   Implementation of the necessary interfaces (`pubsub.Lifecycle`, `pubsub_datasource.RabbitMQConnector`, `pubsub_datasource.RabbitMQPubSub`) based on the patterns derived from NATS and Kafka providers (`.cursor/rules/go-rules/`).
*   Support for GraphQL Publish operations mapping to RabbitMQ message publishing.
*   Support for GraphQL Subscribe operations mapping to RabbitMQ message consumption.
*   Integration with the router's configuration system to allow users to specify RabbitMQ connection details (e.g., AMQP URL, vhost, credentials).
*   Implementation of graceful shutdown logic for RabbitMQ connections and consumers.
*   Standardized error handling using `pubsub.NewError`.
*   Addition of RabbitMQ support to the integration test environment (`router-tests/testenv`).
*   Creation of comprehensive integration tests for the RabbitMQ provider (`router-tests/events/rabbitmq_events_test.go`), mirroring existing provider tests.
*   Addition of RabbitMQ test cases to existing relevant test suites (e.g., `websocket_test.go`, `events_config_test.go`).
*   Necessary additions or modifications within the `graphql-go-tools` submodule to support the RabbitMQ datasource type (e.g., defining `RabbitMQConnector`, `RabbitMQPubSub`, event configuration structs).

### 3.2. Out of Scope

*   Implementation of the Request-Reply pattern for RabbitMQ (as Kafka also doesn't support it in this context, and it adds significant complexity).
*   Advanced RabbitMQ features beyond basic Publish/Subscribe for GraphQL Subscriptions (e.g., complex routing topologies, priority queues, federation, shovels) unless strictly required by the core pattern.
*   UI changes in Cosmo Studio to manage or monitor RabbitMQ specifics.
*   Performance optimization beyond ensuring reasonable baseline performance comparable to existing providers.

## 4. Requirements

### 4.1. Functional Requirements

| ID    | Requirement                                                                                                | Details                                                                                                                                                                                                                         |
| :---- | :--------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| FR-01 | Implement RabbitMQ Provider Structure                                                                      | Create `rabbitmq.go` within `router/pkg/pubsub/rabbitmq/`. Define `connector` and `rabbitmqPubSub` structs following the established pattern.                                                                            |
| FR-02 | Implement Factory (`RabbitMQConnector`)                                                                    | Implement the `NewConnector` function and the `connector.New` method. It should receive necessary configuration (likely `amqp091-go` config options or a connection string/struct) and logger.                       |
| FR-03 | Implement Pub/Sub Logic (`RabbitMQPubSub`)                                                                 | Implement `Publish` and `Subscribe` methods.                                                                                                                                                                                    |
| FR-04 | Implement Publish Operation                                                                                | The `Publish` method must accept `pubsub_datasource.RabbitMQPublishEventConfiguration`, connect to RabbitMQ (if not already), and publish the message to the appropriate exchange/routing key based on `event` data.         |
| FR-05 | Implement Subscribe Operation                                                                              | The `Subscribe` method must accept `pubsub_datasource.RabbitMQSubscriptionEventConfiguration` and `resolve.SubscriptionUpdater`. It must establish a consumer on the appropriate queue(s), launch goroutine(s) to receive messages, call `updater.Update()` for each message, and handle message acknowledgements (`ack`/`nack`). |
| FR-06 | Implement Lifecycle Management (`Lifecycle`)                                                               | Implement the `Shutdown` method. It must gracefully close RabbitMQ connections/channels, stop consumer goroutines, and wait for completion using `sync.WaitGroup`. Handle context cancellation.                         |
| FR-07 | Implement Configuration Handling                                                                           | Integrate with the router's upstream configuration mechanism to accept RabbitMQ connection parameters (URL, vhost, user, password, etc.) and pass them appropriately to the `NewConnector` function.                 |
| FR-08 | Implement Error Handling                                                                                   | Wrap all errors returned by the provider using `pubsub.NewError`, providing relevant public and internal error details.                                                                                                 |
| FR-09 | Implement Concurrency Management                                                                           | Use goroutines for subscription consumers. Ensure thread-safe access to shared resources (e.g., connection/channel pools if used). Use `sync.WaitGroup` for managing goroutine lifecycles during shutdown.               |
| FR-10 | Define/Adapt `graphql-go-tools` Types                                                                      | Ensure necessary types like `pubsub_datasource.RabbitMQConnector`, `pubsub_datasource.RabbitMQPubSub`, `pubsub_datasource.RabbitMQPublishEventConfiguration`, `pubsub_datasource.RabbitMQSubscriptionEventConfiguration` exist or are added/adapted within the `graphql-go-tools` submodule's `pubsub_datasource` package. |

### 4.2. Non-Functional Requirements

| ID    | Requirement             | Details                                                                                                                                  |
| :---- | :---------------------- | :--------------------------------------------------------------------------------------------------------------------------------------- |
| NFR-01 | Code Quality           | Adhere to project Go coding standards, linting rules, and established patterns documented in `.cursor/rules/`.                           |
| NFR-02 | Test Coverage          | Achieve test coverage parity with NATS and Kafka providers, including unit tests (if applicable) and comprehensive integration tests. |
| NFR-03 | Reliability            | Ensure stable connection handling, error recovery (where feasible within the pattern), and graceful shutdown.                           |
| NFR-04 | Observability          | Integrate with the existing `*zap.Logger` passed during initialization for structured logging.                                           |
| NFR-05 | Dependency Management  | Use the official `rabbitmq/amqp091-go` library. Manage dependencies using Go modules.                                                     |

## 5. Dependencies

*   Go Programming Language (version compatible with Cosmo)
*   `rabbitmq/amqp091-go` library
*   Access to a RabbitMQ instance for integration testing
*   Existing Cosmo Router codebase and `graphql-go-tools` submodule

## 6. Open Questions

*   Are there specific RabbitMQ exchange/queue naming conventions or topology expectations within Cosmo? (Assume basic direct/topic exchange for now).
*   What specific `amqp091-go` configuration options should be exposed to the user? (Start with AMQP URL).
*   Are there existing mechanisms in `testenv` for managing AMQP connections/topology during tests?

## 7. User Story Backlog (Ordered)

| ID    | User Story                                                                                                                                                 | Type         | Notes                                                                                                                  | Priority |
| :---- | :--------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------- | :--------------------------------------------------------------------------------------------------------------------- | :------- |
| **US-R1** | As a Cosmo Router Developer, I need to research and understand Cosmo's implementation of GraphQL Subscriptions and Federation patterns.                     | Research     | Review existing NATS/Kafka code, `graphql-go-tools` interaction, relevant documentation/examples.                       | **Highest**  |
| **US-R2** | As a Cosmo Router Developer, I need to research RabbitMQ concepts (exchanges, queues, bindings, acknowledgements) and the `rabbitmq/amqp091-go` library. | Research     | Understand library usage for connecting, publishing, consuming, acknowledging, error handling, channel management.     | **Highest**  |
| US-11 | As a Cosmo Router Developer, I want to define or adapt the necessary RabbitMQ types within the `graphql-go-tools` submodule.                                | Feature      | Add `RabbitMQConnector`, `RabbitMQPubSub`, event configurations in `pubsub_datasource` if they don't exist implicitly. | High     |
| US-01 | As a Cosmo Router Developer, I want to define the necessary RabbitMQ provider structs (`connector`, `rabbitmqPubSub`) following the pattern.                | Feature      | Create `router/pkg/pubsub/rabbitmq/rabbitmq.go`. Implement required interfaces stubbed out.                      | Medium   |
| US-02 | As a Cosmo Router Developer, I want to implement the `NewConnector` factory function for RabbitMQ.                                                          | Feature      | Accept config/logger, prepare for `New` method based on research.                                                      | Medium   |
| US-06 | As a Cosmo Router Developer, I want to integrate RabbitMQ configuration loading into the router's main setup.                                               | Feature      | Adapt upstream config loading to recognize and pass RabbitMQ options.                                                  | Medium   |
| US-03 | As a Cosmo Router Developer, I want to implement the `Publish` method for RabbitMQ using `amqp091-go`.                                                      | Feature      | Handle connection/channel, publish message based on event config.                                                    | Medium   |
| US-04 | As a Cosmo Router Developer, I want to implement the `Subscribe` method for RabbitMQ using `amqp091-go`.                                                    | Feature      | Handle connection/channel, start consumer goroutine(s), use `SubscriptionUpdater`, handle acknowledgements.             | Medium   |
| US-05 | As a Cosmo Router Developer, I want to implement the `Shutdown` method for RabbitMQ.                                                                        | Feature      | Graceful close of connections/channels, wait for consumer goroutines via WaitGroup.                                  | Medium   |
| US-07 | As a Cosmo Router Developer, I want to add RabbitMQ container setup to `router-tests/testenv`.                                                               | Testing      | Use Docker to run RabbitMQ for tests, provide helpers (`xEnv`).                                                        | Medium   |
| US-08 | As a Cosmo Router Developer, I want to create `router-tests/events/rabbitmq_events_test.go` mirroring NATS/Kafka tests.                                     | Testing      | Test core Publish/Subscribe via RabbitMQ using `testenv`.                                                              | Medium   |
| US-09 | As a Cosmo Router Developer, I want to add RabbitMQ test cases to `router-tests/websocket_test.go`.                                                         | Testing      | Ensure subscriptions over WebSockets work when backed by RabbitMQ.                                                   | Medium   |
| US-10 | As a Cosmo Router Developer, I want to add RabbitMQ configuration tests to `router-tests/events/events_config_test.go`.                                     | Testing      | Verify loading and validation of RabbitMQ config sections.                                                           | Medium   |

--- 