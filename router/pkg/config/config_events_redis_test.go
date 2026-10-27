package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullValidRedisProvider(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"
events:
  providers:
    redis:
      - id: my-redis
        urls:
          - "redis://localhost:6379"
        cluster_enabled: true

`)

	_, err := LoadConfig([]string{f})
	require.NoError(t, err)
}

func TestInvalidRedisProviderWithoutUrl(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"

events:
  providers:
    redis:
      - id: my-redis
        urls: []

`)

	_, err := LoadConfig([]string{f})
	require.ErrorContains(t, err, "at '/events/providers/redis/0/urls': minItems: got 0, want 1")
}

func TestInvalidRedisProviderNegativeSubscriptionConnections(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"

events:
  providers:
    redis:
      - id: my-redis
        urls:
          - "redis://localhost:6379"
        subscription_connections: -1

`)

	_, err := LoadConfig([]string{f})
	require.ErrorContains(t, err, "at '/events/providers/redis/0/subscription_connections': minimum: got -1, want 0")
}
