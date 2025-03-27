package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidRabbitMQProviderWithURL(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"

events:
  providers:
    rabbitmq:
      - id: my-rabbitmq
        url: "amqp://localhost:5672"
`)

	_, err := LoadConfig(f, "")
	require.NoError(t, err)
}

func TestValidAuthenticatedRabbitMQProviderWithUserInfo(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"

events:
  providers:
    rabbitmq:
      - id: my-rabbitmq
        url: "amqp://localhost:5672"
        authentication:
          user_info:
            username: "guest"
            password: "guest"
`)

	_, err := LoadConfig(f, "")
	require.NoError(t, err)
}

func TestInvalidAuthenticatedRabbitMQProviderNoUsername(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"

events:
  providers:
    rabbitmq:
      - id: my-rabbitmq
        url: "amqp://localhost:5672"
        authentication:
          user_info:
            password: "guest"
`)

	_, err := LoadConfig(f, "")
	// Note: If none of the oneOf array matches, the first in the array is compared
	require.ErrorContains(t, err, "router config validation error: jsonschema validation failed with 'https://raw.githubusercontent.com/wundergraph/cosmo/main/router/pkg/config/config.schema.json#'\n- at '/events/providers/rabbitmq/0/authentication': oneOf failed, none matched\n  - at '/events/providers/rabbitmq/0/authentication/user_info': missing property 'username'")
}

func TestInvalidAuthenticatedRabbitMQProviderNoPassword(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"

events:
  providers:
    rabbitmq:
      - id: my-rabbitmq
        url: "amqp://localhost:5672"
        authentication:
          user_info:
            username: "guest"
`)

	_, err := LoadConfig(f, "")
	// Note: If none of the oneOf array matches, the first in the array is compared
	require.ErrorContains(t, err, "router config validation error: jsonschema validation failed with 'https://raw.githubusercontent.com/wundergraph/cosmo/main/router/pkg/config/config.schema.json#'\n- at '/events/providers/rabbitmq/0/authentication': oneOf failed, none matched\n  - at '/events/providers/rabbitmq/0/authentication/user_info': missing property 'password'")
}

func TestInvalidRabbitMQProviderNoURL(t *testing.T) {
	t.Parallel()

	f := createTempFileFromFixture(t, `
version: "1"

graph:
  token: "token"

events:
  providers:
    rabbitmq:
      - id: my-rabbitmq
`)

	_, err := LoadConfig(f, "")
	require.ErrorContains(t, err, "router config validation error: jsonschema validation failed with 'https://raw.githubusercontent.com/wundergraph/cosmo/main/router/pkg/config/config.schema.json#'\n- at '/events/providers/rabbitmq/0': missing property 'url'")
}