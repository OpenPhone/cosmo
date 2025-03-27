package events_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wundergraph/cosmo/router-tests/testenv"
	"testing"
)

func TestRabbitMQEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Run("rabbitmq events", func(t *testing.T) {
		err := testenv.Run(t, &testenv.Config{
			RouterConfigJSONTemplate: testenv.ConfigWithEdfsJSONTemplate,
			EnableNats:               true,
			EnableKafka:              true,
			EnableRabbitMQ:           true,
		}, func(t *testing.T, xEnv *testenv.Environment) {
			assert.NoError(t, err)
		})
	})
}