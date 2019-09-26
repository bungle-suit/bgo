package bgo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptionInit(t *testing.T) {
	t.Run("No RequiredVersion", func(t *testing.T) {
		opt := option{
			DbURL:    "mongodb://localhost",
			Database: "test",
		}
		assert.NoError(t, opt.Init())

		mgoOpts := ClientOptions()
		assert.Equal(t, []string{"localhost"}, mgoOpts.Hosts)
		assert.Equal(t, "test", Database())
	})

	t.Run("RequiredVersion match", func(t *testing.T) {
		opt := option{
			DbURL:           "mongodb://localhost",
			Database:        "test3",
			RequiredVersion: "1.0",
		}
		assert.NoError(t, opt.Init())

		mgoOpts := ClientOptions()
		assert.Equal(t, []string{"localhost"}, mgoOpts.Hosts)
		assert.Equal(t, "test3", Database())
	})

	t.Run("RequiredVersion not match", func(t *testing.T) {
		opt := option{
			DbURL:           "mongodb://localhost",
			Database:        "test",
			RequiredVersion: "4.5.2",
		}
		assert.Error(t, opt.Init())
	})
}
