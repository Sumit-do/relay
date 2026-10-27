package memory

import (
	"testing"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/storetest"
)

func TestContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) relay.Store { return New() })
}
