package caboose_test

import (
	"testing"

	"github.com/filecoin-saturn/caboose/internal/util"
)

func TestPoolDynamics(t *testing.T) {
	ch := util.BuildCabooseHarness(t, 3, 3)

	ch.StartOrchestrator()
}
