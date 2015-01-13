package rpool

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestSentinelCloserPanic(t *testing.T) {
	defer ensure.PanicDeepEqual(t, "should never get called")
	sentinelCloser(0).Close()
}
