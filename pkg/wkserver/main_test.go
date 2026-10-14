package wkserver_test

import (
	"os"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

// TestMain configures logging once, before any server or client goroutine
// starts. Otherwise wklog initializes its global loggers lazily from
// whichever goroutine logs first, which the race detector reports.
func TestMain(m *testing.M) {
	wklog.Configure(wklog.NewOptions())
	os.Exit(m.Run())
}
