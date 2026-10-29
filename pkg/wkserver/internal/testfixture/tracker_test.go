package testfixture

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTrackerExactlyOnce(t *testing.T) {
	tr := NewTracker()
	const n = 1000
	for i := uint64(0); i < n; i++ {
		tr.Expect(i)
	}
	var wg sync.WaitGroup
	for w := uint64(0); w < 4; w++ {
		wg.Add(1)
		go func(w uint64) {
			defer wg.Done()
			for i := w; i < n; i += 4 {
				tr.Received(i)
			}
		}(w)
	}
	require.NoError(t, tr.Wait(time.Second))
	wg.Wait()
	require.NoError(t, tr.Verify())
}

func TestTrackerReceiveBeforeExpect(t *testing.T) {
	tr := NewTracker()
	// Delivery can race the return of Send; it must not create an obligation.
	tr.Received(7)
	tr.Expect(7)
	require.Equal(t, 0, tr.Pending())
	require.NoError(t, tr.Wait(time.Millisecond))
}

func TestTrackerCancelBalancesRejectedSend(t *testing.T) {
	tr := NewTracker()
	tr.Expect(1)
	tr.Expect(2)
	tr.Cancel(2)
	tr.Received(1)
	require.Equal(t, 0, tr.Pending())
	require.NoError(t, tr.Wait(time.Millisecond))
}

func TestTrackerTimeoutReportsMissingRanges(t *testing.T) {
	tr := NewTracker()
	for i := uint64(0); i < 10; i++ {
		tr.Expect(i)
	}
	for _, i := range []uint64{0, 4, 9} {
		tr.Received(i)
	}
	start := time.Now()
	err := tr.Wait(20 * time.Millisecond)
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
	require.Contains(t, err.Error(), "missing=7 [1-3 5-8]")
}

func TestTrackerReportsDuplicatesAndUnexpected(t *testing.T) {
	tr := NewTracker()
	tr.Expect(1)
	tr.Received(1)
	tr.Received(1)
	tr.Received(99)
	err := tr.Verify()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicates=1 [1(x2)]")
	require.Contains(t, err.Error(), "unexpected=1 [99]")
}

func TestTrackerReportsReceiptOfCancelledID(t *testing.T) {
	tr := NewTracker()
	tr.Expect(3)
	tr.Received(3)
	tr.Cancel(3)
	err := tr.Verify()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected=1 [3]")
}

func TestTrackerReportsMisuse(t *testing.T) {
	tr := NewTracker()
	tr.Expect(1)
	tr.Expect(1)
	tr.Cancel(2)
	tr.Received(1)
	err := tr.Verify()
	require.Error(t, err)
	require.Contains(t, err.Error(), "id 1 expected twice")
	require.Contains(t, err.Error(), "id 2 cancelled without expectation")
}

func TestRanges(t *testing.T) {
	require.Equal(t, "[]", Ranges(nil))
	require.Equal(t, "[1 3-5 9]", Ranges([]uint64{9, 5, 4, 3, 1}))
}

func TestReceiveIsBounded(t *testing.T) {
	ch := make(chan int)
	_, err := Receive(ch, 10*time.Millisecond)
	require.ErrorIs(t, err, ErrNotReceived)

	ch2 := make(chan int, 1)
	ch2 <- 5
	v, err := Receive(ch2, time.Second)
	require.NoError(t, err)
	require.Equal(t, 5, v)
}

func TestListenerHelpers(t *testing.T) {
	addr := FreeAddr(t)
	require.Error(t, WaitListening(addr, 20*time.Millisecond))

	l, err := net.Listen("tcp", HostPort(addr))
	require.NoError(t, err)
	require.NoError(t, WaitListening(addr, time.Second))
	require.Error(t, WaitAddrReleased(addr, 20*time.Millisecond))

	require.NoError(t, l.Close())
	require.NoError(t, WaitAddrReleased(addr, time.Second))
}

func TestWaitAuthedIsBounded(t *testing.T) {
	err := WaitAuthed(func() bool { return false }, 20*time.Millisecond)
	require.EqualError(t, err, "client not authenticated within 20ms")
	require.NoError(t, WaitAuthed(func() bool { return true }, time.Millisecond))
}
