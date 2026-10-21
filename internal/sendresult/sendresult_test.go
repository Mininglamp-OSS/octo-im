package sendresult

import (
	"testing"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestResultCodecRoundTrip(t *testing.T) {
	in := []Result{
		{Key: 1, Outcome: OutcomePersisted, MessageId: 9, MessageSeq: 42},
		{Key: -2, Outcome: OutcomeRejected, Reason: wkproto.ReasonNotAllowSend},
		{Key: 3, Outcome: OutcomeUnknown, Reason: wkproto.ReasonSystemError},
	}
	out, err := Decode(Encode(in))
	require.NoError(t, err)
	require.Equal(t, in, out)

	empty, err := Decode(Encode(nil))
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestResultCodecRejectsMalformedInput(t *testing.T) {
	data := Encode([]Result{{Key: 1, Outcome: OutcomePersisted}})
	_, err := Decode(data[:len(data)-1])
	require.Error(t, err)

	_, err = Decode([]byte{0xff, 0xff, 0xff, 0xff})
	require.Error(t, err, "a count the payload cannot hold is rejected before allocation")

	bad := Encode([]Result{{Key: 1, Outcome: 9}})
	_, err = Decode(bad)
	require.Error(t, err)
}

func TestRegistryCompletesOnceAndDropsUnknownKeys(t *testing.T) {
	r := NewRegistry()
	ch := make(chan Result, 2)
	require.NoError(t, r.Register(1, ch))
	require.Error(t, r.Register(1, ch), "a key has one waiter")
	require.True(t, r.Complete(Result{Key: 1, Outcome: OutcomePersisted}))
	require.False(t, r.Complete(Result{Key: 1, Outcome: OutcomeUnknown}), "a key is completed at most once")
	require.False(t, r.Complete(Result{Key: 2, Outcome: OutcomePersisted}))
	require.Len(t, ch, 1)
	require.Zero(t, r.Pending())
}

func TestRegistryCompleteNeverBlocks(t *testing.T) {
	r := NewRegistry()
	ch := make(chan Result) // no buffer and no reader
	require.NoError(t, r.Register(1, ch))
	require.False(t, r.Complete(Result{Key: 1, Outcome: OutcomePersisted}))
	require.Zero(t, r.Pending())
}

func TestRegistryCloseRejectsNewWaiters(t *testing.T) {
	r := NewRegistry()
	r.Close()
	r.Close()
	<-r.Done()
	require.ErrorIs(t, r.Register(1, make(chan Result, 1)), ErrClosed)

	r.Open()
	select {
	case <-r.Done():
		t.Fatal("a reopened registry must not report done")
	default:
	}
	require.NoError(t, r.Register(1, make(chan Result, 1)))
}
