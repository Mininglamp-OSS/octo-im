package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type capturedRetryManager struct {
	service.RetryMgr
	msg *types.RetryMessage
}

func (m *capturedRetryManager) AddRetry(msg *types.RetryMessage) { m.msg = msg }

func TestSetupRetryBindsPhysicalSessionIdentity(t *testing.T) {
	old := service.RetryManager
	t.Cleanup(func() { service.RetryManager = old })
	captured := &capturedRetryManager{}
	service.RetryManager = captured
	conn := &eventbus.Conn{
		Uid: "u", NodeId: 2, ConnId: 7, Uptime: 101, OwnerBootID: "boot", SessionID: "session",
	}

	(&Handler{}).setupRetryIfNeeded(&wkproto.RecvPacket{}, "channel", 1, conn, 9)

	require.NotNil(t, captured.msg)
	require.Equal(t, "boot", captured.msg.OwnerBootID)
	require.Equal(t, "session", captured.msg.SessionID)
	require.Equal(t, uint64(101), captured.msg.Uptime)
}
