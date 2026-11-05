package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestHTTPSendAcknowledgesAdmissionAndReportsRejection(t *testing.T) {
	oldOpts, oldAdmit := options.G, service.AdmitHTTPMessage
	t.Cleanup(func() { options.G, service.AdmitHTTPMessage = oldOpts, oldAdmit })
	options.G = options.New()
	vp := viper.New()
	vp.Set("rootDir", t.TempDir())
	options.G.ConfigureWithViper(vp)
	for _, tc := range []struct {
		err    error
		status int
	}{{nil, 200}, {service.ErrMessageAdmissionUnavailable, 503}, {service.ErrMessageAdmissionUnknown, 504}, {service.ErrMessageAdmissionDenied, 400}} {
		called := false
		service.AdmitHTTPMessage = func(ctx context.Context, id string, typ uint8, e *eventbus.Event) error {
			called = true
			require.Equal(t, "room", id)
			require.Equal(t, uint8(2), typ)
			require.Positive(t, e.MessageId)
			_, bounded := ctx.Deadline()
			require.True(t, bounded)
			return tc.err
		}
		r := wkhttp.New()
		m := newMessage(nil)
		r.POST("/message/send", m.send)
		w := httptest.NewRecorder()
		q := httptest.NewRequest(http.MethodPost, "/message/send", strings.NewReader(`{"from_uid":"u","channel_id":"room","channel_type":2,"client_msg_no":"key","payload":"aGk="}`))
		q.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, q)
		require.True(t, called)
		require.Equal(t, tc.status, w.Code)
		var result map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		if tc.err != nil {
			require.NotContains(t, result, "message_id")
		} else {
			require.Equal(t, "key", result["data"].(map[string]any)["client_msg_no"])
		}
	}
}
