package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/spf13/viper"
)

type localSlotCluster struct{ icluster.ICluster }

func (localSlotCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) { return 1, nil }

// droppingChannel accepts events and never persists them, like a channel
// whose persistence proposal never commits.
type droppingChannel struct{ events []*eventbus.Event }

func (c *droppingChannel) AddEvent(_ string, _ uint8, e *eventbus.Event) {
	c.events = append(c.events, e)
}
func (c *droppingChannel) Advance(string, uint8) {}

// Uses only the long-standing API surface so it runs unchanged against the
// asynchronous implementation, which answered 200 before persistence.
func TestMessageSendDoesNotAcknowledgeUnpersistedMessage(t *testing.T) {
	oldOptions, oldCluster, oldChannel := options.G, service.Cluster, eventbus.Channel
	t.Cleanup(func() { options.G, service.Cluster, eventbus.Channel = oldOptions, oldCluster, oldChannel })
	options.G = options.New(options.WithRootDir(t.TempDir()))
	options.G.ConfigureWithViper(viper.New())
	options.G.Cluster.NodeId = 1
	options.G.Cluster.ReqTimeout = 100 * time.Millisecond
	service.Cluster = localSlotCluster{}
	sink := &droppingChannel{}
	eventbus.RegisterChannel(sink)

	router := wkhttp.New()
	newMessage(nil).route(router)
	req := httptest.NewRequest(http.MethodPost, "/message/send", strings.NewReader(`{"from_uid":"alice","channel_id":"g","channel_type":2,"payload":"aGk="}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.GetGinRoute().ServeHTTP(resp, req)

	if len(sink.events) == 0 {
		t.Fatal("the message was never submitted")
	}
	if resp.Code == http.StatusOK {
		t.Fatalf("an unpersisted message was acknowledged with HTTP 200: %s", resp.Body.String())
	}
	if resp.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected outcome unknown (504), got %d: %s", resp.Code, resp.Body.String())
	}
}
