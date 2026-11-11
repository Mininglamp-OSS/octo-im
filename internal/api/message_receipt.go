package api

import (
	"context"
	"net/http"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/sendgrid/rest"
)

// Preserve the owner's capability header; an older peer must not be advertised as supporting receipts.
func forwardCommittedReceipt(c *wkhttp.Context, url string) {
	query := make(map[string]string)
	for key, values := range c.Request.URL.Query() {
		query[key] = values[0]
	}
	resp, err := rest.SendWithContext(c.Request.Context(), rest.Request{Method: rest.Get, BaseURL: url, Headers: c.CopyRequestHeader(c.Request), QueryParams: query})
	if err != nil {
		c.ResponseError(err)
		return
	}
	if version := http.Header(resp.Headers).Get("X-WK-Receipt-Version"); version != "" {
		c.Header("X-WK-Receipt-Version", version)
	}
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.Writer.WriteHeader(resp.StatusCode)
	_, _ = c.Writer.Write([]byte(resp.Body))
}

func committedMessageReceipt(ctx context.Context, db wkdb.DB, channelID string, channelType uint8, fromUID, clientMsgNo string) (wkdb.Message, error) {
	// Read the durable boundary first: an indexed but unapplied suffix can still be replaced.
	applied, err := db.GetChannelAppliedIndex(channelID, channelType)
	if err != nil {
		return wkdb.Message{}, err
	}
	if applied == 0 {
		return wkdb.Message{}, wkdb.ErrNotFound
	}
	msg, err := db.LoadMsgBySenderClientMsgNo(ctx, channelID, channelType, fromUID, clientMsgNo)
	if err != nil {
		return wkdb.Message{}, err
	}
	if msg.MessageSeq == 0 || uint64(msg.MessageSeq) > applied {
		return wkdb.Message{}, wkdb.ErrNotFound
	}
	return msg, nil
}
