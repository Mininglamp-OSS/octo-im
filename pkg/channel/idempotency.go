package channel

import "bytes"

// SameIdempotencyRequest reports whether two durable messages have the same
// caller-visible write intent under one idempotency key. The key itself is
// scoped by channel, sender, and client message number; this comparison also
// includes those fields so corrupted or mismatched records fail closed.
func SameIdempotencyRequest(existing, incoming Message) bool {
	return existing.ChannelID == incoming.ChannelID &&
		existing.ChannelType == incoming.ChannelType &&
		existing.FromUID == incoming.FromUID &&
		existing.ClientMsgNo == incoming.ClientMsgNo &&
		existing.Framer.NoPersist == incoming.Framer.NoPersist &&
		existing.Framer.RedDot == incoming.Framer.RedDot &&
		existing.Framer.SyncOnce == incoming.Framer.SyncOnce &&
		existing.Setting == incoming.Setting &&
		existing.Topic == incoming.Topic &&
		existing.Expire == incoming.Expire &&
		bytes.Equal(existing.Payload, incoming.Payload)
}
