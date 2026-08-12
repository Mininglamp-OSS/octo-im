package message

import runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"

const (
	federationShadowUIDPrefix       = "__fed_"
	federationShadowUIDRandomLength = 32
)

// isFederationShadowUID recognizes the complete local-only shadow UID wire
// contract. Prefix lookalikes remain ordinary UIDs.
func isFederationShadowUID(uid string) bool {
	if len(uid) != len(federationShadowUIDPrefix)+federationShadowUIDRandomLength ||
		uid[:len(federationShadowUIDPrefix)] != federationShadowUIDPrefix {
		return false
	}
	for _, char := range uid[len(federationShadowUIDPrefix):] {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func isFederationShadowPersonChannel(channelID string) bool {
	left, right, err := runtimechannelid.DecodePersonChannel(channelID)
	if err != nil {
		return false
	}
	return isFederationShadowUID(left) || isFederationShadowUID(right)
}
