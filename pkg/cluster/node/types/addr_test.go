package types

import (
	"strings"
	"testing"
)

// ValidateAdvertiseAddr 在实际提案前拒绝不可广播的地址。
// 表驱动覆盖：合法留白、合法目标、非法 bind-all、非法缺端口、格式错误。
func TestValidateAdvertiseAddr(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		wantErr string // 子串，命中即算通过；空串表示要求 err==nil
	}{
		// 合法
		{name: "empty allowed (standalone bootstrap)", addr: "", wantErr: ""},
		{name: "whitespace treated as empty", addr: "   ", wantErr: ""},
		{name: "ipv4 loopback", addr: "127.0.0.1:11110", wantErr: ""},
		{name: "ipv4 intranet", addr: "10.0.0.1:11110", wantErr: ""},
		{name: "ipv6 loopback", addr: "[::1]:11110", wantErr: ""},
		{name: "hostname", addr: "node-1.internal:11110", wantErr: ""},
		{name: "tcp scheme stripped", addr: "tcp://127.0.0.1:11110", wantErr: ""},

		// 非法：bind-all
		{name: "ipv4 unspecified", addr: "0.0.0.0:11110", wantErr: "bind-all"},
		{name: "ipv4 unspecified with scheme", addr: "tcp://0.0.0.0:11110", wantErr: "bind-all"},
		{name: "ipv6 unspecified", addr: "[::]:11110", wantErr: "bind-all"},

		// 非法：结构
		{name: "portless bare ip", addr: "10.0.0.1", wantErr: "host:port"},
		{name: "trailing colon no port", addr: "10.0.0.1:", wantErr: "port"},
		{name: "leading colon no host", addr: ":11110", wantErr: "host"},

		// TCP 端口必须是十进制数字且在 1..65535 内。
		{name: "minimum port", addr: "node.internal:1"},
		{name: "maximum port", addr: "node.internal:65535"},
		{name: "decimal leading zeros", addr: "node.internal:00080"},
		{name: "service name port", addr: "node.internal:abc", wantErr: "port"},
		{name: "zero port", addr: "node.internal:0", wantErr: "port"},
		{name: "negative port", addr: "node.internal:-1", wantErr: "port"},
		{name: "signed port", addr: "node.internal:+1", wantErr: "port"},
		{name: "overflow port", addr: "node.internal:65536", wantErr: "port"},
		{name: "large overflow port", addr: "node.internal:9999999999999999999999", wantErr: "port"},
		{name: "hex port", addr: "node.internal:0x50", wantErr: "port"},
		{name: "non ascii digits", addr: "node.internal:８０", wantErr: "port"},
		{name: "space before port", addr: "node.internal: 11110", wantErr: "whitespace"},
		{name: "space after port", addr: "node.internal:11110 ", wantErr: "whitespace"},
		{name: "tab in port", addr: "node.internal:11\t110", wantErr: "whitespace"},
		{name: "newline after port", addr: "node.internal:11110\n", wantErr: "whitespace"},
		{name: "space in host", addr: "node .internal:11110", wantErr: "whitespace"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAdvertiseAddr(tc.addr)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want nil err, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want err containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want err containing %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}
