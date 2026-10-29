package types

import (
	"strings"
	"testing"
)

// ValidateAdvertiseAddr 是写入 raft 前的把关，命中即拒绝启动 / 拒绝 join。
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
