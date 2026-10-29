package types

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// ValidateAdvertiseAddr 校验一个即将被写入 raft、复制给全集群 peer 用作
// 拨号目标的地址是否可用。空串合法（standalone 首启的暂态），非空时要求：
//   - 形如 host:port（允许可选 "tcp://" 前缀）；
//   - host 与 port 都不为空；
//   - host 不是 unspecified 地址（0.0.0.0 / ::）——这类值只能用于 listen bind，
//     被 peer 当作拨号目标时会解析成 peer 自身的 loopback，产生 silent 自连。
//
// 主机名与 loopback（127.0.0.1 / ::1）合法：单机多实例测试与 exampleconfig 都在用。
func ValidateAdvertiseAddr(addr string) error {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return nil
	}
	trimmed = strings.TrimPrefix(trimmed, "tcp://")
	host, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		return fmt.Errorf("must be host:port: %w", err)
	}
	if strings.TrimSpace(host) == "" {
		return errors.New("host is empty")
	}
	if strings.TrimSpace(port) == "" {
		return errors.New("port is empty")
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("host %q is a bind-all address, not a dial target; use the intranet IP peers can reach", host)
	}
	return nil
}
