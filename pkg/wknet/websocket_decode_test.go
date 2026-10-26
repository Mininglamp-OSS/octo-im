package wknet

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	stls "github.com/WuKongIM/crypto/tls"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/stretchr/testify/require"
)

// 客户端发往服务端的帧必须带掩码
func testMaskedFrame(t *testing.T, fin bool, op ws.OpCode, payload string) []byte {
	t.Helper()
	frame := ws.MaskFrameWith(ws.NewFrame(op, fin, []byte(payload)), [4]byte{1, 2, 3, 4})
	var buf bytes.Buffer
	require.NoError(t, ws.WriteFrame(&buf, frame))
	return buf.Bytes()
}

func testJoin(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func TestClientMessageDecoderKeepsIncompleteFrame(t *testing.T) {
	first := testMaskedFrame(t, true, ws.OpBinary, "first")
	short := "second-frame-payload"
	long := string(bytes.Repeat([]byte("x"), 300)) // 使用 2 字节扩展长度

	tests := []struct {
		name    string
		payload string
		split   int // 第一次读取到的第二个帧的字节数
	}{
		{name: "split in header", payload: short, split: 1},
		{name: "split in mask", payload: short, split: 4},
		{name: "split in payload", payload: short, split: 9},
		{name: "split in extended length", payload: long, split: 3},
		{name: "split in long payload", payload: long, split: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &clientMessageDecoder{}
			stream := testJoin(first, testMaskedFrame(t, true, ws.OpBinary, tt.payload))
			firstRead := stream[:len(first)+tt.split]

			messages, consumed, err := d.decode(firstRead)
			require.NoError(t, err)
			require.Equal(t, len(first), consumed, "后续半帧不能被消费")
			require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte("first")}}, messages)

			// 剩余字节到达后，与保留下来的半帧一起解码
			nextRead := testJoin(firstRead[consumed:], stream[len(firstRead):])
			messages, consumed, err = d.decode(nextRead)
			require.NoError(t, err)
			require.Equal(t, len(nextRead), consumed)
			require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte(tt.payload)}}, messages)
		})
	}
}

func TestClientMessageDecoderEveryChunkSize(t *testing.T) {
	long := string(bytes.Repeat([]byte("y"), 300))
	frames := []struct {
		frame    []byte
		complete []wsutil.Message // 这个帧到齐时应当立即解出的消息
	}{
		{testMaskedFrame(t, true, ws.OpBinary, "first"), []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte("first")}}},
		{testMaskedFrame(t, true, ws.OpPing, "p1"), []wsutil.Message{{OpCode: ws.OpPing, Payload: []byte("p1")}}},
		{testMaskedFrame(t, false, ws.OpText, "frag-"), nil},
		// 分片消息中间插入的控制帧不能等到分片消息结束才处理
		{testMaskedFrame(t, true, ws.OpPing, "p2"), []wsutil.Message{{OpCode: ws.OpPing, Payload: []byte("p2")}}},
		{testMaskedFrame(t, false, ws.OpContinuation, "men"), nil},
		{testMaskedFrame(t, true, ws.OpPong, "p3"), []wsutil.Message{{OpCode: ws.OpPong, Payload: []byte("p3")}}},
		{testMaskedFrame(t, true, ws.OpContinuation, "t"), []wsutil.Message{{OpCode: ws.OpText, Payload: []byte("frag-ment")}}},
		{testMaskedFrame(t, true, ws.OpBinary, long), []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte(long)}}},
	}
	var stream []byte
	for _, f := range frames {
		stream = append(stream, f.frame...)
	}

	// 按每种读取长度切分数据流，模拟任意 TCP 读取边界
	for chunkSize := 1; chunkSize <= len(stream); chunkSize++ {
		d := &clientMessageDecoder{}
		var pending []byte
		var got []wsutil.Message
		for offset := 0; offset < len(stream); offset += chunkSize {
			end := min(offset+chunkSize, len(stream))
			pending = append(pending, stream[offset:end]...)
			messages, consumed, err := d.decode(pending)
			require.NoError(t, err, "chunkSize=%d end=%d", chunkSize, end)
			got = append(got, messages...)
			pending = pending[consumed:]

			// 每次读取后：已到齐的帧全部被消费，只保留末尾不完整的帧；已完整的消息立即返回
			boundary := 0
			var want []wsutil.Message
			for _, f := range frames {
				if boundary+len(f.frame) > end {
					break
				}
				boundary += len(f.frame)
				want = append(want, f.complete...)
			}
			require.Equal(t, end-boundary, len(pending), "chunkSize=%d end=%d", chunkSize, end)
			require.Equal(t, want, got, "chunkSize=%d end=%d", chunkSize, end)
		}
	}
}

func TestClientMessageDecoderWaitsForFragmentedMessage(t *testing.T) {
	d := &clientMessageDecoder{}
	start := testMaskedFrame(t, false, ws.OpBinary, "frag-")
	end := testMaskedFrame(t, true, ws.OpContinuation, "ment")

	// 非 FIN 的分片被解码器收下，但消息要等最后一个分片到达才返回
	messages, consumed, err := d.decode(start)
	require.NoError(t, err)
	require.Equal(t, len(start), consumed)
	require.Empty(t, messages)

	messages, consumed, err = d.decode(end)
	require.NoError(t, err)
	require.Equal(t, len(end), consumed)
	require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte("frag-ment")}}, messages)
}

func TestClientMessageDecoderControlFrameDuringFragmentedMessage(t *testing.T) {
	ready := testMaskedFrame(t, true, ws.OpBinary, "ready")
	fragStart := testMaskedFrame(t, false, ws.OpBinary, "part-")
	closeBody := string(ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))

	tests := []struct {
		name    string
		control []byte
		want    wsutil.Message
	}{
		{name: "ping", control: testMaskedFrame(t, true, ws.OpPing, "p"), want: wsutil.Message{OpCode: ws.OpPing, Payload: []byte("p")}},
		// Close 之后对方不会再发数据帧，不能等最后一个分片
		{name: "close", control: testMaskedFrame(t, true, ws.OpClose, closeBody), want: wsutil.Message{OpCode: ws.OpClose, Payload: []byte(closeBody)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &clientMessageDecoder{}
			buff := testJoin(ready, fragStart, tt.control)
			messages, consumed, err := d.decode(buff)
			require.NoError(t, err)
			require.Equal(t, len(buff), consumed)
			require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte("ready")}, tt.want}, messages)

			// 最后一个分片到达后只返回分片消息，控制帧不会重复返回
			end := testMaskedFrame(t, true, ws.OpContinuation, "rest")
			messages, consumed, err = d.decode(end)
			require.NoError(t, err)
			require.Equal(t, len(end), consumed)
			require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte("part-rest")}}, messages)
		})
	}
}

func TestClientMessageDecoderRejectsOversizedMessage(t *testing.T) {
	const maxSize = 10

	t.Run("declared frame too large", func(t *testing.T) {
		d := &clientMessageDecoder{maxSize: maxSize}
		frame := testMaskedFrame(t, true, ws.OpBinary, strings.Repeat("x", maxSize+1))
		// 只收到帧头和掩码就拒绝，不等数据到齐
		messages, consumed, err := d.decode(frame[:6])
		require.ErrorIs(t, err, errWSMessageTooLarge)
		require.Zero(t, consumed)
		require.Empty(t, messages)
	})

	t.Run("fragmented message too large", func(t *testing.T) {
		d := &clientMessageDecoder{maxSize: maxSize}
		start := testJoin(testMaskedFrame(t, false, ws.OpBinary, "12345"), testMaskedFrame(t, false, ws.OpContinuation, "67890"))
		messages, consumed, err := d.decode(start)
		require.NoError(t, err)
		require.Equal(t, len(start), consumed)
		require.Empty(t, messages)

		messages, consumed, err = d.decode(testMaskedFrame(t, true, ws.OpContinuation, "1"))
		require.ErrorIs(t, err, errWSMessageTooLarge)
		require.Zero(t, consumed)
		require.Empty(t, messages)
	})

	t.Run("messages at the limit", func(t *testing.T) {
		d := &clientMessageDecoder{maxSize: maxSize}
		buff := testJoin(
			testMaskedFrame(t, true, ws.OpBinary, "1234567890"),
			testMaskedFrame(t, false, ws.OpBinary, "12345"),
			testMaskedFrame(t, false, ws.OpContinuation, "67890"),
			// 分片中间的控制帧不计入分片消息的累计长度
			testMaskedFrame(t, true, ws.OpPing, "p"),
			testMaskedFrame(t, true, ws.OpContinuation, ""),
		)
		messages, consumed, err := d.decode(buff)
		require.NoError(t, err)
		require.Equal(t, len(buff), consumed)
		require.Equal(t, []wsutil.Message{
			{OpCode: ws.OpBinary, Payload: []byte("1234567890")},
			{OpCode: ws.OpPing, Payload: []byte("p")},
			{OpCode: ws.OpBinary, Payload: []byte("1234567890")},
		}, messages)
	})
}

func TestClientMessageDecoderRejectsInvalidFrames(t *testing.T) {
	var unmasked bytes.Buffer
	require.NoError(t, ws.WriteFrame(&unmasked, ws.NewBinaryFrame([]byte("data"))))

	rsvFrame := ws.MaskFrameWith(ws.NewBinaryFrame([]byte("data")), [4]byte{1, 2, 3, 4})
	rsvFrame.Header.Rsv = ws.Rsv(true, false, false)
	var rsv bytes.Buffer
	require.NoError(t, ws.WriteFrame(&rsv, rsvFrame))

	invalidUTF8 := string([]byte{0xff, 0xfe})

	tests := []struct {
		name string
		data []byte
		err  error
	}{
		{name: "unmasked", data: unmasked.Bytes(), err: ws.ErrProtocolMaskRequired},
		{name: "rsv bits", data: rsv.Bytes(), err: ws.ErrProtocolNonZeroRsv},
		{name: "unexpected continuation", data: testMaskedFrame(t, true, ws.OpContinuation, "data"), err: ws.ErrProtocolContinuationUnexpected},
		{name: "fragmented control", data: testMaskedFrame(t, false, ws.OpPing, "data"), err: ws.ErrProtocolControlNotFinal},
		{name: "data frame inside fragmented message", data: testJoin(testMaskedFrame(t, false, ws.OpBinary, "a"), testMaskedFrame(t, true, ws.OpBinary, "b")), err: ws.ErrProtocolContinuationExpected},
		{name: "invalid utf8 text", data: testMaskedFrame(t, true, ws.OpText, invalidUTF8), err: wsutil.ErrInvalidUTF8},
		{name: "invalid utf8 fragmented text", data: testJoin(testMaskedFrame(t, false, ws.OpText, "ok"), testMaskedFrame(t, true, ws.OpContinuation, invalidUTF8)), err: wsutil.ErrInvalidUTF8},
		// 帧头已经错误时不再等待后续数据
		{name: "invalid header with incomplete payload", data: unmasked.Bytes()[:3], err: ws.ErrProtocolMaskRequired},
		{name: "invalid frame after valid frame", data: testJoin(testMaskedFrame(t, true, ws.OpBinary, "ok"), unmasked.Bytes()), err: ws.ErrProtocolMaskRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &clientMessageDecoder{}
			messages, consumed, err := d.decode(tt.data)
			require.ErrorIs(t, err, tt.err)
			require.Zero(t, consumed)
			require.Empty(t, messages)
		})
	}
}

type testWSPayloadCollector struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (c *testWSPayloadCollector) onData(conn Conn) error {
	data, err := conn.Peek(-1)
	if err != nil || len(data) == 0 {
		return err
	}
	c.mu.Lock()
	c.data.Write(data)
	c.mu.Unlock()
	_, err = conn.Discard(len(data))
	return err
}

func (c *testWSPayloadCollector) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.String()
}

func (c *testWSPayloadCollector) waitFor(t *testing.T, expected string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.String() == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("received %q, want %q", c.String(), expected)
}

// 先写入第一段数据，等服务端读取后再写入剩余数据，使帧跨两次 TCP 读取到达
func testWriteInTwoReads(t *testing.T, conn net.Conn, data []byte, split int) {
	t.Helper()
	_, err := conn.Write(data[:split])
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	_, err = conn.Write(data[split:])
	require.NoError(t, err)
}

func testDialWS(t *testing.T, opts ...Option) (net.Conn, *testWSPayloadCollector) {
	t.Helper()
	// 引擎默认还会监听 TCP 5100，这里也改用随机端口，避免与本机已有服务冲突
	e := NewEngine(append([]Option{WithAddr("tcp://127.0.0.1:0"), WithWSAddr("ws://0.0.0.0:0")}, opts...)...)
	collector := &testWSPayloadCollector{}
	e.OnData(collector.onData)
	require.NoError(t, e.Start())
	t.Cleanup(func() { e.Stop() })

	conn, _, _, err := ws.Dial(context.Background(), "ws://"+e.WSRealListenAddr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn, collector
}

func testDialWSS(t *testing.T, opts ...Option) (net.Conn, *testWSPayloadCollector) {
	t.Helper()
	cert, err := stls.X509KeyPair(rsaCertPEM, rsaKeyPEM)
	require.NoError(t, err)
	e := NewEngine(append([]Option{WithAddr("tcp://127.0.0.1:0"), WithWSSAddr("wss://0.0.0.0:0"), WithWSTLSConfig(&stls.Config{Certificates: []stls.Certificate{cert}})}, opts...)...)
	collector := &testWSPayloadCollector{}
	e.OnData(collector.onData)
	require.NoError(t, e.Start())
	t.Cleanup(func() { e.Stop() })

	dialer := ws.Dialer{TLSConfig: &tls.Config{InsecureSkipVerify: true, MaxVersion: tls.VersionTLS13}}
	conn, _, _, err := dialer.Dial(context.Background(), "wss://"+e.WSSRealListenAddr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn, collector
}

func TestWSConnKeepsFrameSplitAcrossReads(t *testing.T) {
	first := testMaskedFrame(t, true, ws.OpBinary, "first|")
	second := testMaskedFrame(t, true, ws.OpBinary, "second|")
	fragStart := testMaskedFrame(t, false, ws.OpBinary, "frag-")
	fragEnd := testMaskedFrame(t, true, ws.OpContinuation, "ment|")

	tests := []struct {
		name     string
		data     []byte
		split    int
		expected string
	}{
		{name: "split in header", data: testJoin(first, second), split: len(first) + 1, expected: "first|second|"},
		{name: "split in payload", data: testJoin(first, second), split: len(first) + 8, expected: "first|second|"},
		{name: "fragmented message", data: testJoin(first, fragStart, fragEnd), split: len(first) + len(fragStart), expected: "first|frag-ment|"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, collector := testDialWS(t)
			testWriteInTwoReads(t, conn, tt.data, tt.split)
			collector.waitFor(t, tt.expected)
		})
	}
}

func TestWSSConnKeepsFrameSplitAcrossReads(t *testing.T) {
	conn, collector := testDialWSS(t)
	first := testMaskedFrame(t, true, ws.OpBinary, "first|")
	second := testMaskedFrame(t, true, ws.OpBinary, "second|")
	testWriteInTwoReads(t, conn, testJoin(first, second), len(first)+8)
	collector.waitFor(t, "first|second|")
}

func testReadServerFrame(t *testing.T, conn net.Conn) ws.Frame {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	frame, err := ws.ReadFrame(conn)
	require.NoError(t, err)
	return frame
}

// 服务端应当立即关闭连接（收到 Close 帧或连接断开），而不是一直挂起
func testRequireConnClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	frame, err := ws.ReadFrame(conn)
	if err == nil {
		require.Equal(t, ws.OpClose, frame.Header.OpCode)
		return
	}
	var netErr net.Error
	require.False(t, errors.As(err, &netErr) && netErr.Timeout(), "连接没有被关闭: %v", err)
}

// 发出一个非 FIN 的分片后暂停，紧接着发送控制帧：服务端必须立即处理控制帧，不能等分片消息结束
func testControlFrameDuringFragmentedMessage(t *testing.T, dial func(t *testing.T, opts ...Option) (net.Conn, *testWSPayloadCollector)) {
	ready := testMaskedFrame(t, true, ws.OpBinary, "ready|")
	fragStart := testMaskedFrame(t, false, ws.OpBinary, "part-")

	t.Run("ping", func(t *testing.T) {
		conn, collector := dial(t)
		_, err := conn.Write(testJoin(ready, fragStart, testMaskedFrame(t, true, ws.OpPing, "p")))
		require.NoError(t, err)

		frame := testReadServerFrame(t, conn)
		require.Equal(t, ws.OpPong, frame.Header.OpCode)
		require.Equal(t, []byte("p"), frame.Payload)
		collector.waitFor(t, "ready|")

		// 最后一个分片到达后，分片消息完整交付
		_, err = conn.Write(testMaskedFrame(t, true, ws.OpContinuation, "rest|"))
		require.NoError(t, err)
		collector.waitFor(t, "ready|part-rest|")
	})

	t.Run("close without final fragment", func(t *testing.T) {
		conn, _ := dial(t)
		closeBody := string(ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
		_, err := conn.Write(testJoin(ready, fragStart, testMaskedFrame(t, true, ws.OpClose, closeBody)))
		require.NoError(t, err)

		// 服务端收到 Close 后直接断开连接、不回 Close 帧（原有行为），
		// 这里只要求连接被立即关闭，而不是等最后一个分片一直挂起
		testRequireConnClosed(t, conn)
	})
}

func TestWSConnHandlesControlFrameDuringFragmentedMessage(t *testing.T) {
	testControlFrameDuringFragmentedMessage(t, testDialWS)
}

func TestWSSConnHandlesControlFrameDuringFragmentedMessage(t *testing.T) {
	testControlFrameDuringFragmentedMessage(t, testDialWSS)
}

// 单条消息（分片按累计长度）和连接上未处理完的数据都受 MaxReadBufferSize 限制，超过时关闭连接
func testInboundSizeLimit(t *testing.T, dial func(t *testing.T, opts ...Option) (net.Conn, *testWSPayloadCollector)) {
	const maxSize = 4096
	limit := func(opts *Options) { opts.MaxReadBufferSize = maxSize }

	t.Run("message within limit", func(t *testing.T) {
		conn, collector := dial(t, limit)
		payload := strings.Repeat("a", 3000)
		_, err := conn.Write(testMaskedFrame(t, true, ws.OpBinary, payload))
		require.NoError(t, err)
		collector.waitFor(t, payload)
	})

	t.Run("declared frame too large", func(t *testing.T) {
		conn, _ := dial(t, limit)
		frame := testMaskedFrame(t, true, ws.OpBinary, strings.Repeat("x", 1<<20))
		// 只发帧头（2 字节 + 8 字节扩展长度）和掩码，不发数据
		_, err := conn.Write(frame[:14])
		require.NoError(t, err)
		testRequireConnClosed(t, conn)
	})

	t.Run("fragmented message too large", func(t *testing.T) {
		conn, _ := dial(t, limit)
		chunk := strings.Repeat("y", 1000)
		for i := 0; i < 5; i++ {
			op := ws.OpContinuation
			if i == 0 {
				op = ws.OpBinary
			}
			if _, err := conn.Write(testMaskedFrame(t, false, op, chunk)); err != nil {
				break // 连接可能已被服务端关闭
			}
		}
		testRequireConnClosed(t, conn)
	})
}

func TestWSConnInboundSizeLimit(t *testing.T) {
	testInboundSizeLimit(t, testDialWS)
}

func TestWSSConnInboundSizeLimit(t *testing.T) {
	testInboundSizeLimit(t, testDialWSS)
}
