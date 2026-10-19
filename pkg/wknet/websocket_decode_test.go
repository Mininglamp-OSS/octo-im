package wknet

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
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

func TestDecodeClientMessagesKeepsIncompleteFrame(t *testing.T) {
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
			stream := testJoin(first, testMaskedFrame(t, true, ws.OpBinary, tt.payload))
			firstRead := stream[:len(first)+tt.split]

			messages, consumed, err := decodeClientMessages(firstRead)
			require.NoError(t, err)
			require.Equal(t, len(first), consumed, "后续半帧不能被消费")
			require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte("first")}}, messages)

			// 剩余字节到达后，与保留下来的半帧一起解码
			nextRead := testJoin(firstRead[consumed:], stream[len(firstRead):])
			messages, consumed, err = decodeClientMessages(nextRead)
			require.NoError(t, err)
			require.Equal(t, len(nextRead), consumed)
			require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte(tt.payload)}}, messages)
		})
	}
}

func TestDecodeClientMessagesEveryChunkSize(t *testing.T) {
	long := string(bytes.Repeat([]byte("y"), 300))
	stream := testJoin(
		testMaskedFrame(t, true, ws.OpBinary, "first"),
		testMaskedFrame(t, true, ws.OpPing, "p1"),
		testMaskedFrame(t, false, ws.OpText, "frag-"),
		testMaskedFrame(t, true, ws.OpPing, "p2"), // 分片消息中间插入的控制帧
		testMaskedFrame(t, true, ws.OpContinuation, "ment"),
		testMaskedFrame(t, true, ws.OpBinary, long),
	)
	expected := []wsutil.Message{
		{OpCode: ws.OpBinary, Payload: []byte("first")},
		{OpCode: ws.OpPing, Payload: []byte("p1")},
		{OpCode: ws.OpPing, Payload: []byte("p2")},
		{OpCode: ws.OpText, Payload: []byte("frag-ment")},
		{OpCode: ws.OpBinary, Payload: []byte(long)},
	}

	// 按每种读取长度切分数据流，模拟任意 TCP 读取边界
	for chunkSize := 1; chunkSize <= len(stream); chunkSize++ {
		var pending []byte
		var got []wsutil.Message
		for offset := 0; offset < len(stream); offset += chunkSize {
			end := min(offset+chunkSize, len(stream))
			pending = append(pending, stream[offset:end]...)
			messages, consumed, err := decodeClientMessages(pending)
			require.NoError(t, err, "chunkSize=%d offset=%d", chunkSize, offset)
			got = append(got, messages...)
			pending = pending[consumed:]
		}
		require.Empty(t, pending, "chunkSize=%d", chunkSize)
		require.Equal(t, expected, got, "chunkSize=%d", chunkSize)
	}
}

func TestDecodeClientMessagesWaitsForFragmentedMessage(t *testing.T) {
	start := testMaskedFrame(t, false, ws.OpBinary, "frag-")
	end := testMaskedFrame(t, true, ws.OpContinuation, "ment")

	messages, consumed, err := decodeClientMessages(start)
	require.NoError(t, err)
	require.Zero(t, consumed)
	require.Empty(t, messages)

	messages, consumed, err = decodeClientMessages(testJoin(start, end))
	require.NoError(t, err)
	require.Equal(t, len(start)+len(end), consumed)
	require.Equal(t, []wsutil.Message{{OpCode: ws.OpBinary, Payload: []byte("frag-ment")}}, messages)
}

func TestDecodeClientMessagesRejectsInvalidFrames(t *testing.T) {
	var unmasked bytes.Buffer
	require.NoError(t, ws.WriteFrame(&unmasked, ws.NewBinaryFrame([]byte("data"))))

	rsvFrame := ws.MaskFrameWith(ws.NewBinaryFrame([]byte("data")), [4]byte{1, 2, 3, 4})
	rsvFrame.Header.Rsv = ws.Rsv(true, false, false)
	var rsv bytes.Buffer
	require.NoError(t, ws.WriteFrame(&rsv, rsvFrame))

	tests := []struct {
		name string
		data []byte
		err  error
	}{
		{name: "unmasked", data: unmasked.Bytes(), err: ws.ErrProtocolMaskRequired},
		{name: "rsv bits", data: rsv.Bytes(), err: ws.ErrProtocolNonZeroRsv},
		{name: "unexpected continuation", data: testMaskedFrame(t, true, ws.OpContinuation, "data"), err: ws.ErrProtocolContinuationUnexpected},
		{name: "fragmented control", data: testMaskedFrame(t, false, ws.OpPing, "data"), err: ws.ErrProtocolControlNotFinal},
		// 帧头已经错误时不再等待后续数据
		{name: "invalid header with incomplete payload", data: unmasked.Bytes()[:3], err: ws.ErrProtocolMaskRequired},
		{name: "invalid frame after valid frame", data: testJoin(testMaskedFrame(t, true, ws.OpBinary, "ok"), unmasked.Bytes()), err: ws.ErrProtocolMaskRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages, consumed, err := decodeClientMessages(tt.data)
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
			e := NewEngine(WithWSAddr("ws://0.0.0.0:0"))
			collector := &testWSPayloadCollector{}
			e.OnData(collector.onData)
			require.NoError(t, e.Start())
			defer e.Stop()

			conn, _, _, err := ws.Dial(context.Background(), "ws://"+e.WSRealListenAddr().String())
			require.NoError(t, err)
			defer conn.Close()

			testWriteInTwoReads(t, conn, tt.data, tt.split)
			collector.waitFor(t, tt.expected)
		})
	}
}

func TestWSSConnKeepsFrameSplitAcrossReads(t *testing.T) {
	cert, err := stls.X509KeyPair(rsaCertPEM, rsaKeyPEM)
	require.NoError(t, err)
	e := NewEngine(WithWSSAddr("wss://0.0.0.0:0"), WithWSTLSConfig(&stls.Config{Certificates: []stls.Certificate{cert}}))
	collector := &testWSPayloadCollector{}
	e.OnData(collector.onData)
	require.NoError(t, e.Start())
	defer e.Stop()

	dialer := ws.Dialer{TLSConfig: &tls.Config{InsecureSkipVerify: true, MaxVersion: tls.VersionTLS13}}
	conn, _, _, err := dialer.Dial(context.Background(), "wss://"+e.WSSRealListenAddr().String())
	require.NoError(t, err)
	defer conn.Close()

	first := testMaskedFrame(t, true, ws.OpBinary, "first|")
	second := testMaskedFrame(t, true, ws.OpBinary, "second|")
	testWriteInTwoReads(t, conn, testJoin(first, second), len(first)+8)
	collector.waitFor(t, "first|second|")
}
