package wknet

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/WuKongIM/crypto/tls"
	"go.uber.org/zap"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func CreateWSConn(id int64, connFd NetFd, localAddr, remoteAddr net.Addr, eg *Engine, reactorSub *ReactorSub) (Conn, error) {
	defaultConn := GetDefaultConn(id, connFd, localAddr, remoteAddr, eg, reactorSub)
	return NewWSConn(defaultConn), nil
}

func CreateWSSConn(id int64, connFd NetFd, localAddr, remoteAddr net.Addr, eg *Engine, reactorSub *ReactorSub) (Conn, error) {
	defaultConn := GetDefaultConn(id, connFd, localAddr, remoteAddr, eg, reactorSub)
	tc := newTLSConn(defaultConn)
	tlsCn := tls.Server(tc, eg.options.WSTLSConfig)
	tc.tlsconn = tlsCn
	return NewWSSConn(tc), nil
}

type WSConn struct {
	*DefaultConn
	upgraded         bool
	tmpInboundBuffer InboundBuffer // inboundBuffer InboundBuffer
	decoder          clientMessageDecoder
}

func NewWSConn(d *DefaultConn) *WSConn {
	w := &WSConn{
		DefaultConn:      d,
		tmpInboundBuffer: d.eg.eventHandler.OnNewInboundConn(d, d.eg),
	}
	return w
}

func (w *WSConn) ReadToInboundBuffer() (int, error) {
	readBuffer := w.reactorSub.ReadBuffer
	n, err := w.fd.Read(readBuffer)
	if err != nil || n == 0 {
		return 0, err
	}
	if w.eg.options.Event.OnReadBytes != nil {
		w.eg.options.Event.OnReadBytes(n)
	}
	_, err = w.tmpInboundBuffer.Write(readBuffer[:n])
	if err != nil {
		return 0, err
	}
	w.KeepLastActivity()

	err = w.unpacketWSData()

	return n, err
}

func (w *WSConn) WriteServerBinary(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return wsutil.WriteServerBinary(w.outboundBuffer, data)
}

func (w *WSConn) WriteServerText(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return wsutil.WriteServerText(w.outboundBuffer, data)
}

// 解包ws的数据
func (w *WSConn) unpacketWSData() error {

	if !w.upgraded {
		err := w.upgrade()
		if err != nil {
			return err
		}
		return nil
	}

	messages, err := w.decode()
	if err != nil {
		return err
	}
	if len(messages) > 0 {
		for _, msg := range messages {
			if msg.OpCode.IsControl() {
				err = wsutil.HandleClientControlMessage(w, msg)
				if err != nil {
					return err
				}
				continue
			}
			if msg.OpCode == ws.OpPing {
				w.KeepLastActivity()
			}
			if len(msg.Payload) > 0 {
				_, err = w.inboundBuffer.Write(msg.Payload)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (w *WSConn) decode() ([]wsutil.Message, error) {
	buff, err := w.PeekFromTemp(-1)
	if err != nil {
		return nil, err
	}
	messages, consumed, err := w.decoder.decode(buff)
	if err != nil {
		w.Warn("read client message error", zap.Error(err))
		w.DiscardFromTemp(len(buff)) // 发送错误，丢弃数据
		return nil, err
	}
	w.DiscardFromTemp(consumed)
	return messages, nil
}

// clientMessageDecoder 按帧增量解码客户端发来的 WebSocket 消息。
// 每个连接一个，只在该连接的读协程中使用。
type clientMessageDecoder struct {
	fragmented bool      // 是否正在接收分片消息（已收到非 FIN 的数据帧，还没收到 FIN）
	fragOpCode ws.OpCode // 分片消息的类型，取自第一个分片
	fragData   []byte    // 分片消息已到达部分的数据（已去掩码）
}

// decode 解码 buff 开头所有完整到达的帧，返回解出的消息和消费的字节数。
// 每个完整的帧都会立即被消费：控制帧马上返回（包括分片消息中间插入的控制帧），
// 分片数据先累积在解码器中，等 FIN 到达后合成一条消息；
// 只有末尾不完整的帧留在 buff 中，等后续数据到达后再解码，
// 避免一次读取到“完整帧 + 后续半帧”时把半帧丢掉，导致后续帧边界错乱。
func (d *clientMessageDecoder) decode(buff []byte) ([]wsutil.Message, int, error) {
	var messages []wsutil.Message
	offset := 0
	reader := bytes.NewReader(nil)
	for offset < len(buff) {
		reader.Reset(buff[offset:])
		header, err := ws.ReadHeader(reader)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF { // 帧头不完整
				break
			}
			return nil, 0, err
		}
		state := ws.StateServerSide
		if d.fragmented {
			state = state.Set(ws.StateFragmented)
		}
		if err = ws.CheckHeader(header, state); err != nil {
			return nil, 0, err
		}
		if header.Length > int64(reader.Len()) { // 帧数据不完整
			break
		}
		payloadStart := len(buff) - reader.Len()
		offset = payloadStart + int(header.Length)
		// 复制一份：buff 中已消费的部分随后会被丢弃
		payload := make([]byte, header.Length)
		copy(payload, buff[payloadStart:offset])
		ws.Cipher(payload, header.Mask, 0)

		if header.OpCode.IsControl() {
			messages = append(messages, wsutil.Message{OpCode: header.OpCode, Payload: payload})
			continue
		}
		if !header.Fin {
			if !d.fragmented {
				d.fragmented = true
				d.fragOpCode = header.OpCode
			}
			d.fragData = append(d.fragData, payload...)
			continue
		}
		opCode := header.OpCode
		if d.fragmented {
			opCode = d.fragOpCode
			payload = append(d.fragData, payload...)
			d.fragmented, d.fragData = false, nil
		}
		if opCode == ws.OpText && !utf8.Valid(payload) {
			return nil, 0, wsutil.ErrInvalidUTF8
		}
		messages = append(messages, wsutil.Message{OpCode: opCode, Payload: payload})
	}
	return messages, offset, nil
}

func (w *WSConn) upgrade() error {
	buff, err := w.PeekFromTemp(-1)
	if err != nil {
		return err
	}
	tmpReader := bytes.NewReader(buff)
	tmpWriter := bytes.NewBuffer(nil)
	_, err = ws.Upgrade(&readWrite{
		Reader: tmpReader,
		Writer: tmpWriter,
	})
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF { //数据不完整
			return nil
		}
		w.DiscardFromTemp(len(buff)) // 发送错误，丢弃数据
		return err
	}

	// 解析http请求
	req, err := w.parseHttpRequest(buff)
	if err != nil {
		return err
	}

	realIp := w.getRealIp(req) // 获取真实ip
	realPortStr := req.Header.Get("X-Real-Port")
	if strings.TrimSpace(realIp) != "" {
		realPort := 0
		if strings.TrimSpace(realPortStr) != "" {
			realPort = wkutil.ParseInt(realPortStr)
		} else {
			if w.remoteAddr != nil {
				realPort = w.remoteAddr.(*net.TCPAddr).Port
			}
		}
		w.SetRemoteAddr(&net.TCPAddr{
			IP:   net.ParseIP(realIp),
			Port: realPort,
		})
	}

	_, err = w.Write(tmpWriter.Bytes())
	if err != nil {
		return err
	}

	w.DiscardFromTemp(len(buff) - tmpReader.Len())
	w.upgraded = true
	return nil
}

func (w *WSConn) getRealIp(r *http.Request) string {
	realIp := r.Header.Get("X-Forwarded-For")
	if strings.TrimSpace(realIp) == "" {
		realIp = r.Header.Get("X-Real-IP")
	}
	return realIp
}

func (w *WSConn) parseHttpRequest(data []byte) (*http.Request, error) {
	requestStr := string(data)

	// 创建一个虚拟的Request对象
	req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(requestStr)))
	if err != nil {
		fmt.Println("Error parsing request:", err)
		w.Error("Error parsing request", zap.Error(err))
		return nil, err
	}
	return req, nil
}

func (w *WSConn) PeekFromTemp(n int) ([]byte, error) {
	totalLen := w.tmpInboundBuffer.BoundBufferSize()
	if n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if w.tmpInboundBuffer.IsEmpty() {
		return nil, nil
	}
	head, tail := w.tmpInboundBuffer.Peek(n)
	w.reactorSub.cache.Reset()
	w.reactorSub.cache.Write(head)
	w.reactorSub.cache.Write(tail)

	data := w.reactorSub.cache.Bytes()
	return data, nil
}

func (w *WSConn) DiscardFromTemp(n int) {
	_, _ = w.tmpInboundBuffer.Discard(n)
}

func (w *WSConn) Close() error {
	_ = w.tmpInboundBuffer.Release()
	return w.DefaultConn.Close()
}

type readWrite struct {
	io.Reader
	io.Writer
}

type WSSConn struct {
	*TLSConn
	upgraded bool

	wsTmpInboundBuffer InboundBuffer // inboundBuffer InboundBuffer
	decoder            clientMessageDecoder
}

func NewWSSConn(tlsConn *TLSConn) *WSSConn {
	return &WSSConn{
		TLSConn:            tlsConn,
		wsTmpInboundBuffer: tlsConn.d.eg.eventHandler.OnNewInboundConn(tlsConn.d, tlsConn.d.eg), // tls解码后的数据
	}
}

func (w *WSSConn) ReadToInboundBuffer() (int, error) {
	readBuffer := w.d.reactorSub.ReadBuffer
	n, err := w.d.fd.Read(readBuffer)
	if err != nil || n == 0 {
		return 0, err
	}
	if w.d.eg.options.Event.OnReadBytes != nil {
		w.d.eg.options.Event.OnReadBytes(n)
	}

	_, err = w.tmpInboundBuffer.Write(readBuffer[:n])
	if err != nil {
		return 0, err
	}

	for {
		tlsN, err := w.tlsconn.Read(readBuffer)
		if err != nil {
			if err == tls.ErrDataNotEnough {
				return n, nil
			}
			return n, err
		}
		if tlsN == 0 {
			break
		}
		_, err = w.wsTmpInboundBuffer.Write(readBuffer[:tlsN])
		if err != nil {
			return n, err
		}
	}

	w.d.KeepLastActivity()

	err = w.unpacketWSData()
	return n, err
}

func (w *WSSConn) peekFromWSTemp(n int) ([]byte, error) {
	totalLen := w.wsTmpInboundBuffer.BoundBufferSize()
	if n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if w.wsTmpInboundBuffer.IsEmpty() {
		return nil, nil
	}
	head, tail := w.wsTmpInboundBuffer.Peek(n)
	w.d.reactorSub.cache.Reset()
	w.d.reactorSub.cache.Write(head)
	w.d.reactorSub.cache.Write(tail)

	data := w.d.reactorSub.cache.Bytes()
	return data, nil
}

func (w *WSSConn) discardFromWSTemp(n int) {
	_, _ = w.wsTmpInboundBuffer.Discard(n)
}

func (w *WSSConn) upgrade() error {
	buff, err := w.peekFromWSTemp(-1)
	if err != nil {
		return err
	}
	if len(buff) == 0 {
		return nil
	}

	tmpReader := bytes.NewReader(buff)
	tmpWriter := bytes.NewBuffer(nil)
	_, err = ws.Upgrade(&readWrite{
		Reader: tmpReader,
		Writer: tmpWriter,
	})
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF { //数据不完整
			return nil
		}
		w.discardFromWSTemp(len(buff)) // 发送错误，丢弃数据
		return err
	}
	_, err = w.TLSConn.Write(tmpWriter.Bytes())
	if err != nil {
		return err
	}

	w.discardFromWSTemp(len(buff) - tmpReader.Len())

	w.upgraded = true

	return nil
}

// 解包ws的数据
func (w *WSSConn) unpacketWSData() error {
	if !w.upgraded {
		err := w.upgrade()
		if err != nil {
			return err
		}
		return nil
	}

	messages, err := w.decode()
	if err != nil {
		return err
	}
	if len(messages) > 0 {
		for _, msg := range messages {
			if msg.OpCode.IsControl() {
				err = wsutil.HandleClientControlMessage(w.TLSConn, msg)
				if err != nil {
					return err
				}
				continue
			}
			if msg.OpCode == ws.OpPing {
				w.d.KeepLastActivity()
			}
			if len(msg.Payload) > 0 {
				_, err = w.d.inboundBuffer.Write(msg.Payload)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (w *WSSConn) Close() error {
	w.upgraded = false
	_ = w.wsTmpInboundBuffer.Release()
	return w.TLSConn.Close()
}

func (w *WSSConn) WriteServerBinary(data []byte) error {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	return wsutil.WriteServerBinary(w.TLSConn, data)
}

func (w *WSSConn) WriteServerText(data []byte) error {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	return wsutil.WriteServerText(w.TLSConn, data)
}

func (w *WSSConn) decode() ([]wsutil.Message, error) {
	buff, err := w.peekFromWSTemp(-1)
	if err != nil {
		return nil, err
	}
	messages, consumed, err := w.decoder.decode(buff)
	if err != nil {
		w.d.Warn("read client message error", zap.Error(err))
		w.discardFromWSTemp(len(buff)) // 发送错误，丢弃数据
		return nil, err
	}
	w.discardFromWSTemp(consumed)
	return messages, nil
}
