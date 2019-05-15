package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

const maxTimeout = time.Hour

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocolV2 struct {
	ctx *context
}

// 针对连接到 nsqd 的每一个 client，都会单独在一个 goroutine 中执行这样一个 IOLoop 请求处理循环
func (p *protocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	clientID := atomic.AddInt64(&p.ctx.nsqd.clientIDSequence, 1)
	client := newClientV2(clientID, conn, p.ctx)
	p.ctx.nsqd.AddClient(client.ID, client)

	// synchronize the startup of messagePump in order
	// to guarantee that it gets a chance to initialize
	// goroutine local state derived from client attributes
	// and avoid a potential race with IDENTIFY (where a client
	// could have changed or disabled said attributes)
	// 1. 同步 messagePump 的启动过程，因为 messagePump会从client获取的一些属性
	// 而避免与执行 IDENTIFY 命令的 client 产生数据竞争，即当前client在后面可能修改相关的数据
	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	// 2. 开始循环读取 client 的请求，然后解析参数并处理
	for {
		// 如果在与客户端协商 negotiation过程中，客户端设置了 HeartbeatInterval，
		// 则在正常通信情况下，若间隔超过 2*HeartbeatInterval 未收到客户端的消息，
		// 则关闭连接。
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			// 若客户端未设置 HeartbeatInterval，则读取等待不会超时。
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		// 2.1 读取命令请求，并对它进行解析，解析命令的类型及参数
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)

		p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %s", client, params)

		var response []byte
		// 2.2 执行命令
		response, err = p.Exec(client, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}
		// 2.3 返回命令处理结果
		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting ioloop", client)
	conn.Close()
	close(client.ExitChan)
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	p.ctx.nsqd.RemoveClient(client.ID)
	return err
}

func (p *protocolV2) SendMessage(client *clientV2, msg *Message) error {
	p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): writing msg(%s) to client(%s) - %s", msg.ID, client, msg.Body)
	var buf = &bytes.Buffer{}

	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	client.writeLock.Lock()

	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval))
	} else {
		client.SetWriteDeadline(zeroTime)
	}

	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	if frameType != frameTypeMessage {
		err = client.Flush()
	}

	client.writeLock.Unlock()

	return err
}

// 根据客户端发送的命令请求的类型，调用不同的命令请求处理函数
func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("DPUB")):
		return p.DPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// nsqd 针对每一个 client 的订阅消息的处理循环
func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) {
	var err error
	var memoryMsgChan chan *Message
	var backendMsgChan chan []byte
	var subChannel *Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time
	var sampleRate int32
	// 1. 获取客户端的属性
	subEventChan := client.SubEventChan
	identifyEventChan := client.IdentifyEventChan
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout)
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C
	msgTimeout := client.MsgTimeout

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	// V2 版本的协议采用了选择性地将返回给 client 的数据进行缓冲，即通过减少系统调用频率来提高效率
	// 只有在两种情况下才采取显式地刷新缓冲数据
	// 		1. 当 client 还未准备好接收数据。
	// 			a. 若 client 所订阅的 channel 被 paused
	// 			b. client 的readyCount被设置为0，
	// 			c. readyCount小于当前正在发送的消息的数量 inFlightCount
	//		2. 当 channel 没有更多的消息给我发送了，在这种情况下，当前程序会阻塞在两个通道上
	flushed := true

	// signal to the goroutine that started the messagePump
	// that we've started up
	// 2. 向 IOLoop goroutine 发送消息，可以继续运行
	close(startedChan)

	for {// 1. 当前 client 未准备好接消息，原因包括 subChannel为空，即此客户端未订阅任何 channel
		// 或者客户端还未准备好接收消息，
		// 即 ReadyCount(通过 RDY 命令设置) <= InFlightCount(已经给此客户端正在发送的消息的数量) 或 ReadyCount <= 0
		// 刚开始进入循环是肯定会执行这个分支
		if subChannel == nil || !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			// 初始化各个消息 channel
			memoryMsgChan = nil
			backendMsgChan = nil
			flusherChan = nil
			// 强制刷新缓冲区
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			// 2. 表明上一个循环中，我们已经显式地刷新过
			// 准确而言，应该上从 client.SubEventChan 中接收到了 subChannel（client订阅某个 channel 导致的）
			// 因此 初始化 memoryMsgChan 和 backendMsgChan 两个 channel，实际上这两个 channel 即为 client 所订阅的 channel的两个消息队列
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backend.ReadChan()
			// 同时，禁止从 flusherChan 取消息，因为才刚刚设置接收消息的 channel，缓冲区不会数据等待刷新
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			// 3. 在执行到此之前，subChannel 肯定已经被设置过了，且已经从 memoryMsgChan 或 backendMsgChan 取出过消息
			// 因此，可以准备刷新消息发送缓冲区了，即设置 flusherChan
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = outputBufferTicker.C
		}

		select {
		// 4. 定时刷新消息发送缓冲区
		case <-flusherChan:
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		// 5. 客户端处理消息的能力发生了变化，比如客户端刚消费了某个消息
		case <-client.ReadyStateChan:
		// 6. 发现 client 订阅了某个 channel，channel 是在 SUB命令请求方法中被压入的
		// 然后，将 subEventChan 重置为nil，重置为 nil原因表示后面不能从此 channel 中接收到消息（注意与close(channel)的区别）
		// 而置为nil的原因是，在SUB命令请求方法中第一行即为检查此客户端是否处于 stateInit 状态，而调用 SUB 了之后，状态变为 stateSubscribed
		case subChannel = <-subEventChan:
			// you can't SUB anymore
			subEventChan = nil
		// 7. 当 nsqd 收到 client 发送的 IDENTIFY 请求时，会设置此 client 的属性信息，然后将信息 push 到	identifyEventChan。
		// 因此此处就会收到一条消息，同样将 identifyEventChan 重置为nil，
		// 这表明只能从 identifyEventChan 通道中接收一次消息，因为在一次连接过程中，只允许客户端初始化一次
		// 在 IDENTIFY 命令处理请求中可看到在第一行时进行了检查，若此时客户端的状态不是 stateInit，则会报错。
		// 最后，根据客户端设置的信息，更新部分属性，如心跳间隔 heartbeatTicker
		case identifyData := <-identifyEventChan:
			// you can't IDENTIFY anymore
			identifyEventChan = nil

			outputBufferTicker.Stop()
			if identifyData.OutputBufferTimeout > 0 {
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 {
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 {
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout
		// 8. 定时向所连接的客户端发送 heartbeat 消息
		case <-heartbeatChan:
			err = p.Send(client, frameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
			}
		// 9. 从 backendMsgChan 队列中收到了消息
		case b := <-backendMsgChan:
			// 根据 client 在与 nsqd 建立连接后，第一次 client 会向 nsqd 发送 IDENFITY 请求来提供 client 的一些信息
			// 即为 identifyData，而 sampleRate 就包含在其中。换言之，客户端会发送一个 0-100 的数字给 nsqd，
			// 在 nsqd 服务端，它通过从 0-100 之间随机生成一个数字，若其大于 客户端发送过来的数字 sampleRate
			// 则 client 虽然订阅了此 channel，且此 channel 中也有消息了，但是不会发送给此 client。
			// 这里就体现了 官方文档 中所说的，当一个 channel 被 client 订阅时，它会将收到的消息随机发送给这一组 client 中的一个
			// 而且，就算只有一个 client，从程序中来看，也不一定能够获取到此消息，具体情况也与 client 编写的程序规则相关
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			// 将消息解码
			msg, err := decodeMessage(b)
			if err != nil {
				p.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
			// 递增消息尝试发送次数
			msg.Attempts++ // 注意： 当消息发送的次数超过一定限制时，可由 client 自己在应用程序中做处理
			// 调用client 所订阅的 channel 的 StartInFlightTimeout 方法，将消息加入到 in-flight 队列
			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			// 更新client 的关于正在发送消息的属性
			client.SendingMessage()
			// 正式发送消息到指定的 client
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
			// 重置 flused 变量
			flushed = false
		// 9. 从 memoryMsgChan 队列中收到了消息
		case msg := <-memoryMsgChan:
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			msg.Attempts++

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			client.SendingMessage()
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
			flushed = false
		// 10. 客户端退出
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
}

// IDENTIFY 表示 nsqd 收到 client 给自己发送的身份确认信息
func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	var err error
	//  1. 校验 client 的状态
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}
	// 2. 读取请求的内容长度，并进行校验
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}
	// 3. 根据请求体读取请求内容，请求体中的信息是客户端（即producer）的一些信息，并反序列化成 identifyDataV2 实例
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %+v", client, identifyData)
	// 4. 设置此 client 的身份属性信息 identifyData
	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	// 5. 若未设置允许双方协商一些属性的设置（比如是否开启 tls、是否启用压缩及压缩级别），则直接返回 Ok
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.ctx.nsqd.tlsConfig != nil && identifyData.TLSv1
	deflate := p.ctx.nsqd.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 6
	if deflate && identifyData.DeflateLevel > 0 {
		deflateLevel = identifyData.DeflateLevel
	}
	if max := p.ctx.nsqd.getOpts().MaxDeflateLevel; max < deflateLevel {
		deflateLevel = max
	}
	snappy := p.ctx.nsqd.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}
	// 6. 构造响应实体，并将响应返回，同时后面还会对一些设置，针对性地返回 ok
	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
	}{
		MaxRdyCount:         p.ctx.nsqd.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.ctx.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.ctx.nsqd.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.ctx.nsqd.IsAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to deflate (level %d)", client, deflateLevel)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

func (p *protocolV2) AUTH(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot AUTH in current state")
	}

	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH invalid number of parameters")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH already set")
	}

	if !client.ctx.nsqd.IsAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH disabled")
	}

	if err := client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		p.ctx.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH no authorizations found")
	}

	resp, err := json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

func (p *protocolV2) CheckAuth(client *clientV2, cmd, topicName, channelName string) error {
	// if auth is enabled, the client must have authorized already
	// compare topic/channel against cached authorization data (refetching if expired)
	if client.ctx.nsqd.IsAuthEnabled() {
		if !client.HasAuthorizations() {
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// we don't want to leak errors contacting the auth server to untrusted clients
			p.ctx.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil
}

// 客户端在指定的 topic 上订阅消息
func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	// 1. 做一些校验工作，只有当 client 处于 stateInit 状态才能订阅某个 topic 的 channel
	// 换言之，当一个 client 订阅了某个 channel 之后，它的状态会被更新为 stateSubscribed，因此不能再订阅 channel 了
	// 总而言之，一个 client 只能订阅一个 channel
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}
	// 2. 获取订阅的 topic 名称、channel 名称，并对它们进行校验
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	channelName := string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}
	// 3. 同时检查此客户端是否有订阅的权限
	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	// This retry-loop is a work-around for a race condition, where the
	// last client can leave the channel between GetChannel() and AddClient().
	// Avoid adding a client to an ephemeral channel / topic which has started exiting.
	// 此循环是为了避免 client 订阅到正在退出的 ephemeral 属性的 channel 或 topic
	var channel *Channel
	for {
		// 4. 获取 topic 及 channel 实例
		topic := p.ctx.nsqd.GetTopic(topicName)
		channel = topic.GetChannel(channelName)
		// 5. 调用 channel的 AddClient 方法添加指定客户端
		if err := channel.AddClient(client.ID, client); err != nil {
			return nil, protocol.NewFatalClientErr(nil, "E_TOO_MANY_CHANNEL_CONSUMERS",
				fmt.Sprintf("channel consumers for %s:%s exceeds limit of %d",
					topicName, channelName, p.ctx.nsqd.getOpts().MaxChannelConsumers))
		}
		// 6. 若此 channel 或 topic 是 ephemeral的，并且 channel或 topic 正在退出，则移除此 client
		if (channel.ephemeral && channel.Exiting()) || (topic.ephemeral && topic.Exiting()) {
			channel.RemoveClient(client.ID)
			time.Sleep(1 * time.Millisecond)
			continue
		}
		break
	}
	// 6. 修改客户端的状态为 stateSubscribed
	atomic.StoreInt32(&client.State, stateSubscribed)
	// 7. 这一步比较关键，将订阅的 channel 实例传递给了 client，同时将 channel 发送到了 client.SubEventChan 通道中
	// 后面的 SubEventChan 就会使得当前的 client 在一个 goroutine 中订阅这个 channel 的消息
	client.Channel = channel
	// update message pump
	// 8. 通知后台订阅协程来订阅消息,包括内存管道和磁盘
	client.SubEventChan <- channel
	// 9. 返回 ok
	return okBytes, nil
}

func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		// just ignore ready changes on a closing channel
		p.ctx.nsqd.logf(LOG_INFO,
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.ctx.nsqd.getOpts().MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ctx.nsqd.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

// 消费者 client 收到消息后，会向 nsqd　响应　FIN+msgID　通知服务器成功投递消息，可以清空消息了'
func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	// 1. 正式处理　FIN　请求前，对 client 及 请求参数属性信息进行校验
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}
	// 2. 获取 msgID
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}
	// 3. client 调用 channel.FinishMessage 方法，即将消息从 channel 的 in-flight queue 及 inFlightMessages 字典中移除
	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", *id, err.Error()))
	}
	// 4. 更新 client 维护的消息消费的统计信息
	client.FinishedMessage()

	return nil, nil
}

// nsqd 为此 client 将 message 重新入队，并指定是否需要延时发送
func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) {
	// 1. 先检验 client 的状态以及参数信息
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}
	// 2. 从请求中取出重入队的消息被延迟的时间，并转化单位，同时限制其不能超过最大的延迟时间 maxReqTimeout
	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	maxReqTimeout := p.ctx.nsqd.getOpts().MaxReqTimeout
	clampedTimeout := timeoutDuration

	if timeoutDuration < 0 {
		clampedTimeout = 0
	} else if timeoutDuration > maxReqTimeout {
		clampedTimeout = maxReqTimeout
	}
	if clampedTimeout != timeoutDuration {
		p.ctx.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] REQ timeout %d out of range 0-%d. Setting to %d",
			client, timeoutDuration, maxReqTimeout, clampedTimeout)
		timeoutDuration = clampedTimeout
	}
	// 3. 调用 channel.RequeueMessage 将消息重新入队。首先会将其从 in-flight queue 中删除，
	// 然后依据其 timeout 而定，若其为0,则直接将其添加到消息队列中，
	// 否则，若其 timeout 不为0,则构建一个 deferred message，并设置好延迟时间为 timeout，并将其添加到 deferred queue 中
	err = client.Channel.RequeueMessage(client.ID, *id, timeoutDuration)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", *id, err.Error()))
	}
	// 4. 更新 client 保存的关于消息的统计计数
	client.RequeuedMessage()

	return nil, nil
}

func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

// 客户端在指定的 topic 上发布消息
func (p *protocolV2) PUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}
	// 1. 读取 topic 名称
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}
	// 2. 读取消息体长度 bodyLen，并在长度上进行校验
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}
	// 3. 读取指定字节长度的消息内容到 messageBody
	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}
	// 4. 检查客户端是否具备 PUB 此 topic 命令的权限
	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return nil, err
	}
	// 5. 获取 topic 实例
	topic := p.ctx.nsqd.GetTopic(topicName)
	// 6. 构造一条 message，并将此 message 投递到此 topic 的消息队列中
	msg := NewMessage(topic.GenerateID(), messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}
	// 7. 开始发布此消息，即将对应的 client 修改为此 topic 保存的消息的计数。c.pubCounts[topic] += count
	client.PublishedMessage(topicName, 1)

	// 回复 Ok
	return okBytes, nil
}

// 客户端在指定的 topic 上发布多条消息，逻辑同发布单条消息类似
func (p *protocolV2) MPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name %q is not valid", topicName))
	}

	if err := p.CheckAuth(client, "MPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	messages, err := readMPUB(client.Reader, client.lenSlice, topic,
		p.ctx.nsqd.getOpts().MaxMsgSize, p.ctx.nsqd.getOpts().MaxBodySize)
	if err != nil {
		return nil, err
	}

	// if we've made it this far we've validated all the input,
	// the only possible error is that the topic is exiting during
	// this next call (and no messages will be queued in that case)
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, uint64(len(messages)))

	return okBytes, nil
}

func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "DPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("DPUB topic name %q is not valid", topicName))
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("DPUB could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.ctx.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("DPUB timeout %d out of range 0-%d",
				timeoutMs, p.ctx.nsqd.getOpts().MaxReqTimeout/time.Millisecond))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body")
	}

	if err := p.CheckAuth(client, "DPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	msg.deferred = timeoutDuration
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_DPUB_FAILED", "DPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, 1)

	return okBytes, nil
}

// 为客户端重置正在发送的消息的超时时间
func (p *protocolV2) TOUCH(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	client.writeLock.RLock()
	msgTimeout := client.MsgTimeout
	client.writeLock.RUnlock()
	err = client.Channel.TouchMessage(client.ID, *id, msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", *id, err.Error()))
	}

	return nil, nil
}

func readMPUB(r io.Reader, tmp []byte, topic *Topic, maxMessageSize int64, maxBodySize int64) ([]*Message, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 4 == total num, 5 == length + min 1
	maxMessages := (maxBodySize - 4) / 5
	if numMessages <= 0 || int64(numMessages) > maxMessages {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		messages = append(messages, NewMessage(topic.GenerateID(), msgBody))
	}

	return messages, nil
}

// validate and cast the bytes on the wire to a message ID
func getMessageID(p []byte) (*MessageID, error) {
	if len(p) != MsgIDLength {
		return nil, errors.New("Invalid Message ID")
	}
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func enforceTLSPolicy(client *clientV2, p *protocolV2, command []byte) error {
	if p.ctx.nsqd.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
