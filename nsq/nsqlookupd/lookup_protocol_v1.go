package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

// LookupProtocolV1： Context/NSQLookupd 的一个 wrapper。是 protocol.Protocol 的一个实现。
// nsqd 使用 tcp 接口来广播
type LookupProtocolV1 struct {
	ctx *Context
}

func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string
	// 1. 先创建客户端实例，并构建对应的 reader
	client := NewClientV1(conn)
	reader := bufio.NewReader(client)
	for {
		// 2. 读取一行内容，并分离出参数信息
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		// 3. 调用 Exec 方法获取响应内容
		var response []byte
		response, err = p.Exec(client, reader, params)
		if err != nil { // 4. Exec 方法执行失败，返回对应的异常信息
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		// 5. 执行成功，则返回响应内容
		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	// 6. 连接关闭时，清除  client(nsqd) 在 NSQLookupd 注册的信息
	conn.Close()
	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): closing", client)
	if client.peerInfo != nil {
		registrations := p.ctx.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}
	return err
}

func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// 由 client 发送的参数解析出 topic 名称，以及 channel 名称（若有的话）
func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

//  Client 向 NSQLookupd 发送注册/订阅 topic 的消息。注意，当消息中带有 channel 时，对于此 client会注册两个 producer，分别针对 channel 和 topic。
func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	// 1. 必须先要发送 IDENTIFY 消息进行身份认证。
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	// 2. 获取 client 注册的 topic 和 channel(若有的话)
	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	// 3. 若 channel 不为空，则向 DB 中添加一个 Producer 实例，其键(Category)为 channel 类型的 Registration。
	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	// 4. 若 topic 不为空，则还需要向 DB 中添加一个 Producer 实例，其键(Category)为 topic 类型的 Registration。
	key := Registration{"topic", topic, ""}
	if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}
	// 5. 返回 OK
	return []byte("OK"), nil
}

//  Client 向 NSQLookupd 发送取消注册/订阅 topic 的消息。即为 REGISTER 的逆过程。
func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	// 1. 必须先要发送 IDENTIFY 消息进行身份认证。
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	// 2. 获取 client 注册的 topic 和 channel(若有的话)
	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	// 3. 若 channel 不为空，则在 DB 中移除一个 Producer 实例，其键(Category)为 channel 类型的 Registration。
	if channel != "" {
		key := Registration{"channel", topic, channel}
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// 对于 ephemeral 类型的 channel，若它没有被任何 Producer 订阅，则需要移除此 channel 代表的 Registration 对象。
		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		// 4. 取消注册 topic。因此它会删除掉 类型(Category)为 channel 且 Key 为 topic 且 subKey不限的　Registration 集合
		// 也会删除掉类型(Category)为 topic 且 Key 为 topic　且subKey为 "" 的　Registration集合。
		registrations := p.ctx.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id)
			if removed {
				p.ctx.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		// 同样，对于 ephemeral 类型的 topic，若它没有被任何 Producer 订阅，则需要移除此 channel 代表的 Registration 对象。
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	}

	return []byte("OK"), nil
}

// client 向 NSQLookupd 发送认证身份的消息。 注意在此过程中会将客户端构造成Producer添加到 Registration DB中。
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	// 1. client 不能重复发送 IDENTIFY 消息
	if client.peerInfo != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}
	// 2. 读取消息体的长度
	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}
	// 3. 读取消息体内容，包含生产者的信息
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}
	// 4. 根据消息体构建 PeerInfo 实例
	// body is a json structure with producer information
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String()

	// 5. 检验属性不能为空，同时更新上一次PING的时间
	// require all fields
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)

	// 6. 将此 client 构建成一个 Producer 注册到 DB中
	client.peerInfo = &peerInfo
	if p.ctx.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// 7. 构建响应消息，包含 NSQLookupd 的 hostname、port及 version
	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = p.ctx.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.ctx.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.ctx.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname

	response, err := json.Marshal(data)
	if err != nil {
		p.ctx.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

// PING 消息： client 在发送其它命令之前，可能会先发送一个 PING 消息。
func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		// 打印 PING 日志
		p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur))
		// 更新上一次PING的时间
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	// 回复 OK
	return []byte("OK"), nil
}
