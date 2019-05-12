package nsq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

var byteSpace = []byte(" ")
var byteNewLine = []byte("\n")

// Command represents a command from a client to an NSQ daemon
// Command 代表 nsq 守护程序从客户端收到的一个命令请求
type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
}

// String returns the name and parameters of the Command
func (c *Command) String() string {
	if len(c.Params) > 0 {
		return fmt.Sprintf("%s %s", c.Name, string(bytes.Join(c.Params, byteSpace)))
	}
	return string(c.Name)
}

// WriteTo implements the WriterTo interface and
// serializes the Command to the supplied Writer.
//
// It is suggested that the target Writer is buffered
// to avoid performing many system calls.
// 将指定的命令（包括命令的 name、params，一个空行以及body（写body之前要先写入其长度））写入到指定的输出流中，带缓冲
func (c *Command) WriteTo(w io.Writer) (int64, error) {
	var total int64
	var buf [4]byte

	n, err := w.Write(c.Name)
	total += int64(n)
	if err != nil {
		return total, err
	}

	for _, param := range c.Params {
		n, err := w.Write(byteSpace)
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write(param)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	n, err = w.Write(byteNewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	if c.Body != nil {
		bufs := buf[:]
		binary.BigEndian.PutUint32(bufs, uint32(len(c.Body)))
		n, err := w.Write(bufs)
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write(c.Body)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// Identify creates a new Command to provide information about the client.  After connecting,
// it is generally the first message sent.
//
// The supplied map is marshaled into JSON to provide some flexibility
// for this command to evolve over time.
//
// See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
// on the supported options
// IDENTIFY 命令请求通常在连接建立后被第一个发送的消息，以标识客户端的信息
func Identify(js map[string]interface{}) (*Command, error) {
	body, err := json.Marshal(js)
	if err != nil {
		return nil, err
	}
	return &Command{[]byte("IDENTIFY"), nil, body}, nil
}

// Auth sends credentials for authentication
//
// After `Identify`, this is usually the first message sent, if auth is used.
func Auth(secret string) (*Command, error) {
	return &Command{[]byte("AUTH"), nil, []byte(secret)}, nil
}

// Register creates a new Command to add a topic/channel for the connected nsqd
func Register(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("REGISTER"), params, nil}
}

// UnRegister creates a new Command to remove a topic/channel for the connected nsqd
func UnRegister(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("UNREGISTER"), params, nil}
}

// Ping creates a new Command to keep-alive the state of all the
// announced topic/channels for a given client
func Ping() *Command {
	return &Command{[]byte("PING"), nil, nil}
}

// Publish creates a new Command to write a message to a given topic
func Publish(topic string, body []byte) *Command {
	var params = [][]byte{[]byte(topic)}
	return &Command{[]byte("PUB"), params, body}
}

// DeferredPublish creates a new Command to write a message to a given topic
// where the message will queue at the channel level until the timeout expires
func DeferredPublish(topic string, delay time.Duration, body []byte) *Command {
	var params = [][]byte{[]byte(topic), []byte(strconv.Itoa(int(delay / time.Millisecond)))}
	return &Command{[]byte("DPUB"), params, body}
}

// MultiPublish creates a new Command to write more than one message to a given topic
// (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
func MultiPublish(topic string, bodies [][]byte) (*Command, error) {
	var params = [][]byte{[]byte(topic)}

	num := uint32(len(bodies))
	bodySize := 4
	for _, b := range bodies {
		bodySize += len(b) + 4
	}
	body := make([]byte, 0, bodySize)
	buf := bytes.NewBuffer(body)

	err := binary.Write(buf, binary.BigEndian, &num)
	if err != nil {
		return nil, err
	}
	for _, b := range bodies {
		err = binary.Write(buf, binary.BigEndian, int32(len(b)))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(b)
		if err != nil {
			return nil, err
		}
	}

	return &Command{[]byte("MPUB"), params, buf.Bytes()}, nil
}

// Subscribe creates a new Command to subscribe to the given topic/channel
// SUB 命令表示：客户端（消费者 consumer）通知 nsq，其需要订阅的 topic 或 channel
func Subscribe(topic string, channel string) *Command {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &Command{[]byte("SUB"), params, nil}
}

// Ready creates a new Command to specify
// the number of messages a client is willing to receive
// RDY 命令表示： 客户端（消费者 consumer）通知 nsq，其一次性能够接收的最大的消息的数量
func Ready(count int) *Command {
	var params = [][]byte{[]byte(strconv.Itoa(count))}
	return &Command{[]byte("RDY"), params, nil}
}

// Finish creates a new Command to indiciate that
// a given message (by id) has been processed successfully
// FIN 命令表示：客户端（消费者 consumer）通知 nsq指定ID的命令请求已经执行成功
func Finish(id MessageID) *Command {
	var params = [][]byte{id[:]}
	return &Command{[]byte("FIN"), params, nil}
}

// Requeue creates a new Command to indicate that
// a given message (by id) should be requeued after the given delay
// NOTE: a delay of 0 indicates immediate requeue
// REQ 命令表示：客户端（消费者 consumer）通知 nsq指定ID的命令请求已经执行成功
func Requeue(id MessageID, delay time.Duration) *Command {
	var params = [][]byte{id[:], []byte(strconv.Itoa(int(delay / time.Millisecond)))}
	return &Command{[]byte("REQ"), params, nil}
}

// Touch creates a new Command to reset the timeout for
// a given message (by id)
// TOUCH 命令表示：客户端（消费者 consumer）通知 nsq重置某条消息的剩余执行时间
func Touch(id MessageID) *Command {
	var params = [][]byte{id[:]}
	return &Command{[]byte("TOUCH"), params, nil}
}

// StartClose creates a new Command to indicate that the
// client would like to start a close cycle.  nsqd will no longer
// send messages to a client in this state and the client is expected
// finish pending messages and close the connection
// CLS 命令表示：客户端（消费者 consumer）通知 nsq 其不再接收任何消息，并且在 pending 的消息处理完成后，就会关闭连接
func StartClose() *Command {
	return &Command{[]byte("CLS"), nil, nil}
}

// Nop creates a new Command that has no effect server side.
// Commonly used to respond to heartbeats
// NOP 命令表示：客户端（消费者 consumer）此命令不需要在对端执行，通常是对于 heartbeat 命令的回复
func Nop() *Command {
	return &Command{[]byte("NOP"), nil, nil}
}
