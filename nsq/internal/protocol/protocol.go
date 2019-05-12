package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Protocol： 系统中协议行为规范的抽象

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
// 为数据添加一个头部，头部内容为数据的长度。并将数据写入到指定的 Writer 中，返回总共写入的长度。
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
// 相比 SendResponse 方法，其还要将一个数据帧添加到数据头部，同时更新返回的长度字段为 n+8
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}
