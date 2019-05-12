package nsqlookupd

import (
	"net"
)

// ClientV1 代表一个连接到 NSQLookupd 的一个客户端
type ClientV1 struct {
	net.Conn           // 具备了 net.Conn 的方法，因此包含有 Read、Write和Close等方法
	peerInfo *PeerInfo // Client 实体在 NSQLookupd 端的一个用作网络通信的抽象视图
}

func NewClientV1(conn net.Conn) *ClientV1 {
	return &ClientV1{
		Conn: conn,
	}
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}
