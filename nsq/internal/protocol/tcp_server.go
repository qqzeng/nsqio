package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"

	"github.com/nsqio/nsq/internal/lg"
)

// tcp 连接处理器，只是一个统一的入口，当 accept 到一个连接后，将此连接交给对应的 handler 处理
type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}
		// 针对每一个连接到 nsqd 的 client，会单独开启一个 goroutine 去处理
		go handler.Handle(clientConn)
	}

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
