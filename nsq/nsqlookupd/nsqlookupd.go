package nsqlookupd

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex                       // 读写锁
	opts         *Options              // 参数配置信息
	tcpListener  net.Listener          // tcp 监听器用于监听 tcp 连接
	httpListener net.Listener          // 监听 http 连接
	waitGroup    util.WaitGroupWrapper // sync.WaitGroup 增强体，功能类似于 sync.WaitGroup，一般用于等待所有的go routine 全部退出
	DB           *RegistrationDB       // 生产者注册信息 DB
}

// 创建 NSQLookupd 实例
func New(opts *Options) (*NSQLookupd, error) {
	var err error

	// 1. 启用日志输出
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	// 2. 创建 NSQLookupd 实例
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	// 3. 版本号等信息
	l.logf(LOG_INFO, version.String("nsqlookupd"))
	// 4. 开启 tcp 和 http 连接监听
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}

	return l, nil
}

// Main starts an instance of nsqlookupd and returns an
// error if there was a problem starting up.
// 启动 NSQLookupd 实例
func (l *NSQLookupd) Main() error {
	// 1. 构建 Context 实例， Context 是 NSQLookupd 的一个 wrapper
	ctx := &Context{l}

	// 2. 创建进程退出前需要执行的 hook 函数
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				l.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// 3. 创建用于处理 tcp 连接的 handler，并开启 tcp 连接的监听动作
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		// 3.1 在 protocol.TCPServer 方法中统一处理监听
		exitFunc(protocol.TCPServer(l.tcpListener, tcpServer, l.logf))
	})
	// 4. 创建用于处理 http 连接的 handler，并开启 http 连接的监听动作
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(l.httpListener, httpServer, "HTTP", l.logf))
	})

	// 5. 阻塞等待错误退出
	err := <-exitCh
	return err
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}

// NSQLookupd 服务退出方法中，关闭了网络连接，并且需等待 hook 函数被执行
func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
