package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqlookupd"
)

// 根据命令行参数配置 nsqlookupd 的参数
func nsqlookupdFlagSet(opts *nsqlookupd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqlookupd] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address of this lookupd node, (default to the OS hostname)")

	flagSet.Duration("inactive-producer-timeout", opts.InactiveProducerTimeout, "duration of time a producer will remain in the active list since its last ping")
	flagSet.Duration("tombstone-lifetime", opts.TombstoneLifetime, "duration of time a producer will remain tombstoned if registration remains")

	return flagSet
}

type program struct {
	once       sync.Once
	nsqlookupd *nsqlookupd.NSQLookupd
}

// nsqlookupd 服务程序执行入口
func main() {
	prg := &program{}
	// 1. 利用 svc 启动一个进程/服务，在 svc.Run 方法中会依次调用 Init 和 Start 方法，Init 和 Start 方法都是 no-blocking的；
	// 2. Run 方法会阻塞直到接收到 SIGINT(程序终止，如ctrl+c)或SIGTERM(程序结束信号，如kill -15 PID)，然后调用 stop方法后退出；
	// 3. 这是通过传递一个 channel 及感兴趣的信号集(SIGINT&SGITERM)给 signal.Notify 方法来实现此功能；
	// 4. Run方法中阻塞等待从 channel 中接收消息，一旦收到消息，则调用 stop 方法返回，进程退出。
	// 5. 更多可以查看 golang 标准包的 signal.Notify() 以及 github.com/judwhite/go-svc/svc 包是如何协助启动一个进程。
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	// 1. 默认初始化 nsqlookupd 配置参数
	opts := nsqlookupd.NewOptions()

	// 2. 根据命令行传递的参数更新默认参数
	flagSet := nsqlookupdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	// 3. 输出版本号并退出
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	// 4. 解析配置文件获取用户设置参数
	var cfg map[string]interface{}
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	// 5.  合并默认参数及配置文件中的参数
	options.Resolve(opts, flagSet, cfg)

	// 6. 创建 nsqlookupd 进程
	nsqlookupd, err := nsqlookupd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqlookupd", err)
	}
	p.nsqlookupd = nsqlookupd

	// 7. 执行 nsqlookupd 的主函数
	go func() {
		err := p.nsqlookupd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

// 进程退出方法，注意使用 sync.Once 来保证 nsqlookupd.Exit 方法只被执行一次
func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqlookupd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqlookupd] ", f, args...)
}
