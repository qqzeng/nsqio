package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type Client interface {
	Stats() ClientStats
	IsProducer() bool
}

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64							// nsqd 借助它为订阅的 client 生成 ID

	sync.RWMutex

	opts atomic.Value								// 配置参数实例

	dl        *dirlock.DirLock
	isLoading int32									// nsqd 当前是否处于启动加载过程
	errValue  atomic.Value
	startTime time.Time

	topicMap map[string]*Topic						// nsqd 所包含的 topic 集合

	clientLock sync.RWMutex							// guards clients
	clients    map[int64]Client						// 向 nsqd 订阅的 client 的集合，即订阅了此 nsqd 所维护的 topic 的客户端

	lookupPeers atomic.Value						// nsqd 与 nsqlookupd 之间的网络连接的抽象实体

	tcpListener   net.Listener						// tcp 连接 listener
	httpListener  net.Listener						// http 连接 listener
	httpsListener net.Listener						// https 连接 listener
	tlsConfig     *tls.Config

	poolSize int									// queueScanWorker 的数量，每一个 queueScanWorker 代表一个单独的 goroutine，用于处理消息队列

	notifyChan           chan interface{}			// 当 channel 或 topic 更新时（新增或删除），用于通知 nsqlookupd 服务更新对应的注册信息
	optsNotificationChan chan struct{}				// 当 nsqd 的配置发生变更时，可以通过此 channel 通知
	exitChan             chan int					// nsqd 退出开关
	waitGroup            util.WaitGroupWrapper		// waitGroup 的一个 wrapper 结构

	ci *clusterinfo.ClusterInfo
}

func New(opts *Options) (*NSQD, error) {
	var err error

	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		clients:              make(map[int64]Client),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})

	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("--data-path=%s in use (possibly by another instance of nsqd)", dataPath)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}

	if opts.StatsdPrefix != "" {
		var port string
		_, port, err = net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	n.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	n.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}

	return n, nil
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	return n.tcpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	return n.httpsListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) AddClient(clientID int64, client Client) {
	n.clientLock.Lock()
	n.clients[clientID] = client
	n.clientLock.Unlock()
}

func (n *NSQD) RemoveClient(clientID int64) {
	n.clientLock.Lock()
	_, ok := n.clients[clientID]
	if !ok {
		n.clientLock.Unlock()
		return
	}
	delete(n.clients, clientID)
	n.clientLock.Unlock()
}

// NSQD 进程启动入口程序
func (n *NSQD) Main() error {
	// 1. 构建 Context 实例， NSQD wrapper
	ctx := &context{n}

	// 2. 同 NSQLookupd 类似，构建一个退出 hook 函数，且在退出时仅执行一次
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// 3. 构建用于处理 tcp 连接的 tcp handler，同样注册退出前需要执行的函数（打印连接关闭错误信息）
	tcpServer := &tcpServer{ctx: ctx}
	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, tcpServer, n.logf))
	})
	// 4. 构建用于处理 http 连接的 http handler，注册错误打印函数
	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
	n.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
	})
	// 5. 若配置了 https 通信，则仍然构建 http 连接的 https handler（但同时开启 tls），同样注册错误打印函数
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
		httpsServer := newHTTPServer(ctx, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}
	// 6. 等待直到 queueScanLoop循环，lookupLoop循环以及statsdLoop，主程序才能退出
	// 即开启了 队列scan扫描 goroutine 以及  lookup 的查找 goroutine
	n.waitGroup.Wrap(n.queueScanLoop)
	n.waitGroup.Wrap(n.lookupLoop)
	if n.getOpts().StatsdAddress != "" {
		// 还有 状态统计处理 go routine
		n.waitGroup.Wrap(n.statsdLoop)
	}

	err := <-exitCh
	return err
}

// metadata 结构， Topic 结构的数组
type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"`
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"`
		} `json:"channels"`
	} `json:"topics"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// 加载 metadata
func (n *NSQD) LoadMetadata() error {
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)

	// 1. 构建 metadata 文件全路径， nsqd.dat，并读取文件内容
	fn := newMetadataFile(n.getOpts())

	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	// 2. 若文件内容为空，则表明是第一次启动， metadata 加载过程结束
	if data == nil {
		return nil // fresh start
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	// 3. 若文件内容不为空，则遍历所有 topic，针对每一个 topic 及 channel先前保持的情况进行还原．比如是否有被 pause，最后启动 topic
	for _, t := range m.Topics {
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		// 根据 topic name 获取对应的 topic 实例，若对应的 topic 实例不存在，则会创建它。
		// （因此在刚启动时，会创建所有之前保存的到文件中的 topic 实例，后面的 channel 也是类似的）
		topic := n.GetTopic(t.Name)
		if t.Paused {
			topic.Pause()
		}
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
		// 启动对应的 topic，开启了消息处理循环
		topic.Start()
	}
	return nil
}

// 创建 metadata 文件，遍历 nsqd 节点所有的 topic，
// 针对每一个非 ephemeral 属性的 topic，保存其 name、paused 属性（换言之不涉及到 topic 及 channel 的数据部分）
// 另外，保存 topic 所关联的非 ephemeral 的 channel 的 name、paused 属性
// 最后同步写入文件，注意在写文件，先是写到临时文件中，然后调用　OS.rename操作，以保证写入文件的原子性
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

// nsqd 退出
func (n *NSQD) Exit() {
	// 1. 关闭所有的网络连接监听
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	// 2. 持久化 topic 及 channel 的元信息数据 metadata，即不包括其所关联的 message
	// 然后关闭所有的 topic，因此也会递归地关闭 topic 下的 channels
	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()
	// 3. 发送退出信号 exitChan，退出当前进程中的所有循环
	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)
	n.waitGroup.Wait()
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
// GetTopic 是一个线程安全的方法，其根据 topic name 返回指向一个 topic 对象的指针，此 topic 对象有可能是新创建的
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	// 1. 通常，此 topic 已经被创建，因此（使用读锁）先从 nsqd 的 topicMap 中查询指定指定的 topic
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock()

	// 2. 因为上面查询指定的 topic 是否存在时，使用的是读锁，
	// 因此有线程可能同时进入到这里，执行了创建同一个 topic 的操作，因此这里还需要判断一次。
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	// 3. 创建删除指定 topic 的回调函数，即在删除指定的 topic 之前，需要做的一些清理工作，
	// 比如关闭 与此 topic 所关联的channel，同时判断删除此 topic 所包含的所有 channel
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	// 4. 通过 nsqd、topicName 和删除回调函数创建一个新的　topic，并将此 topic　添加到 nsqd 的 topicMap中
	// 创建 topic 过程中会初始化 diskqueue, 同时开启消息协程
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if loading metadata at startup, no lookupd connections yet, topic started after load
	// 5. 若 metadata 正在启动，且暂时没有 lookupd 连接，则在加载　topic　被加载之后，启动　topic
	// 此时内存中的两个消息队列 memoryMsgChan 和 backend 还未开始正常工作
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	// TODO
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		// 5.1 从指定的 nsqlookupd 及 topic 所获取的 channel 的集合
		// nsqlookupd 存储所有之前此 topic 创建的 channel 信息，因此需要加载消息
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		// 5.2 对那些非 ephemeral 的 channel，创建对应的实例（因为没有使用返回值，因此纯粹是更新了内在中的memoryMsgChan和backend结构）
		for _, channelName := range channelNames {
			// 对于临时的 channel，则不需要创建，使用的时候再创建
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			} // 5.3 根据 channel name 获取 channel 实例，且有可能是新建的
			// 若是新建了一个 channel，则通知 topic 的后台消息协程去处理 channel 的更新事件
			// 之所以在查询到指定 channel 的情况下，新建 channel，是为了保证消息尽可能不被丢失，
			// 比如在 nsq 重启时，需要在重启的时刻创建那些 channel，避免生产者生产的消息
			// 不能被放到 channel 中，因为在这种情况下，只能等待消费者来指定的 channel 中获取消息才会创建。
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	// 6. 启动了消息处理的循环，往 startChan 通道中 push 了一条消息，
	// 此时会内存消息队列 memoryMsgChan，以及持久化的消息队列 backendChan 就开始工作。
	// 即能处理内存中消息更新的的事件了。
	t.Start()
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
// 移除指定的 topic，若其存在。
// 同时，同时需要同时删除 topic 关联的所有的 channel，然后关闭它们，并将
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// 通知 nsqd 将 metadata 信息持久化到磁盘，若 nsqd 当前未处于启动过程
func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	// 考虑到若在 nsqd 刚启动处于加载元数据，则此时数据并不完整，因此不会在此时执行持久化操作
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if !persist {
				return
			}
			n.Lock()
			// 重新持久化 topic 及 channel 的元信息
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
// 返回　nsqd　所有的 channel
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
// 调整 queueScanWorker 的数量。
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	// 1. 根据 channel 的数量来设置合适的 pool size，默认为 1/4 的 channel 数量。
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	// 2. 开启一个循环，直到设置的 pool size 同实际的 pool size 相同才退出。
	// 否则，若理想值更大，则扩展已有的 queueScanWorker 的数量，即在一个单独的 goroutine 中调用一次 nsqd.queueScanWorker 方法（开启了一个循环）
	// 反之， 往 closeCh 中 push 一条消息，强制 queueScanWorker goroutine 退出
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanWorker
// and processes the deferred and in-flight queues
// 在 queueScanLoop 中处理 channel 的具体就是由 queueScanWorker 来负责。
// 调用此方法 queueScanWorker 即表示新增一个  queueScanWorker 来处理 channel
// 一旦开始工作 (从 workCh 中收到了信号， 即 dirty 的 channel 的数量超过 20个)，则循环的处理 in-flight queue 及 deferred queue，
// 并将处理结果（即是否是 dirty）通过 reponseCh 反馈给 queueScanWorker。
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) { // 若返回 true，则表明　in-flight 优先队列中有需要处理的　message，
											// 　因此其会将消息写入到　内存队列 memoryMsgChan　或后端持久化　backend
				dirty = true
			}
			if c.processDeferredQueue(now) { // 若返回 true，则表明　deferred 优先队列中有需要处理的　message
											// 　因此其会将消息写入到　内存队列 memoryMsgChan　或后端持久化　backend
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.

// queueScanLoop 方法在一个单独的 go routine 中运行。
// 用于处理正在发送的 in-flight 消息以及被延迟处理的 deferred 消息
// 它管理了一个 queueScanWork pool，其默认数量为5。queueScanWorker 可以并发地处理 channel。
// 它借鉴了 Redis 随机化超时的策略，即它每 QueueScanInterval 时间内（默认100ms）会从本地的缓存队列中
// 随机选择 QueueScanSelectionCount 个（默认20个） channels。其中 缓存队列每  QueueScanRefreshInterval 刷新。
// 若任何 queue 存在需要被处理的 channel，则此 queue 被标记为 dirty。此时循环不会休眠。
func (n *NSQD) queueScanLoop() {
	// 1. 获取随机选择的 channel 的数量，以及队列扫描的时间间隔，及队列刷新时间间隔
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)
	// 2. 获取 nsqd 所包含的 channel 集合
	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		// 每过 QueueScanInterval 时间（默认100ms），则开始随机挑选 QueueScanSelectionCount 个 channel。转到 loop: 开始执行
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		// 每过 QueueScanRefreshInterval 时间（默认5s），则调整 pool 的大小，即调整开启的 queueScanWorker 的数量为 pool 的大小
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		// nsqd 已退出
		case <-n.exitChan:
			goto exit
		}

		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		// 统计 dirty 的 channel 的数量，若其数量超过配置的 QueueScanDirtyPercent（默认为25%）
		// 则调用 util.UniqRands 函数，随机选取 num（QueueScanSelectionCount 默认20个）channel
		// 将它们 push 到 workCh 通道，queueScanWorker 中会收到此消息，然后调用处理 in-flight queue 和 deferred queue 中的消息
		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}
