package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64					// 需要重新排队的消息数
	messageCount uint64					// 接收到的消息的总数
	timeoutCount uint64					// 正在发送的消息的数量

	sync.RWMutex						// guards

	topicName string					// 其所对应的 topic 名称
	name      string					// channel 名称
	ctx       *context					// nsqd 实例

	backend BackendQueue				// 后端消息持久化的队列
	// 内存消息通道。 其关联的 topic 会向此 channel 发送消息，且所有订阅的 client 会开启一个 go routine 订阅此 channel
	memoryMsgChan chan *Message
	exitFlag      int32					// 退出标识（同 topic 的 exitFlag 作用类似）
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer	// 与此 channel 相联的 client 集合，准确而言是 Consumer 集合
	paused         int32				// 若其 paused属性被设置，则那些订阅了此`channel`的客户端不会被推送消息
	ephemeral      bool					// 标记此 channel 是否是临时的
	deleteCallback func(*Channel)		// 删除回调函数（同 topic 的 deleteCallback 作用类似）
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	// 延迟投递消息集合，消息体会放入 deferredPQ，并且由后台的queueScanLoop协程来扫描消息
	// 将过期的消息照常使用 c.put(msg) 发送出去。
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue		// 被延迟投递消息集合 对应的 优先级队列 PriorityQueue
	deferredMutex    sync.Mutex					// guards deferredMessages
	// 正在发送中的消息记录集合，直到收到客户端的 FIN 才删除，否则一旦超过 timeout，则重传消息。
	// （因此client需要对消息做去重处理 de-duplicate）
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue				// 正在发送中的消息记录集合 对应的 inFlightPqueue
	inFlightMutex    sync.Mutex					// guards inFlightMessages
}

// NewChannel creates a new instance of the Channel type and returns a pointer
// channel 构造函数
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {
	// 1. 初始化 channel 部分参数
	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	// 2. 初始化 channel 维护的两个消息队列
	c.initPQ()
	// 3. 同　topic　类似，那些 ephemeral 类型的 channel 不会关联到一个 BackendQueue，而只是被赋予了一个 dummy BackendQueue
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// backend names, for uniqueness, automatically include the topic...
		// 4. 实例化一个后端持久化存储，同样是通过 go-diskqueue  来创建的，其初始化参数同 topic 中实例化 backendQueue 参数类似
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}
	// 5. 通知 lookupd 添加注册信息
	c.ctx.nsqd.Notify(c)

	return c
}

// in-flight queue 及 deferred queue 初始化
func (c *Channel) initPQ() {
	// 默认队列大小为 MemQueueSize/10
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
// 删除此 channel，清空所有消息，然后关闭
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
// 只是将三个消息队列中的消息刷盘，然后关闭
func (c *Channel) Close() error {
	return c.exit(false)
}

//
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()
	// 1. 保证还未被设置 exitFlag，即还在运行中，同时设置 exitFlag
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}
	// 2. 若需要删除数据，则通知 nsqlookupd，有 channel 被删除
	if deleted {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	// 3. 强制关闭所有订阅了此 channel 的客户端
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()
	// 4. 清空此 channel 所维护的内存消息队列和持久化存储消息队列中的消息
	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		// 5. 删除持久化存储消息队列中的消息
		return c.backend.Delete()
	}

	// write anything leftover to disk
	// 6. 强制将内存消息队列、以及两个发送消息优先级队列中的消息写到持久化存储中
	c.flush()
	// 7. 关闭持久化存储消息队列
	return c.backend.Close()
}

// 清空 channel 的消息
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()
	// 1. 重新初始化（清空） in-flight queue 及 deferred queue
	c.initPQ()
	// 2. 清空由 channel 为客户端维护的一些信息，比如 当前正在发送的消息的数量 InFlightCount
	// 同时更新了 ReadyStateChan
	for _, client := range c.clients {
		client.Empty()
	}
	// 3. 将 memoryMsgChan 中的消息清空
	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

	// 4. 最后将后端持久化存储中的消息清空
finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
// 将未消费的消息都写到持久化存储中，主要包括三个消息集合：memoryMsgChan、inFlightMessages和deferredMessages
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}
	// 1. 将内存消息队列中的积压的消息刷盘
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

	// 2. 将还未发送出去的消息 inFlightMessages 也写到持久化存储
finish:
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()
	// 3. 将被推迟发送的消息集合中的 deferredMessages 消息也到持久化存储
	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
// 此方法会由 topic.messagePump 方法中调用。
// 即当 topic 收到生产者投递的消息时，将此消息放到与其关联的 channels 的延迟队列 deferred queue
// 或者 普通的消息队列中(包括 内存消息队列 memoryMsgChan 或 后端持久化 backend)（即此方法）
// channel 调用 put 方法将消息放到消息队列中，同时更新消息计数
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}
// 同 topic.put 方法类似，其在 put message 时，依据实际情况将消息 push 到内在队列 memoryMsgChan 或者后端持久化 backend
func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m:
	default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// 将 message 添加到 deferred queue 中
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage resets the timeout for an in-flight message
// 重置正在发送的消息的超时时间，即将消息从 inFlightMessages 及 in-flight queue 中弹出
// 然后更新其超时时间为指定的超时时间，再将消息压入到两个集合中
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
// 丢弃 in-flight 中指定的消息，因为此消息已经被消费者成功消费
// 即将消息从 channel 的 in-flight queue 及 inFlightMessages 字典中移除
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// 同时，记录此次消费的延迟情况
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
// 将消息重新入队。这与 timeout 参数密切相关。
// 当 timeout == 0 时，直接将此消息重入队。否则，异步等待此消息超时，然后 再将此消息重入队，即是相当于消息被延迟了
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// 1. 先将消息从 inFlightMessages 移除
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	// 2. 同时将消息从 in-flight queue 中移除，并更新 chanel 维护的消息重入队数量 requeueCount
	c.removeFromInFlightPQ(msg)
	atomic.AddUint64(&c.requeueCount, 1)

	// 3. 若 timeout 为0,则将消息重新入队。即调用 channel.put 方法，将消息添加到 memoryMsgChan 或 backend
	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	// 否则，创建一个延迟消息，并设置延迟时间
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
// 添加一个 client 到 channel 的 client 列表。
// client 使用 tcp协议发送一个 SUB 命令请求。
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return nil
	}
	// 检查是否超过了最大的 client 的数量了
	maxChannelConsumers := c.ctx.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && len(c.clients) >= maxChannelConsumers {
		return errors.New("E_TOO_MANY_CHANNEL_CONSUMERS")
	}

	c.clients[clientID] = client
	return nil
}

// RemoveClient removes a client from the Channel's client list
// 从channel 的 client 列表移除一个 client。
// 当一个 ephemeral 的 channel的所有的 client 全部都被移除后，则其也会被删除（对于 ephemeral属性的 topic 也是类似）
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// 设置 in-flight message 计时属性，同时将此 message 加入到 in-flight queue 中，等待被 queueScanWorker 处理
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	// 1. 设置 message 属性，特别是 message.pri 即为当前消息处理时间的 deadline，超过此时间，则不再处理
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	// 2. 将 message 添加到 inFlightMessages 字典
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	// 3. 将消息添加到 in-flight 优先级队列中
	c.addToInFlightPQ(msg)
	return nil
}

// 将 message 加入到 deferred queue 中，等待被 queueScanWorker 处理
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	// 1. 计算超时超时戳，作为 priority
	absTs := time.Now().Add(timeout).UnixNano()
	// 2. 构造 item
	item := &pqueue.Item{Value: msg, Priority: absTs}
	// 3. item 添加到 deferred 字典
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	// 4. 将 item 放入到 deferred message 优先级队列
	c.addToDeferredPQ(item)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
// 将 message 添加到 in-flight 字典 < msg.ID, message>
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
// 将 message 从 inFlightMessages 字典中移除，并且需要考虑消息的 client ID 是否 match
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// 将 item/message 添加到 deferredMessages 字典
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

// 将 item/message 从 deferredMessages 字典中删除
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// queueScanWorker 循环处理 deferred queue
func (c *Channel) processDeferredQueue(t int64) bool {
	// 1. 保证 channel 未退出，即正常工作中
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	// 2. 循环查询队列中是否有消息达到了延迟时间，需要被处理
	dirty := false
	for {
		// 2.1 从队列中弹出最早被处理的消息，即堆顶元素（依据 Message.priority）
		// 若堆顶元素 deadline 未到（即 meesage.Priority > t）则返回空，表明此延迟消息还不能被添加到消息队列中，还需要继续被延迟
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true
		// 2.2 否则，表明此消息已经达到了其 deadline了，即从 deferred message 字典中删除对应的消息，将消息添加到消息队列中
		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		// 2.3 将消息添加到内存队列 memeoryMsgChan 或者后端持久化存储中 backend
		c.put(msg)
	}

exit:
	return dirty
}


// queueScanWorker 循环处理 in-flight queue
func (c *Channel) processInFlightQueue(t int64) bool {
	// 1. 先上一把锁，防止在处理 in-flight queue 时， channel 被关闭退出
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// 2. 若当前 channel 已经设置了 exitFlag 标记，则放弃处理，直接退出
	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		// 3. 弹出 channel.inFlightPQ 队列首元素，并且若队首元素的 pri 大于当前的时间戳，则表明此消息还不能被投递，还没到投递 deadline。
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true
		// 4. 否则，说明有需要处理的消息，则将消息从 channel 维护的消息字典 channel.inFlightMessages 中删除
		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		// 5. 增加正在投递的消息的计数 timeoutCount
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		// 6. 将 message put 到内存队列或持久化存储
		c.put(msg)
	}

exit:
	return dirty
}
