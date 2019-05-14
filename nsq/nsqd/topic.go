package nsqd

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64							// 此 topic 所包含的消息的总数（内存+磁盘）
	messageBytes uint64							// 此 topic 所包含的消息的总大小（内存+磁盘）

	sync.RWMutex								// guards channelMap

	name              string					// topic 名称
	channelMap        map[string]*Channel		// topic 所包含的 channel 集合
	backend           BackendQueue				// 代表持久化存储的通道
	memoryMsgChan     chan *Message				// 代表消息在内存中的通道
	startChan         chan int					// 消息处理循环开关
	exitChan          chan int					// topic 消息处理循环退出开关
	channelUpdateChan chan int					// 消息更新的开关
	waitGroup         util.WaitGroupWrapper		// waitGroup 的一个 wrapper
	// 其会在删除一个topic时被设置，且若被设置，则 putMessage(s)操作会返回错误，拒绝写入消息
	exitFlag          int32
	idFactory         *guidFactory				// 用于生成客户端实例的ID
	// 临时的 topic（#ephemeral开头），此种类型的 topic 不会进行持久化，
	// 当此 topic 所包含的所有的 channel 都被删除后，被标记为ephemeral的topic也会被删除
	ephemeral      bool
	// topic 被删除前的回调函数，且对 ephemeral 类型的 topic有效，并且它只在 DeleteExistingChannel 方法中被调用
	deleteCallback func(*Topic)
	deleter        sync.Once
	// 标记此 topic 是否有被 paused，若被 paused，则其不会将消息写入到其关联的 channel 的消息队列
	paused    int32
	pauseChan chan int

	ctx *context								// nsqd 实例的 wrapper
}

// Topic constructor
// topic 的构造函数
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	// 1. 构造 topic 实例
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		idFactory:         NewGUIDFactory(ctx.nsqd.getOpts().ID),
	}
	// 2. 标记那些带有 ephemeral 的 topic，并为它们构建一个 Dummy BackendQueue，
	// 因为这些 topic 所包含的的消息不会被持久化，因此不需要持久化队列 BackendQueue。
	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// 3. 通过 diskqueue (https://github.com/nsqio/go-diskqueue) 构建持久化队列实例
		t.backend = diskqueue.New(
			topicName,												// topic 名称
			ctx.nsqd.getOpts().DataPath,							// 数据存储路径
			ctx.nsqd.getOpts().MaxBytesPerFile,						// 存储文件的最大字节数
			int32(minValidMsgLength),								// 最小的有效消息的长度
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength, // 最大的有效消息的长度
			// 单次同步刷新消息的数量，即当消息数量达到 SyncEvery 的数量时，
			// 需要执行刷新动作（否则会留在操作系统缓冲区）
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,							// 两次同步刷新的时间间隔，即两次同步操作之间的最大间隔
			dqLogf,													// 日志
		)
	}
	// 4. 执行 messagePump 方法，即 开启消息监听 go routine
	t.waitGroup.Wrap(t.messagePump)
	// 5. 通知 nsqlookupd 有新的 topic 产生
	t.ctx.nsqd.Notify(t)

	return t
}

func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
// 返回此 topic 是否已经或者正在退出
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// 根据 channel 名称返回 channel 实例，且有可能是新建的。线程安全。
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		// 若此 channel 为新创建的，则 push 消息到 channelUpdateChan中，使 memoryMsgChan 及 backend 刷新状态
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
// 根据 channel 名称获取指定的 channel，若不存在，则创建一个新的 channel 实例。非线程安全
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		// 注册 channel 被删除时的回调函数
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
// 删除 topic 中一个已经存在的 channel
func (t *Topic) DeleteExistingChannel(channelName string) error {
	// 1. 若不存在则直接报错
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	// 2. 然后从 topic 的 channelMap 中删除
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	// 3. 关闭 channel
	channel.Delete()

	// update messagePump state
	// 通知 memoryMsgChan 及 backend 消息队列 channel 已被更新
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	// 当此 topic 为 ephemeral，且此次删除的 channel 为最后一个 channel，则需要执行删除回调函数，
	// 且此删除回调函数也只会在这种情况下才会被调用
	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
// 将消息写入消息队列，在 exitFlag 为0时才进行（一般由消费者 producers 来调用，即向 topic 中生产消息）
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	// 1. 消息写入操作只在 exitFlag 为0时才进行
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	// 2. 写入消息内存队列 memoryMsgChan 或者 持久化存储 backend
	err := t.put(m)
	if err != nil {
		return err
	}
	// 3. 更新当前 topic 所对应的消息数量以及消息总大小
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

// PutMessages writes multiple Messages to the queue
// PutMessage 方法的批量版本
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// 将指定消息进行持久化
// 通常情况下，在 memoryMsChan 未达到其设置的最大的消息的数量时（即内存中的消息队列中保存的消息的数量未达到上限时，由 MemQueueSize 指定）
// 会先将消息 push 到内在消息队列 memoryChan 中，否则会被 push 到后端持久化队列 backend 中。
// 这是通过 go buffered channel 语法来实现。
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m:
	default: // 内存消息队列已满时，会将消息存放到持久化存储
		// 从缓冲池中获取缓冲
		b := bufferPoolGet()
		// 将消息写入持久化消息队列
		err := writeMessageToBackend(b, m, t.backend)
		bufferPoolPut(b) // 回收从缓冲池中获取的缓冲
		t.ctx.nsqd.SetHealth(err)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

// 返回内存消息队列和后端持久化存储消息队列中消息的总的数量
func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
// messagePump 监听 message 的更新的一些状态，以及时将消息持久化，同时写入到此 topic 对应的channel
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	for {
		select {
		case <-t.channelUpdateChan:
			continue
		case <-t.pauseChan:
			continue
		case <-t.exitChan:
			goto exit
		// 在 nsqd.Main 中最后一个阶段会开启消息处理循环（处理由客户端（producers）向 topci 投递的消息）
		case <-t.startChan:
		}
		break
	}
	t.RLock()
	// 根据 channelMap 初始化两个通道memoryMsgChan，backendChan
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// 消息处理主循环
	for {
		select {
		// 从内存消息队列 memoryMsgChan 或 持久化存储 backend 中收到消息
		// 则将消息解码，然后会将此消息 push 到此 topic 关联的所有 channel
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		// 当从 channelUpdateChan 读取到消息时，表明有 channel 更新，比如创建了新的消息，因此需要重新初始化 memoryMsgChan及 backendChan
		case <-t.channelUpdateChan:
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		// 当收到 pause 消息时，则将 memoryMsgChan及backendChan置为 nil，注意不能 close，
		// 二者的区别是 nil的chan不能接收消息了，但不会报错。而若从一个已经 close 的 chan 中尝试取消息，则会 panic。
		case <-t.pauseChan:
			// 当 topic 被 paused 时，其不会将消息投递到 channel 的消息队列
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}
		// 当从 memoryMsgChan 或 backendChan 中 pull 到一个 msg 后，会执行这里：
		// 遍历 channelMap 中的每一个 channel，将此 msg 拷贝到 channel 中的后备队列。
		// 注意，因为每个 channel 需要一个独立 msg，因此需要在拷贝时需要创建 msg 的副本
		// 同时，针对 msg 是否需要被延时投递来选择将 msg 放到延时队列 deferredMessages中还是普通的队列中
		for i, channel := range chans {
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 { // 若此 topic 只有一个 channel，则明显不需要显式地拷贝了
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			if chanMsg.deferred != 0 { // 将 msg push 到 channel 所维护的延时消息队列 deferred queue
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			err := channel.PutMessage(chanMsg) // 将 msg push 到普通消息队列
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}

// Delete empties the topic and all its channels and closes
// Delete 方法和 Close 方法都调用的是 exit 方法。
// 区别在于 Delete 还需要显式得通知 lookupd，让它删除此 topic 的注册信息
// 而　Close　方法是在　topic　关闭时调用，因此需要持久化所有未被处理/消费的消息，然后再关闭所有的 channel，退出
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

// 使当前 topic 对象　exit，同时若指定删除其所关联的 channels 及 closes，则清空它们
func (t *Topic) exit(deleted bool) error {
	// 1. 保证目前还处于运行的状态
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	// 2. 当被　Delete　调用时，则需要先通知 lookupd 删除其对应的注册信息
	if deleted {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}
	// 3. 关闭 exitChan，保证所有的循环全部会退出，比如消息处理循环 messagePump 会退出
	close(t.exitChan)

	// synchronize the close of messagePump()
	// 4. 同步等待消息处理循环 messagePump 方法的退出，才继续执行下面的操作（只有消息处理循环退出后，才能删除对应的 channel集合）
	t.waitGroup.Wait()

	// 4. 若是被 Delete 方法调用，则需要清空 topic 所包含的 channel（同 topic 的操作类似）
	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	// 5. 否则若是被 Close 方法调用，则只需要关闭所有的 channel，不会将所有的 channel 从 topic 的 channelMap 中删除
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	// 6. 将内存中的消息，即 t.memoryMsgChan 中的消息刷新到持久化存储
	t.flush()
	return t.backend.Close()
}

// 清空内存消息队列和持久化存储消息队列中的消息
func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

// 刷新内存消息队列即 t.memoryMsgChan 中的消息到持久化存储 backend
func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		t.ctx.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
retry:
	id, err := t.idFactory.NewGUID()
	if err != nil {
		time.Sleep(time.Millisecond)
		goto retry
	}
	return id.Hex()
}
