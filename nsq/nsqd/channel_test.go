package nsqd

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

// ensure that we can push a message through a topic and get it out of a channel
// 测试当向 topic 中投递一个消息时，能够从 topic 关联的 channel 的 内存消息队列 memoryMsgChan 取出
func TestPutMessage(t *testing.T) {
	// 1. 根据默认参数创建 nsqd 的配置项实例
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	// 2. 在测试结束后删除数据配置文件，然后安全退出
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	// 3. 构建一个新的 topic及一个新的 channel
	// nsqd.GetTopic会返回一个新的 topic 实例，若不存在，topic.GetChannel也是类似
	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")
	// 4.构建一条消息
	var id MessageID
	msg := NewMessage(id, []byte("test"))
	// 5. 调用 topic.PutMessage 方法，将消息投递到消息队列（memoryMsgChan 或 backend）
	// 在 topic.messagePump 会循环处理客户端/producers投递的消息，即将此消息放入到消息队列中，
	// 同时，会将此消息转发到此 topic 关联的所有的 channel 的消息队列中（memoryMsgChan 或 backend）
	topic.PutMessage(msg)
	// 6. 因此正常情况下，从 channel 的 memoryMsgChan 中能取出投递的消息
	outputMsg := <-channel1.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg.ID)
	test.Equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
// 测试当向 topic 中投递一个消息时，能够从 topic 关联的所有channel 的 内存消息队列 memoryMsgChan 取出
// 并且，从所有的 channel 中取出的消息是相同的
func TestPutMessage2Chan(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg1 := <-channel1.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg1.ID)
	test.Equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg2.ID)
	test.Equal(t, msg.Body, outputMsg2.Body)
}

// 测试 in-flight queue 的功能是否正确，即消息队列扫描循环 queueScanLoop 中是否有将队列中的消息转移到 memoryMsgChan 或 backend
func TestInFlightWorker(t *testing.T) {
	count := 250

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MsgTimeout = 100 * time.Millisecond
	opts.QueueScanRefreshInterval = 100 * time.Millisecond
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_in_flight_worker" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	for i := 0; i < count; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		// 将消息添加到 in-flight queue（当然同时也添加到了channel.inFlightMessages字典），注意消息会被设置超时时间
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
	}

	// 校验 inFlightMessages 字典添加的消息的数量
	channel.Lock()
	inFlightMsgs := len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, count, inFlightMsgs)

	// 校验 in-flight queue 添加的消息的数量
	channel.inFlightMutex.Lock()
	inFlightPQMsgs := len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, count, inFlightPQMsgs)

	// the in flight worker has a resolution of 100ms so we need to wait
	// at least that much longer than our msgTimeout (in worst case)
	// 等待超时。 正常情况下，因为正常情况下在 nsqd.Main 方法中会开启消息队列循环扫描，即 queueScanLoop
	// 在此方法中，会每过 100ms 统计 dirty 的 channel 的数量（即统计 in-flight queue 及 deferred queue 中是否存在消息）
	// 若有的话，则会将消息首先从in-flight queue 及 deferred queue移除（且总是移除优先级最高的，即超时时间最短的）
	// 然后将消息添加到消息队列（memoryMsgChan 或 backend），并标记为 dirty。
	time.Sleep(4 * opts.MsgTimeout)

	// 因此，当程序运行到这里（4s后），channel.inFlightMessages 中的消息应该已经全部被转移清空了
	channel.Lock()
	inFlightMsgs = len(channel.inFlightMessages)
	channel.Unlock()
	test.Equal(t, 0, inFlightMsgs)

	// 同样，channel.inFlightPQ 也是类似
	channel.inFlightMutex.Lock()
	inFlightPQMsgs = len(channel.inFlightPQ)
	channel.inFlightMutex.Unlock()
	test.Equal(t, 0, inFlightPQMsgs)
}

// 测试
func TestChannelEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	msgs := make([]*Message, 0, 25)
	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		// 将消息添加到 in-flight queue（当然同时也添加到了channel.inFlightMessages字典），同时消息会被设置超时时间
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		msgs = append(msgs, msg)
	}
	// 手动模拟消息重入队（即一般在客户端回复消息处理失败，或者超时时间内客户端未回复FIN消息时，nsqd 会执行的动作）
	// 因此，此时正常情况下，队列中消息数量为 24。而被延迟的消息的消息数量为 1。
	channel.RequeueMessage(0, msgs[len(msgs)-1].ID, 100*time.Millisecond)
	test.Equal(t, 24, len(channel.inFlightMessages))
	test.Equal(t, 24, len(channel.inFlightPQ))
	test.Equal(t, 1, len(channel.deferredMessages))
	test.Equal(t, 1, len(channel.deferredPQ))
	// 清空此 channel 的消息（会清空 in-flight queue、deferred queue、memoryMsgChan 及 backend，还有 client 的一些信息）
	channel.Empty()
	// 校验清空是否达到了效果
	test.Equal(t, 0, len(channel.inFlightMessages))
	test.Equal(t, 0, len(channel.inFlightPQ))
	test.Equal(t, 0, len(channel.deferredMessages))
	test.Equal(t, 0, len(channel.deferredPQ))
	test.Equal(t, int64(0), channel.Depth())
}

func TestChannelEmptyConsumer(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")
	client := newClientV2(0, conn, &context{nsqd})
	client.SetReadyCount(25)
	err := channel.AddClient(client.ID, client)
	test.Equal(t, err, nil)

	for i := 0; i < 25; i++ {
		msg := NewMessage(topic.GenerateID(), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		client.SendingMessage()
	}

	for _, cl := range channel.clients {
		stats := cl.Stats()
		test.Equal(t, int64(25), stats.InFlightCount)
	}

	channel.Empty()

	for _, cl := range channel.clients {
		stats := cl.Stats()
		test.Equal(t, int64(0), stats.InFlightCount)
	}
}

// 测试 channel 能否成功限制其关联的 client 数量
func TestMaxChannelConsumers(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MaxChannelConsumers = 1
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_max_channel_consumers" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	client1 := newClientV2(1, conn, &context{nsqd})
	client1.SetReadyCount(25)
	err := channel.AddClient(client1.ID, client1)
	test.Equal(t, err, nil)

	client2 := newClientV2(2, conn, &context{nsqd})
	client2.SetReadyCount(25)
	err = channel.AddClient(client2.ID, client2)
	test.NotEqual(t, err, nil)
}

// 测试内存队列的长度限制，以及当构建的后端持久化有问题的情况
func TestChannelHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 2

	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel := topic.GetChannel("channel")

	channel.backend = &errorBackendQueue{}

	msg := NewMessage(topic.GenerateID(), make([]byte, 100))
	err := channel.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.NotNil(t, err)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 500, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "NOK - never gonna happen", string(body))

	channel.backend = &errorRecoveredBackendQueue{}

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = channel.PutMessage(msg)
	test.Nil(t, err)

	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "OK", string(body))
}
