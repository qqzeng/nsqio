package nsqd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

// 测试 nsqd.GetTopic 方法的正确性
// 1. 若对应的 topic 不存在时，会创建一个新的 topic
// 2. 测试 topic　可以根据 topicName 唯一标识
func TestGetTopic(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic1 := nsqd.GetTopic("test")
	test.NotNil(t, topic1)
	test.Equal(t, "test", topic1.name)

	topic2 := nsqd.GetTopic("test")
	test.Equal(t, topic1, topic2)

	topic3 := nsqd.GetTopic("test2")
	test.Equal(t, "test2", topic3.name)
	test.NotEqual(t, topic2, topic3)
}

// 测试 topic.GetChannel，其行为同 nsqd.GetTopic 方法类似。
func TestGetChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)
	test.Equal(t, "ch1", channel1.name)

	channel2 := topic.GetChannel("ch2")

	test.Equal(t, channel1, topic.channelMap["ch1"])
	test.Equal(t, channel2, topic.channelMap["ch2"])
}

type errorBackendQueue struct{}

func (d *errorBackendQueue) Put([]byte) error      { return errors.New("never gonna happen") }
func (d *errorBackendQueue) ReadChan() chan []byte { return nil }
func (d *errorBackendQueue) Close() error          { return nil }
func (d *errorBackendQueue) Delete() error         { return nil }
func (d *errorBackendQueue) Depth() int64          { return 0 }
func (d *errorBackendQueue) Empty() error          { return nil }

type errorRecoveredBackendQueue struct{ errorBackendQueue }

func (d *errorRecoveredBackendQueue) Put([]byte) error { return nil }

func TestHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 2
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")
	topic.backend = &errorBackendQueue{}

	msg := NewMessage(topic.GenerateID(), make([]byte, 100))
	err := topic.PutMessage(msg)
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	test.Nil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessage(msg)
	test.NotNil(t, err)

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	test.NotNil(t, err)

	url := fmt.Sprintf("http://%s/ping", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 500, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "NOK - never gonna happen", string(body))

	topic.backend = &errorRecoveredBackendQueue{}

	msg = NewMessage(topic.GenerateID(), make([]byte, 100))
	err = topic.PutMessages([]*Message{msg})
	test.Nil(t, err)

	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "OK", string(body))
}

// 测试 topic.DeleteExistingChannel和nsqd.DeleteExistingTopic 方法的正确性。
func TestDeletes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	channel2 := topic.GetChannel("ch2")
	test.NotNil(t, channel2)

	err = nsqd.DeleteExistingTopic("test")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))
	test.Equal(t, 0, len(nsqd.topicMap))
}

// 测试 topic.DeleteExistingChannel，即使没有任何 channel 与 topic 关联
// 那些投递的消息，也不会丢失，会缓存在 topic 的 memoryMsgChan 或 backend 中
func TestDeleteLast(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("test")

	channel1 := topic.GetChannel("ch1")
	test.NotNil(t, channel1)

	err := topic.DeleteExistingChannel("ch1")
	test.Nil(t, err)
	test.Equal(t, 0, len(topic.channelMap))

	msg := NewMessage(topic.GenerateID(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	time.Sleep(100 * time.Millisecond)
	test.Nil(t, err)
	test.Equal(t, int64(1), topic.Depth())
}

// 测试 topic 被 paused的功能的正确性。
// 当 topic 被 paused 时，其不会将消息投递到 channel 的消息队列
func TestPause(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_topic_pause" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	err := topic.Pause()
	test.Nil(t, err)

	channel := topic.GetChannel("ch1")
	test.NotNil(t, channel)

	msg := NewMessage(topic.GenerateID(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	err = topic.PutMessage(msg)
	test.Nil(t, err)

	time.Sleep(15 * time.Millisecond)

	test.Equal(t, int64(1), topic.Depth())
	test.Equal(t, int64(0), channel.Depth())

	err = topic.UnPause()
	test.Nil(t, err)

	time.Sleep(15 * time.Millisecond)

	test.Equal(t, int64(0), topic.Depth())
	test.Equal(t, int64(1), channel.Depth())
}

// topic 投递消息的压力测试
func BenchmarkTopicPut(b *testing.B) {
	b.StopTimer() // 停止压力测试的时间计数，在初始化，同时启动 nqsd 后才开始计时
	topicName := "bench_topic_put" + strconv.Itoa(b.N)
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		msg := NewMessage(topic.GenerateID(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}
}

// 消息从 topic 消息队列（memoryMsgChan 或 backend）转移到 channel 的消息队列操作的压力测试
func BenchmarkTopicToChannelPut(b *testing.B) {
	b.StopTimer()
	topicName := "bench_topic_to_channel_put" + strconv.Itoa(b.N)
	channelName := "bench"
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()
	channel := nsqd.GetTopic(topicName).GetChannel(channelName)
	b.StartTimer()

	for i := 0; i <= b.N; i++ {
		topic := nsqd.GetTopic(topicName)
		msg := NewMessage(topic.GenerateID(), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		topic.PutMessage(msg)
	}

	for {
		if len(channel.memoryMsgChan) == b.N {
			break
		}
		runtime.Gosched()
	}
}
