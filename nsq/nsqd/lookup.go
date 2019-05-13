package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

// 连接成功后需要执行的回调函数
func connectCallback(n *NSQD, hostname string) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		// 1. 打包 nsqd 自己的信息，主要是与网络连接相关
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.RealTCPAddr().Port
		ci["http_port"] = n.RealHTTPAddr().Port
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress
		// 2. 发送一个 IDENTIFY 命令请求，以提供自己的身份信息
		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		resp, err := lp.Command(cmd)
		// 3. 检验 IDENTIFY 请求的响应内容
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return
		} else {
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
				lp.Close()
				return
			} else {
				n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
				if lp.Info.BroadcastAddress == "" {
					n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
				}
			}
		}

		// build all the commands first so we exit the lock(s) as fast as possible
		// 4. 构建所有即将发送的 REGISTER 命令请求，用于向 nsqlookupd 注册信息 topic 和 channel 信息
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap {
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()
		// 5. 最后，遍历 REGISTER 命令集合，依次执行它们，并忽略返回结果（当然肯定要检测请求是否执行成功）
		for _, cmd := range commands {
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

// 开启 lookup 循环
func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	connect := true

	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		// 1. 在 nsqd 刚创建时，先构造 nsqd 同各 nsqlookupd（从配置文件中读取）的 lookupPeer 连接，并执行一个回调函数
		if connect { // 在 nsqd 启动时会进入到这里，即创建与各 nsqlookupd 的连接
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname))
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}

		select {
		// 2. 每 15s 就发送一个 heartbeat 消息给所有的 nsqlookupd，并读取响应。此目的是为了及时检测到已关闭的连接
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				// 发送一个 PING 命令请求，利用 lookupPeer 的 Command 方法发送此命令请求，并读取响应，忽略响应（正常情况下 nsqlookupd 端的响应为 ok）
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		// 3. 收到 nsqd 的通知，即 nsqd.Notify 方法被调用，
		// 从 notifyChan 中取出对应的对象 channel 或 topic（在 channel 或 topic 创建及退出/exit(Delete)会调用 nsqd.Notify 方法）
		case val := <-n.notifyChan:
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			// 3.1 若是 Channel，则通知所有的 nsqlookupd 有 channel 更新（新增或者移除）
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() == true { // 若 channel 已经退出，即 channel 被 Delete，则 nsqd 构造 UNREGISTER 命令请求
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else { // 否则表明 channel 是新创建的，则 nsqd 构造 REGISTER 命令请求
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			// 3.2 若是 Topic，则通知所有的 nsqlookupd 有 topic 更新（新增或者移除），处理同 channel 类似
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() == true { // 若 topic 已经退出，即 topic 被 Delete，则 nsqd 构造 UNREGISTER 命令请求
					cmd = nsq.UnRegister(topic.name, "")
				} else { // 若 topic 已经退出，即 topic 被 Delete，则 nsqd 构造 UNREGISTER 命令请求
					cmd = nsq.Register(topic.name, "")
				}
			}
			// 3.3 遍历所有 nsqd 保存的 nsqlookupd 实例的地址信息
			// 向每个 nsqlookupd 发送对应的 Command
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd) // 这里忽略了返回的结果，nsqlookupd 返回的是 ok
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		// 4. 若是 nsqlookupd 的地址变更消息，则重新从配置文件中加载 nsqlookupd 的配置信息
		case <-n.optsNotificationChan:
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		// 5. nsqd 退出消息
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

// 查找所有的 lookupPeers 的地址信息
func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
