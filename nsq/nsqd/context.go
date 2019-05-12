package nsqd

// 同 NSQLookupd 的设计类似，单独构建一个Context 用作 NSQD 的一个 wrapper
// 以使得其它引用 NSQD 的实例能够与 NSQD 解耦
type context struct {
	nsqd *NSQD
}
