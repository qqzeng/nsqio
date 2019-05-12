package nsqd

import (
	"bytes"
	"sync"
)

// 标准库中的缓冲池，通过将缓冲区池化，可以降低反复分配缓冲区的开销
var bp sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

func bufferPoolPut(b *bytes.Buffer) {
	bp.Put(b)
}
