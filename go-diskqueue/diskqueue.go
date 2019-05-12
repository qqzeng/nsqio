package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

// diskQueue 向应用程序所提供的接口操作
type Interface interface {
	Put([]byte) error
	ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// diskQueue implements a filesystem backed FIFO queue
// diskQueue 实现了一个基于后端持久化的 FIFO 队列
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	// 运行时状态，需要被持久化
	readPos      int64					// 当前的文件读取索引
	writePos     int64					// 当前的文件写入索引
	readFileNum  int64					// 当前读取的文件号
	writeFileNum int64					// 当前写入的文件号
	depth        int64					// diskQueue 中等待被读取的消息数

	sync.RWMutex

	// instantiation time metadata
	// 初始化时元数据
	name            string				// diskQueue 名称
	dataPath        string				// 数据持久化路径
	maxBytesPerFile int64 				// 目前，此此属性一旦被初始化，则不可变更
	minMsgSize      int32				// 最小消息的大小
	maxMsgSize      int32				// 最大消息的大小
	syncEvery       int64         		// 累积的消息数量，才进行一次同步刷新到磁盘操作 number of writes per fsync
	syncTimeout     time.Duration 		// 两次同步之间的间隔 duration of time per fsync
	exitFlag        int32				// 退出标志
	needSync        bool				// 是否需要同步刷新

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	// 之所以存在 nextReadPos 及 nextReadFileNum, 是因为虽然消费者已经发起了数据读取请求，但 diskQueue 还未将此消息发送给消费者，
	// 当发送完成后，会将 readPos 更新到 nextReadPos，readFileNum 也类似
	nextReadPos     int64				// 下一个应该被读取的索引位置
	nextReadFileNum int64				// 下一个应该被读取的文件号

	readFile  *os.File					// 当前读取文件句柄
	writeFile *os.File					// 当前写入文件句柄
	reader    *bufio.Reader				// 当前文件读取流
	writeBuf  bytes.Buffer				// 当前文件写入流

	// exposed via ReadChan()
	readChan chan []byte				// 应用程序可通过此通道从 diskQueue 中读取消息，因为 readChan 是 unbuffered的，所以，读取操作是同步的
										// 另外当一个文件中的数据被读取完时，文件会被删除，同时切换到下一个被读取的文件

	// internal channels
	writeChan         chan []byte		// 应用程序可通过此通道往 diskQueue 中压入消息，写入操作也是同步的
	writeResponseChan chan error		// 可通过此通道向应用程序返回消息写入结果
	emptyChan         chan int			// 应用程序可通过此通道发送清空 diskQueue 的消息
	emptyResponseChan chan error		// 可通过此通道向应用程序返回清空 diskQueue 的结果
	exitChan          chan int			// 退出信号
	exitSyncChan      chan int			// 保证 ioLoop 已退出的信号

	logf AppLogFunc
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
// New 方法初始化一个 diskQueue 实例，并从持久化存储中加载元数据信息，然后开始启动
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	// 1. 实例化 diskQueue
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	// 2. 从持久化存储中初始化 diskQueue 的一些属性状态： readPos, writerPos, depth 等
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}
	// 3. 在一个单独的 goroutien 中执行主循环
	go d.ioLoop()
	return &d
}

// Depth returns the depth of the queue
// 返回 diskQueue 中还未被读取的消息数量 depth
func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
// 获取 diskQueu 的读取通道，即 readChan，通过此通道从 diskQueue 中读取/消费消息
func (d *diskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
// 往 diskQueue 压入数据，将数据压入 writeChan，同时将执行结果通过 writeResponseChan 返回
// 因此为同步操作
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
// 关闭 diskQueue，关闭读写文件句柄，同步刷盘
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

// 删除 diskQueue，但与 Close 操作并无不同？
func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// 等待 ioLoop 循环退出才关闭读写文件句柄
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
// 清空 diskQueue 中救济未读取的文件
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

// 调用 skipToNextRWFile 方法清空 readFileNum -> writeFileNum 之间的文件，并且设置 depth 为 0
// 同时删除元数据文件
func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

// 将 readFileNum 到 writeFileNum 之间的文件全部删除
// 将 readFileNum 设置为 writeFileNum
// 即将前面不正确的文件全部删除掉，重新开始读取
// 其也可用作清空 diskQueue 当前未读取的所有文件的操作，重置 depth
func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
// 读取单个字节数组，同时推进读索引，并且滚动读取文件
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	// 1. 若当前文件不存在
	if d.readFile == nil {
		// 1.1. 根据 readFileNum 获取当前需要读取的文件名，并打开输入文件以构造输入文件流
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			// 1.2. 以相对于原文件读取索引来设置 readPos
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
		// 1.3. 构建输入流读取器
		d.reader = bufio.NewReader(d.readFile)
	}
	// 2. 读取内容，并读取内容的长度是否合法
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}
	// 3. 若合法，则将数据读取到缓冲数组
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	// 4. 更新 nextReadPos 及 nextReadFileNum，
	// 但不更新 readPos 和 readFileNum，因为还未将此消息发送给消费者
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	// 5. 若 nextReadPos 大于文件的最大大小，则跳过当前文件，定位到下一个文件
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}
	// 6. 返回读取的缓冲数组
	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
// 将一个字节数组内容写入到持久化存储，同时更新读写位置信息，以及判断是否需要滚动文件
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	// 若当前写入文件句柄为空，则需要先实例化
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)
		// 同时，若当前的写入索引大于0,则重新定位写入索引
		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}
	// 获取写入数据长度，并检查长度合法性。然后将写入到写入缓冲，最后将写入缓冲的数据一次性写入到文件
	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}
	// 更新写入索引 writePos 及 depth，且若 writePos 大于 maxBytesPerFile，则说明已经当前写入文件的末尾
	// 则更新 writeFileNum，重置 writePos，即更换到一个新的文件执行写入操作
	// 且每一次更换到下一个文件，则需要将写入文件同步到磁盘
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
// 同步刷新 writeFile 文件流（即将操作系统缓冲区中的数据写入到磁盘），同时持久化元数据信息
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
// 从持久化存储中初始化 diskQueue 的一些属性状态
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error
	// 1. 获取元数据文件名 *.diskqueue.meta.dat，并打开文件，准备读取文件
	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	// 2. 从文件中内容初始化特定状态属性信息 readPos, writerPos, depth
	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	// 3. 初始化 nextReadFileNum 和 nextReadPos
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
// 持久化元数据信息到磁盘
// 同样是先写入到临时文件，然后同步刷新磁盘缓冲，最后才原子重命名临时文件
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	// 若当前还有消息可供读取，则说明未读取到文件末尾，暂时不用检查
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	// 若代码能够执行，则正常情况下，说明已经读取到 diskQueue 的尾部，
	// 即读取到了最后一个文件的尾部了，因此，此时的 depth(累积等待读取或消费的消息数量)
	// 应该为0,因此若其不为0,则表明文件尾部已经损坏，报错。
	// 且若其小于 0,则表明在初始化加载的元数据已经损坏
	// 否则，说明是消息实体数据存在丢失的情况
	// 同时，强制重置 depth，并且设置 needSync
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)
		d.needSync = true
	}
	// 另外，若 depth == 0。
	// 但文件读取记录信息不合法 d.readFileNum != d.writeFileNum || d.readPos != d.writePos
	// 则跳过下一个需要被读或写的文件
	// 同时设置 needSync
	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

// sets needSync flag if a file is removed
// 检查当前读取的文件和上一次读取的文件是否为同一个，即读取是否涉及到文件的更换
// 若是，则说明可以将磁盘中上一个文件删除掉，同时需要设置 needSync
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1)

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}

	d.checkTailCorruption(depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem

// ioLoop 通过 ReadChan 方法来提供后端持久化的有，并可以同时给并发的消费者消费
// 通过特定的 channels 来执行读写操作
// 应用程序可以异步地从持久化存储中读取消息
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		// 1. 指定间隔时间才执行同步刷新到磁盘的操作
		if count == d.syncEvery {
			d.needSync = true
		}
		// 2. 刷新磁盘操作，重置计数信息，即将 writeFile 流刷新到磁盘，同时持久化元数据
		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}
		// 3. 若当前还有数据（消息）可供消息（即当前读取的文件编号 readFileNum < 目前已经写入的文件编号 writeFileNum
		// 或者 当前的读取索引 readPos < 当前的写的索引）
		// 因为初始化读每一个文件时都需要重置 readPos = 0
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			// 保证当前处于可读取的状态，即 readPos + totalByte == nextReadPos，
			// 若二者相等，同需要通过 d.readOne 方法先更新 nextReadPos
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			// 取出读取通道 readChan
			r = d.readChan
		} else {
			r = nil // 当 r == nil时，使用 select 不能将数压入其中
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		// 4. 当读取到数据时，将它压入到 r/readChan 通道，同时判断是否需要更新到下一个文件读取，同时设置 needSync
		case r <- dataRead:
			count++ // 更新当前等待刷盘的消息数量
			// 判断是否可以将磁盘中上一个文件删除掉（已经读取完毕），同时需要设置 needSync
			// 值得注意的是，moveForward 方法中将 readPos 更新为了 nextReadPos，且 readFileNum 也被更新为 nextReadFileNum
			// 因为此时消息已经发送给了消费者
			d.moveForward()
		// 5. 收到清空持久化存储 disQueue 的消息
		case <-d.emptyChan:
			// 删除目前还未读取的文件，同时删除元数据文件
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0 // 重置当前等待刷盘的消息数量
		// 6. 收到写入消息磁盘的消息(当应用程序调用 diskQueue.put 方法时触发)
		case dataWrite := <-d.writeChan:
			count++ // 更新当前等待刷盘的消息数量
			// 将字节数组内容写入到持久化存储，同时更新读写位置信息，以及判断是否需要滚动文件
			d.writeResponseChan <- d.writeOne(dataWrite)
		// 7. 定时执行刷盘操作，在数据等待时，才需要
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		// 8. 退出信号
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
