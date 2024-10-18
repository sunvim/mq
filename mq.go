package mq

import (
	"encoding/binary"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	MetadataSize = 128 // 文件头预留 128 字节存储元数据
)

// QueueMetadata 定义消息队列元数据结构
type QueueMetadata struct {
	WriteOffset int64 // 写入偏移量
	ReadOffset  int64 // 读取偏移量
}

// MessageQueue 是消息队列的主要结构
type MessageQueue struct {
	file     *os.File      // 文件映射到内存
	mmap     []byte        // 映射的内存
	size     int64         // 映射文件的大小
	metadata QueueMetadata // 消息队列元数据
	mutex    sync.Mutex    // 保护多生产者写入
	cond     *sync.Cond    // 生产者和消费者的同步
	ticker   *time.Ticker  // 消费频率控制
}

// NewMessageQueue 创建一个新的消息队列并将其文件映射到内存中
func NewMessageQueue(filePath string, size int64, ratePerSecond, maxRetainMsgs int) (*MessageQueue, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// 预先分配文件大小
	if err := file.Truncate(size); err != nil {
		return nil, err
	}

	// 使用 mmap 将文件映射到内存
	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// 初始化 ticker 以控制消费频率
	var ticker *time.Ticker
	if ratePerSecond > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(ratePerSecond))
	}

	mq := &MessageQueue{
		file:   file,
		mmap:   mmap,
		size:   size,
		ticker: ticker,
	}
	mq.cond = sync.NewCond(&mq.mutex)

	// 加载元数据
	mq.loadMetadata()
	if mq.metadata.WriteOffset < MetadataSize {
		mq.metadata.WriteOffset = MetadataSize
	}
	if mq.metadata.ReadOffset < MetadataSize {
		mq.metadata.ReadOffset = MetadataSize
	}

	return mq, nil
}

// Close 关闭消息队列并清理资源
func (mq *MessageQueue) Close() error {
	err := mq.saveMetadata()
	if err != nil {
		return err
	}

	if err := syscall.Munmap(mq.mmap); err != nil {
		return err
	}
	return mq.file.Close()
}

// Push 向队列中写入消息
func (mq *MessageQueue) Push(msg *Message) error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	dataLen := len(msg.Data)
	if mq.metadata.WriteOffset+int64(dataLen+4) > mq.size {
		return os.ErrInvalid // 队列已满
	}

	// 写入消息长度
	binary.LittleEndian.PutUint32(mq.mmap[mq.metadata.WriteOffset:], uint32(dataLen))
	mq.metadata.WriteOffset += 4

	// 写入消息数据
	copy(mq.mmap[mq.metadata.WriteOffset:], msg.Data)
	mq.metadata.WriteOffset += int64(dataLen)

	// 通知消费者有新消息
	mq.cond.Signal()

	return nil
}

// Pop 从队列中读取可变长度消息，并按照设定的频率消费
func (mq *MessageQueue) Pop() (*Message, error) {
	for {
		if mq.ticker == nil { // 没有频率限制时，立即消费
			return mq.consumeMessage()
		}

		select {
		case <-mq.ticker.C: // 按照设定的频率吐出消息
			return mq.consumeMessage()
		}
	}
}

// Flush 刷盘，将内存中的消息持久化到磁盘
func (mq *MessageQueue) Flush() error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	// 更新元数据并保存
	err := mq.saveMetadata()
	if err != nil {
		return err
	}

	return mq.file.Sync()
}
