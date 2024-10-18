package mq

import (
	"encoding/binary"
	"os"
)

// saveMetadata 将元数据写入文件头部
func (mq *MessageQueue) saveMetadata() error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	buf := make([]byte, MetadataSize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(mq.metadata.WriteOffset))
	binary.LittleEndian.PutUint64(buf[8:], uint64(mq.metadata.ReadOffset))

	_, err := mq.file.WriteAt(buf, 0) // 写入文件头部
	if err != nil {
		return err
	}

	return mq.file.Sync() // 确保数据写入磁盘
}

// loadMetadata 从文件头部加载元数据
func (mq *MessageQueue) loadMetadata() error {
	buf := make([]byte, MetadataSize)
	_, err := mq.file.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	mq.metadata.WriteOffset = int64(binary.LittleEndian.Uint64(buf[0:]))
	mq.metadata.ReadOffset = int64(binary.LittleEndian.Uint64(buf[8:]))

	return nil
}

// 显示文件头部的元数据
func (mq *MessageQueue) showMetadata() QueueMetadata {
	return mq.metadata
}

// consumeMessage 是一个帮助函数，用于实际消费消息
func (mq *MessageQueue) consumeMessage() (*Message, error) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	for mq.metadata.ReadOffset >= mq.metadata.WriteOffset {
		mq.cond.Wait() // 等待有新消息
	}

	// 读取消息长度
	msgLen := int(binary.LittleEndian.Uint32(mq.mmap[mq.metadata.ReadOffset:]))
	mq.metadata.ReadOffset += 4

	if mq.metadata.ReadOffset+int64(msgLen) > mq.size {
		return nil, os.ErrInvalid // 超出范围
	}

	// 读取消息内容
	msgData := mq.mmap[mq.metadata.ReadOffset : mq.metadata.ReadOffset+int64(msgLen)]
	mq.metadata.ReadOffset += int64(msgLen)

	return &Message{Data: msgData}, nil
}
