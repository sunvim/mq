package mq

import (
	"encoding/binary"
	"os"
)

// saveMetadata writes metadata to the file header
func (mq *MessageQueue) saveMetadata() error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	buf := make([]byte, MetadataSize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(mq.metadata.WriteOffset))
	binary.LittleEndian.PutUint64(buf[8:], uint64(mq.metadata.ReadOffset))

	_, err := mq.file.WriteAt(buf, 0) // write to the file header
	if err != nil {
		return err
	}

	return mq.file.Sync() // ensure data is written to disk
}

// loadMetadata loads metadata from the file header
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

// showMetadata displays the metadata in the file header
func (mq *MessageQueue) showMetadata() QueueMetadata {
	return mq.metadata
}

// consumeMessage is a helper function to actually consume a message
func (mq *MessageQueue) consumeMessage() (*Message, error) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	for mq.metadata.ReadOffset >= mq.metadata.WriteOffset {
		mq.cond.Wait() // wait for new messages
	}

	// read message length
	msgLen := int(binary.LittleEndian.Uint32(mq.mmap[mq.metadata.ReadOffset:]))
	mq.metadata.ReadOffset += 4

	if mq.metadata.ReadOffset+int64(msgLen) > mq.size {
		return nil, os.ErrInvalid // out of range
	}

	// read message content
	msgData := mq.mmap[mq.metadata.ReadOffset : mq.metadata.ReadOffset+int64(msgLen)]
	mq.metadata.ReadOffset += int64(msgLen)

	return &Message{Data: msgData}, nil
}
