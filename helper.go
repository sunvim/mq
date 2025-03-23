package mq

import (
	"encoding/binary"
	"os"
)

// saveMetadata writes metadata to the file header
func (mq *MessageQueue) saveMetadata() error {
	mq.metadataMux.Lock()
	defer mq.metadataMux.Unlock()

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

	mq.metadataMux.Lock()
	defer mq.metadataMux.Unlock()

	mq.metadata.WriteOffset = int64(binary.LittleEndian.Uint64(buf[0:]))
	mq.metadata.ReadOffset = int64(binary.LittleEndian.Uint64(buf[8:]))

	return nil
}

// showMetadata displays the metadata in the file header
func (mq *MessageQueue) showMetadata() QueueMetadata {
	mq.metadataMux.RLock()
	defer mq.metadataMux.RUnlock()

	return mq.metadata
}

// consumeMessage is a helper function to actually consume a message
func (mq *MessageQueue) consumeMessage() (*Message, error) {
	mq.readMutex.Lock()
	defer mq.readMutex.Unlock()

	// Get current read and write offsets
	readOffset := mq.getReadOffset()
	writeOffset := mq.getWriteOffset()

	// If there are no messages to read, wait for new ones
	if readOffset >= writeOffset {
		mq.readMutex.Unlock() // Release lock before waiting
		mq.WaitForMessages()  // This will block until new messages are available
		mq.readMutex.Lock()   // Re-acquire lock

		// Re-read offsets after wait
		readOffset = mq.getReadOffset()
		writeOffset = mq.getWriteOffset()

		// Double-check there are messages after waiting
		if readOffset >= writeOffset {
			return nil, os.ErrNotExist
		}
	}

	// read message length
	msgLen := int(binary.LittleEndian.Uint32(mq.mmap[readOffset:]))
	readOffset += 4

	if readOffset+int64(msgLen) > mq.size {
		return nil, os.ErrInvalid // out of range
	}

	// read message content
	msgData := make([]byte, msgLen)
	copy(msgData, mq.mmap[readOffset:readOffset+int64(msgLen)])
	readOffset += int64(msgLen)

	// Update read offset
	mq.setReadOffset(readOffset)

	return &Message{Data: msgData}, nil
}
