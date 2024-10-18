package mq

import (
	"encoding/binary"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	MetadataSize = 128 // Reserve 128 bytes at the beginning of the file for storing metadata
)

// QueueMetadata defines the metadata structure for the message queue
type QueueMetadata struct {
	WriteOffset int64 // Write offset
	ReadOffset  int64 // Read offset
}

// MessageQueue is the main structure of the message queue
type MessageQueue struct {
	file     *os.File      // File mapped to memory
	mmap     []byte        // Mapped memory
	size     int64         // Size of the mapped file
	metadata QueueMetadata // Message queue metadata
	mutex    sync.Mutex    // Protect multi-producer writes
	cond     *sync.Cond    // Synchronization between producers and consumers
	ticker   *time.Ticker  // Control consumption frequency
}

// NewMessageQueue creates a new message queue and maps its file to memory
func NewMessageQueue(filePath string, size int64, ratePerSecond int) (*MessageQueue, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Pre-allocate file size
	if err := file.Truncate(size); err != nil {
		return nil, err
	}

	// Use mmap to map the file to memory
	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Initialize ticker to control consumption frequency
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

	// Load metadata
	mq.loadMetadata()
	if mq.metadata.WriteOffset < MetadataSize {
		mq.metadata.WriteOffset = MetadataSize
	}
	if mq.metadata.ReadOffset < MetadataSize {
		mq.metadata.ReadOffset = MetadataSize
	}

	return mq, nil
}

// Close closes the message queue and cleans up resources
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

// Push writes a message to the queue
func (mq *MessageQueue) Push(msg *Message) error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	dataLen := len(msg.Data)
	if mq.metadata.WriteOffset+int64(dataLen+4) > mq.size {
		return os.ErrInvalid // Queue is full
	}

	// Write message length
	binary.LittleEndian.PutUint32(mq.mmap[mq.metadata.WriteOffset:], uint32(dataLen))
	mq.metadata.WriteOffset += 4

	// Write message data
	copy(mq.mmap[mq.metadata.WriteOffset:], msg.Data)
	mq.metadata.WriteOffset += int64(dataLen)

	// Notify consumers of new message
	mq.cond.Signal()

	return nil
}

// Pop reads a variable-length message from the queue and consumes it at the set frequency
func (mq *MessageQueue) Pop() (*Message, error) {
	for {
		if mq.ticker == nil { // Consume immediately if there is no frequency limit
			return mq.consumeMessage()
		}

		select {
		case <-mq.ticker.C: // Consume messages at the set frequency
			return mq.consumeMessage()
		}
	}
}

// Flush flushes the queue, persisting messages in memory to disk
func (mq *MessageQueue) Flush() error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	// Update metadata and save
	err := mq.saveMetadata()
	if err != nil {
		return err
	}

	return mq.file.Sync()
}
