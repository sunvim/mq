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
	file        *os.File      // File mapped to memory
	mmap        []byte        // Mapped memory
	size        int64         // Size of the mapped file
	metadata    QueueMetadata // Message queue metadata
	writeMutex  sync.Mutex    // Protect multi-producer writes
	readMutex   sync.Mutex    // Protect multi-consumer reads
	cond        *sync.Cond    // Synchronization between producers and consumers
	ticker      *time.Ticker  // Control consumption frequency
	metadataMux sync.RWMutex  // Protects metadata access
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
		file:      file,
		mmap:      mmap,
		size:      size,
		ticker:    ticker,
	}
	mq.cond = sync.NewCond(&mq.writeMutex)

	// Load metadata
	if err := mq.loadMetadata(); err != nil {
		return nil, err
	}

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
	mq.writeMutex.Lock()
	defer mq.writeMutex.Unlock()

	// Get current write offset
	currentWriteOffset := mq.getWriteOffset()

	dataLen := len(msg.Data)
	if currentWriteOffset+int64(dataLen+4) > mq.size {
		return os.ErrInvalid // Queue is full
	}

	// Write message length
	binary.LittleEndian.PutUint32(mq.mmap[currentWriteOffset:], uint32(dataLen))
	currentWriteOffset += 4

	// Write message data
	copy(mq.mmap[currentWriteOffset:], msg.Data)
	currentWriteOffset += int64(dataLen)

	// Update write offset
	mq.setWriteOffset(currentWriteOffset)

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

// PopAll reads all unconsumed messages from the queue at once
// This returns all available messages without waiting for new ones
func (mq *MessageQueue) PopAll() ([]*Message, error) {
	mq.readMutex.Lock()
	defer mq.readMutex.Unlock()

	// Get current read and write offsets
	readOffset := mq.getReadOffset()
	writeOffset := mq.getWriteOffset()

	// If there are no messages to read
	if readOffset >= writeOffset {
		return []*Message{}, nil
	}

	var messages []*Message
	currentOffset := readOffset

	// Read all available messages
	for currentOffset < writeOffset {
		// Check if we have enough space to read message length
		if currentOffset+4 > writeOffset {
			break
		}

		// Read message length
		msgLen := int(binary.LittleEndian.Uint32(mq.mmap[currentOffset:]))
		currentOffset += 4

		// Check if the complete message is available
		if currentOffset+int64(msgLen) > writeOffset {
			break
		}

		// Read message content
		msgData := make([]byte, msgLen)
		copy(msgData, mq.mmap[currentOffset:currentOffset+int64(msgLen)])
		currentOffset += int64(msgLen)

		messages = append(messages, &Message{Data: msgData})
	}

	// Update read offset
	mq.setReadOffset(currentOffset)

	return messages, nil
}

// WaitForMessages waits until there are new messages available
func (mq *MessageQueue) WaitForMessages() {
	mq.writeMutex.Lock()
	for mq.getReadOffset() >= mq.getWriteOffset() {
		mq.cond.Wait() // wait for new messages
	}
	mq.writeMutex.Unlock()
}

// Flush flushes the queue, persisting messages in memory to disk
func (mq *MessageQueue) Flush() error {
	mq.writeMutex.Lock()
	defer mq.writeMutex.Unlock()

	return mq.file.Sync()
}

// DeleteConsumedMessages deletes consumed messages and updates the queue offsets
func (mq *MessageQueue) DeleteConsumedMessages() error {
	// Lock both read and write to perform this operation
	mq.writeMutex.Lock()
	mq.readMutex.Lock()
	defer mq.readMutex.Unlock()
	defer mq.writeMutex.Unlock()

	readOffset := mq.getReadOffset()
	writeOffset := mq.getWriteOffset()

	// If there are no consumed messages, return directly
	if readOffset == MetadataSize {
		return nil // No consumed messages
	}

	// Calculate the size of unconsumed messages
	remainingDataSize := writeOffset - readOffset
	if remainingDataSize > 0 {
		// Move unconsumed messages to the beginning of the queue
		copy(mq.mmap[MetadataSize:], mq.mmap[readOffset:writeOffset])
	}

	// Update offsets
	mq.setWriteOffset(MetadataSize + remainingDataSize)
	mq.setReadOffset(MetadataSize)

	return nil
}

// Helper methods for thread-safe metadata access

func (mq *MessageQueue) getReadOffset() int64 {
	mq.metadataMux.RLock()
	defer mq.metadataMux.RUnlock()
	return mq.metadata.ReadOffset
}

func (mq *MessageQueue) getWriteOffset() int64 {
	mq.metadataMux.RLock()
	defer mq.metadataMux.RUnlock()
	return mq.metadata.WriteOffset
}

func (mq *MessageQueue) setReadOffset(offset int64) {
	mq.metadataMux.Lock()
	defer mq.metadataMux.Unlock()
	mq.metadata.ReadOffset = offset
}

func (mq *MessageQueue) setWriteOffset(offset int64) {
	mq.metadataMux.Lock()
	defer mq.metadataMux.Unlock()
	mq.metadata.WriteOffset = offset
}
