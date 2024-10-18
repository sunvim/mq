package mq

import (
	"bytes"
	"encoding/binary"
)

// Message is a variable-length message structure with an added length field
type Message struct {
	Length int32  // Message length
	Data   []byte // Actual message data
}

// Encode encodes the message into a byte array
func (msg *Message) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	msg.Length = int32(len(msg.Data))

	// First write the message length
	if err := binary.Write(buf, binary.LittleEndian, msg.Length); err != nil {
		return nil, err
	}

	// Write the message data
	if _, err := buf.Write(msg.Data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode decodes the message from a byte array
func Decode(data []byte) (*Message, error) {
	buf := bytes.NewReader(data)

	// Read the message length
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// Read the message data
	msgData := make([]byte, length)
	if _, err := buf.Read(msgData); err != nil {
		return nil, err
	}

	return &Message{
		Length: length,
		Data:   msgData,
	}, nil
}
