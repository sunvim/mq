package mq

import (
	"bytes"
	"encoding/binary"
)

// Message 是可变长度的消息结构体，增加了长度字段
type Message struct {
	Length int32  // 消息长度
	Data   []byte // 实际消息数据
}

// Encode 将消息编码为字节数组
func (msg *Message) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	msg.Length = int32(len(msg.Data))

	// 先写入消息长度
	if err := binary.Write(buf, binary.LittleEndian, msg.Length); err != nil {
		return nil, err
	}

	// 写入消息数据
	if _, err := buf.Write(msg.Data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode 从字节数组解码消息
func Decode(data []byte) (*Message, error) {
	buf := bytes.NewReader(data)

	// 读取消息长度
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// 读取消息数据
	msgData := make([]byte, length)
	if _, err := buf.Read(msgData); err != nil {
		return nil, err
	}

	return &Message{
		Length: length,
		Data:   msgData,
	}, nil
}
