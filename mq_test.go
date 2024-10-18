package mq

import (
	"sync"
	"testing"
	"time"
)

func TestBasicPushAndPop(t *testing.T) {
	queue, err := NewMessageQueue("test_queue_basic.dat", 1024*1024, 0)
	if err != nil {
		t.Fatalf("failed to create message queue: %v", err)
	}
	defer queue.Close()

	meta := queue.showMetadata()

	t.Logf("meta: %+v", meta)

	msg1 := &Message{Data: []byte("Hello")}
	msg2 := &Message{Data: []byte("World")}

	// 写入消息
	err = queue.Push(msg1)
	if err != nil {
		t.Fatalf("failed to push message: %v", err)
	}

	err = queue.Push(msg2)
	if err != nil {
		t.Fatalf("failed to push message: %v", err)
	}

	// 读取消息
	poppedMsg1, err := queue.Pop()
	if err != nil {
		t.Fatalf("failed to pop message: %v", err)
	}
	if string(poppedMsg1.Data) != "Hello" {
		t.Fatalf("expected 'Hello', got '%s'", string(poppedMsg1.Data))
	}

	poppedMsg2, err := queue.Pop()
	if err != nil {
		t.Fatalf("failed to pop message: %v", err)
	}
	if string(poppedMsg2.Data) != "World" {
		t.Fatalf("expected 'World', got '%s'", string(poppedMsg2.Data))
	}
}

func TestMessageOrder(t *testing.T) {
	queue, err := NewMessageQueue("test_queue_order.dat", 1024*1024, 0)
	if err != nil {
		t.Fatalf("failed to create message queue: %v", err)
	}
	defer queue.Close()

	messages := []string{"Msg1", "Msg2", "Msg3"}

	// 写入多条消息
	for _, msg := range messages {
		err := queue.Push(&Message{Data: []byte(msg)})
		if err != nil {
			t.Fatalf("failed to push message: %v", err)
		}
	}

	// 读取并验证顺序
	for _, expected := range messages {
		msg, err := queue.Pop()
		if err != nil {
			t.Fatalf("failed to pop message: %v", err)
		}
		if string(msg.Data) != expected {
			t.Fatalf("expected '%s', got '%s'", expected, string(msg.Data))
		}
	}
}

func TestFrequencyLimit(t *testing.T) {
	queue, err := NewMessageQueue("test_queue_freq.dat", 1024*1024, 2) // 每秒最多消费2条消息
	if err != nil {
		t.Fatalf("failed to create message queue: %v", err)
	}
	defer queue.Close()

	for i := 0; i < 5; i++ {
		err := queue.Push(&Message{Data: []byte("Message")})
		if err != nil {
			t.Fatalf("failed to push message: %v", err)
		}
	}

	start := time.Now()

	// 消费5条消息
	for i := 0; i < 5; i++ {
		_, err := queue.Pop()
		if err != nil {
			t.Fatalf("failed to pop message: %v", err)
		}
	}

	elapsed := time.Since(start)

	// 预期消费5条消息需要大约2.5秒 (因为频率是每秒2条)
	if elapsed < 2*time.Second || elapsed > 3*time.Second {
		t.Fatalf("expected consumption time around 2.5 seconds, got %v", elapsed)
	}
}

func TestNoFrequencyLimit(t *testing.T) {
	queue, err := NewMessageQueue("test_queue_no_freq.dat", 1024*1024, 0) // 没有频率限制
	if err != nil {
		t.Fatalf("failed to create message queue: %v", err)
	}
	defer queue.Close()

	for i := 0; i < 10; i++ {
		err := queue.Push(&Message{Data: []byte("Message")})
		if err != nil {
			t.Fatalf("failed to push message: %v", err)
		}
	}

	start := time.Now()

	// 消费10条消息
	for i := 0; i < 10; i++ {
		_, err := queue.Pop()
		if err != nil {
			t.Fatalf("failed to pop message: %v", err)
		}
	}

	elapsed := time.Since(start)

	// 因为没有频率限制，预期时间非常短
	if elapsed > 100*time.Millisecond {
		t.Fatalf("expected fast consumption, but got %v", elapsed)
	}
}

func TestMultiProducerConsumer(t *testing.T) {
	queue, err := NewMessageQueue("test_queue_multi.dat", 1024*1024, 0)
	if err != nil {
		t.Fatalf("failed to create message queue: %v", err)
	}
	defer queue.Close()

	totalMessages := 100
	produced := 0
	consumed := 0
	var mu sync.Mutex
	errCh := make(chan error, 10) // 错误传递的 channel

	// 多生产者
	producer := func() {
		for i := 0; i < totalMessages/2; i++ {
			err := queue.Push(&Message{Data: []byte("Message")})
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			produced++
			mu.Unlock()
		}
	}

	// 多消费者
	consumer := func() {
		for {
			_, err := queue.Pop()
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			consumed++
			mu.Unlock()
		}
	}

	// 启动2个生产者和2个消费者
	go producer()
	go producer()
	go consumer()
	go consumer()

	// 等待一段时间，模拟生产和消费过程
	time.Sleep(2 * time.Second)

	// 检查是否有错误
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("error occurred during test: %v", err)
		}
	default:
		// 如果没有错误，继续检查生产和消费数量
	}

	if produced != totalMessages {
		t.Fatalf("expected %d messages produced, but got %d", totalMessages, produced)
	}

	if consumed != totalMessages {
		t.Fatalf("expected %d messages consumed, but got %d", totalMessages, consumed)
	}
}
