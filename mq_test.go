package mq

import (
	"fmt"
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

	// Write messages
	err = queue.Push(msg1)
	if err != nil {
		t.Fatalf("failed to push message: %v", err)
	}

	err = queue.Push(msg2)
	if err != nil {
		t.Fatalf("failed to push message: %v", err)
	}

	// Read messages
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

	// Write multiple messages
	for _, msg := range messages {
		err := queue.Push(&Message{Data: []byte(msg)})
		if err != nil {
			t.Fatalf("failed to push message: %v", err)
		}
	}

	// Read and verify order
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
	queue, err := NewMessageQueue("test_queue_freq.dat", 1024*1024, 2) // Consume at most 2 messages per second
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

	// Consume 5 messages
	for i := 0; i < 5; i++ {
		_, err := queue.Pop()
		if err != nil {
			t.Fatalf("failed to pop message: %v", err)
		}
	}

	elapsed := time.Since(start)

	// Expect to consume 5 messages in about 2.5 seconds (since the rate is 2 messages per second)
	if elapsed < 2*time.Second || elapsed > 3*time.Second {
		t.Fatalf("expected consumption time around 2.5 seconds, got %v", elapsed)
	}
}

func TestNoFrequencyLimit(t *testing.T) {
	queue, err := NewMessageQueue("test_queue_no_freq.dat", 1024*1024, 0) // No frequency limit
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

	// Consume 10 messages
	for i := 0; i < 10; i++ {
		_, err := queue.Pop()
		if err != nil {
			t.Fatalf("failed to pop message: %v", err)
		}
	}

	elapsed := time.Since(start)

	// Since there is no frequency limit, the expected time is very short
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
	errCh := make(chan error, 10) // Channel for error transmission

	// Multiple producers
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

	// Multiple consumers
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

	// Start 2 producers and 2 consumers
	go producer()
	go producer()
	go consumer()
	go consumer()

	// Wait for a while to simulate the production and consumption process
	time.Sleep(2 * time.Second)

	// Check for errors
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("error occurred during test: %v", err)
		}
	default:
		// If no errors, continue to check the production and consumption counts
	}

	if produced != totalMessages {
		t.Fatalf("expected %d messages produced, but got %d", totalMessages, produced)
	}

	if consumed != totalMessages {
		t.Fatalf("expected %d messages consumed, but got %d", totalMessages, consumed)
	}
}

func TestDeleteConsumedMessages(t *testing.T) {
	// Initialize the message queue
	queue, err := NewMessageQueue("test_queue_delete_consumed.dat", 1024*1024, 0)
	if err != nil {
		t.Fatalf("failed to initialize message queue: %v", err)
	}
	defer queue.Close()

	// Produce 50 messages
	for i := 0; i < 50; i++ {
		msg := &Message{Data: []byte(fmt.Sprintf("Message %d", i))}
		if err := queue.Push(msg); err != nil {
			t.Fatalf("failed to push message: %v", err)
		}
	}

	// Consume the first 30 messages
	for i := 0; i < 30; i++ {
		msg, err := queue.Pop()
		if err != nil {
			t.Fatalf("failed to pop message: %v", err)
		}
		if string(msg.Data) != fmt.Sprintf("Message %d", i) {
			t.Fatalf("message order incorrect, expected: %d, got: %s", i, msg.Data)
		}
	}

	// Call DeleteConsumedMessages to delete the consumed 30 messages
	err = queue.DeleteConsumedMessages()
	if err != nil {
		t.Fatalf("failed to delete consumed messages: %v", err)
	}

	// Verify that the remaining messages are correctly retained
	for i := 30; i < 50; i++ {
		msg, err := queue.Pop()
		if err != nil {
			t.Fatalf("failed to pop message: %v", err)
		}
		if string(msg.Data) != fmt.Sprintf("Message %d", i) {
			t.Fatalf("message order incorrect, expected: %d, got: %s", i, msg.Data)
		}
	}

}
