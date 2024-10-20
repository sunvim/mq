[中文](README_CN.md)
# mq

embed message queue for golang A high-performance, multi-producer, multi-consumer message queue library written in Go, based on memory-mapped files (`mmap`). This library supports maintaining message order, ensures successful message delivery, retains high performance, and avoids message loss. It also supports flexible features such as retaining recently consumed messages, periodic deletion of older consumed messages, and controlled message consumption rate.

## Features

- **Memory-Mapped Files**: Efficient storage and retrieval of messages using memory-mapped files (`mmap`).
- **Multi-Producer, Multi-Consumer**: Supports multiple producers and consumers simultaneously, ensuring thread safety and data integrity.
- **Message Order**: Guarantees that messages are consumed in the order they are produced.
- **No Message Loss**: Messages are persistently stored, ensuring no data loss even in case of service restarts.
- **Retain Recently Consumed Messages**: Configurable retention of the last N consumed messages, with periodic deletion of older messages.
- **Control Message Consumption Rate**: Supports limiting message consumption speed (e.g., 30 messages per second).
- **Service Restart Handling**: Automatically resumes from the last unconsumed message after service restarts. Also supports the option to re-consume messages from the beginning.
- **New File Detection**: The library detects when the file is newly created and initializes offsets and metadata accordingly.

Usage

- Initialize the Message Queue

```go
queue, err := NewMessageQueue("queue.dat", 1024*1024, 30)
if err != nil {
    log.Fatalf("Failed to initialize the message queue: %v", err)
}
defer queue.Close()
```

Parameters:

"queue.dat": The path to the message queue file.
1024*1024: The size of the message queue file (in bytes).
30: Message consumption rate limit (e.g., 30 messages per second).
10: Number of recently consumed messages to retain.

- Produce Messages

```go
msg := &Message{Data: []byte("Hello World")}
err := queue.Push(msg)
if err != nil {
    log.Fatalf("Failed to push message: %v", err)
}
```

- Consume Messages

```go
for {
    msg, err := queue.Pop()
    if err != nil {
        log.Fatalf("Failed to pop message: %v", err)
    }
    fmt.Printf("Consumed Message: %s\n", string(msg.Data))
}
```

Configuration
Metadata Handling
The library stores metadata, including writeOffset, readOffset, and isNewFile, at the head of the message queue file. This ensures that the library can resume operations from where it left off after service restarts.

Consumption Rate Control
The library uses a time.Ticker to control the message consumption rate. By setting the ratePerSecond parameter during initialization, you can control the number of messages consumed per second. Setting ratePerSecond to 0 disables rate limiting.

Example

```go
package main

import (
    "fmt"
    "log"
    "time"
)

func main() {
    // Initialize the message queue with a rate limit of 30 messages per second and retain 10 consumed messages.
    queue, err := NewMessageQueue("queue.dat", 1024*1024, 30, 10)
    if err != nil {
        log.Fatalf("Failed to initialize the message queue: %v", err)
    }
    defer queue.Close()

    // Producer: Push messages into the queue
    go func() {
        for i := 0; i < 100; i++ {
            msg := &Message{Data: []byte(fmt.Sprintf("Message %d", i))}
            if err := queue.Push(msg); err != nil {
                log.Fatalf("Failed to push message: %v", err)
            }
        }
    }()

    // Consumer: Pop messages from the queue
    go func() {
        for {
            msg, err := queue.Pop()
            if err != nil {
                log.Fatalf("Failed to pop message: %v", err)
            }
            fmt.Printf("Consumed: %s\n", string(msg.Data))
        }
    }()

    time.Sleep(5 * time.Second)
    queue.StopTicker()
}
```

Contributing

We welcome contributions! Here's how you can help:

Fork the repository.
Create a new branch for your feature or bug fix.
Commit your changes and push them to your fork.
Create a pull request with a detailed description of your changes.
Testing
To run the test cases, you can use the following command:

```bash
go test ./...
```

We have included various test cases to validate the core functionalities such as:

Basic message push and pop operations.
Multi-producer and multi-consumer behavior.
Handling of rate-limited consumption.
Retaining recently consumed messages.
Recovery from service restarts.
License
This project is licensed under the MIT License. See the LICENSE file for details.
