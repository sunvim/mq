# 消息队列库

这是一个高性能的多生产者、多消费者消息队列库，基于 Go 语言开发，使用了内存映射文件（`mmap`）。该库支持保持消息顺序，确保消息成功投递，具有高性能，并且避免消息丢失。它还支持灵活的功能，例如保留最近消费的消息、定期删除已消费的旧消息，并支持控制消息消费速度。

## 功能

- **内存映射文件**：使用内存映射文件（`mmap`）进行高效的消息存储和读取。
- **多生产者、多消费者**：支持多个生产者和消费者同时操作，确保线程安全和数据一致性。
- **消息顺序**：保证消息按照生产的顺序被消费。
- **消息不丢失**：消息持久化存储，确保在服务重启后不会丢失数据。
- **保留最近消费的消息**：可配置保留最后 N 条已消费的消息，并定期删除旧的已消费消息。
- **控制消息消费速率**：支持限制消息消费的速度（例如每秒 30 条消息）。
- **服务重启处理**：在服务重启后自动从未消费的消息继续消费，支持从头重新消费消息。
- **新建文件检测**：库可以检测文件是否为新创建，必要时进行元数据初始化。

使用方法

- 初始化消息队列

```go
queue, err := NewMessageQueue("queue.dat", 1024*1024, 30, 10)
if err != nil {
    log.Fatalf("无法初始化消息队列: %v", err)
}
defer queue.Close()
```

参数：

"queue.dat"：消息队列文件的路径。
1024*1024：消息队列文件的大小（以字节为单位）。
30：消息消费速率限制（例如每秒 30 条消息）。
10：保留最近消费的消息数量。

- 生产消息

```go
Copy code
msg := &Message{Data: []byte("Hello World")}
err := queue.Push(msg)
if err != nil {
    log.Fatalf("推送消息失败: %v", err)
}
```

- 消费消息

```go
for {
    msg, err := queue.Pop()
    if err != nil {
        log.Fatalf("消费消息失败: %v", err)
    }
    fmt.Printf("消费消息: %s\n", string(msg.Data))
}
```

配置说明
元数据处理
该库将元数据（包括 writeOffset、readOffset 和 isNewFile）存储在消息队列文件的头部。这确保了在服务重启后可以从上次未消费的位置继续运行。

消息消费速率控制
该库使用 time.Ticker 来控制消息消费速率。在初始化时，通过设置 ratePerSecond 参数可以控制每秒消费的消息数量。将 ratePerSecond 设置为 0 时，将禁用速率限制。

示例代码

```go
package main

import (
    "fmt"
    "log"
    "time"
)

func main() {
    // 初始化消息队列，消费速率限制为每秒 30 条消息，并保留最近 10 条消费的消息。
    queue, err := NewMessageQueue("queue.dat", 1024*1024, 30, 10)
    if err != nil {
        log.Fatalf("无法初始化消息队列: %v", err)
    }
    defer queue.Close()

    // 生产者：向队列中推送消息
    go func() {
        for i := 0; i < 100; i++ {
            msg := &Message{Data: []byte(fmt.Sprintf("Message %d", i))}
            if err := queue.Push(msg); err != nil {
                log.Fatalf("推送消息失败: %v", err)
            }
        }
    }()

    // 消费者：从队列中消费消息
    go func() {
        for {
            msg, err := queue.Pop()
            if err != nil {
                log.Fatalf("消费消息失败: %v", err)
            }
            fmt.Printf("消费消息: %s\n", string(msg.Data))
        }
    }()

    time.Sleep(5 * 秒)
    queue.StopTicker()
}
```

贡献
欢迎贡献！以下是贡献代码的步骤：

Fork 此仓库。
创建一个新分支用于你的功能或修复。
提交你的修改并推送到你的 Fork。
提交一个 Pull Request，详细描述你的更改。
测试
使用以下命令运行测试：

```bash
go test ./...
```

我们的测试用例涵盖了以下核心功能：

- 基本的消息推送和消费操作。
- 多生产者和多消费者的行为。
- 消息消费速率限制的处理。
- 服务重启后的恢复能力。

许可证
此项目基于 MIT 许可证。请查看 LICENSE 文件以获取详细信息。
