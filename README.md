# mqtt-go: Go client for the ConnectMQ public MQTT broker

[![Go Reference](https://pkg.go.dev/badge/github.com/connectmq/mqtt-go.svg)](https://pkg.go.dev/github.com/connectmq/mqtt-go)

Publish and subscribe on the free public MQTT broker **broker.connectmq.com** from Go, with no account needed.
This package wraps [Eclipse Paho](https://github.com/eclipse/paho.mqtt.golang) and adds context-aware helpers.
It supports TCP, TLS, WebSocket and secure WebSocket.
It also works with private brokers created on [connectmq.com](https://connectmq.com).

```bash
go get github.com/connectmq/mqtt-go
```

Requires Go 1.24 or later.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	connectmq "github.com/connectmq/mqtt-go"
)

func main() {
	ctx := context.Background()
	c, err := connectmq.Connect(ctx, nil) // mqtts://broker.connectmq.com:8883
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	prefix := connectmq.TopicPrefix() // e.g. "connectmq/demo/7f3a91"

	unsubscribe, err := c.Subscribe(ctx, prefix+"/#", 0, func(m connectmq.Message) {
		fmt.Println(m.Topic, m.String())
	})
	if err != nil {
		log.Fatal(err)
	}
	defer unsubscribe()

	// []byte and string are sent as-is; other values are encoded as JSON.
	err = c.Publish(ctx, prefix+"/temperature", map[string]float64{"value": 21.5}, 1, false)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}
```

The public area is shared, so use a random prefix to keep your topics apart from other testers.

## API

| Function / method | Description |
|---|---|
| `Connect(ctx, *Options)` | Connects; `nil` options mean the public broker over TLS |
| `(*Client).Publish(ctx, topic, payload, qos, retain)` | Publishes and waits for the acknowledgement (QoS 1/2) |
| `(*Client).Subscribe(ctx, filter, qos, handler)` | Calls `handler` for every message; returns an unsubscribe function |
| `(*Client).Messages(ctx, filter, qos)` | Channel of messages, closed when `ctx` ends |
| `(*Client).WaitForMessage(ctx, filter)` | Next message (retained messages count) |
| `(*Client).Close()`, `URL()`, `IsConnected()`, `Paho()` | Connection management; `Paho()` gives the underlying client |
| `Publish(ctx, topic, payload, qos, retain, *Options)` | One message with a short-lived connection |
| `Test(ctx, *Options)` | Connects, publishes, receives; returns `TestResult{OK, URL, Connect, RoundTrip, Err}` |
| `TopicPrefix()`, `TopicMatches(filter, topic)`, `BrokerURL(transport, host)` | Helpers |

A `Message` has the fields `Topic`, `Payload`, `QoS`, `Retained` and `Received`, plus the methods `String()` and `JSON(v)`.

### Options

| Field | Default |
|---|---|
| `Transport` | `connectmq.TLS` (`TCP` 1883, `TLS` 8883, `WS` 8083, `WSS` 8084) |
| `Host` | `broker.connectmq.com` |
| `Port` | Standard port of the transport |
| `ClientID` | `go-` plus a random suffix |
| `Username`, `Password` | Empty (not needed on the public broker) |
| `KeepAlive`, `Timeout` | 60 s, 10 s |
| `Configure` | `func(*paho.ClientOptions)` for a last will, certificates or other Paho options |

The client uses MQTT 3.1.1 and reconnects automatically.

## Examples

**Range over messages**

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
defer cancel()

messages, err := c.Messages(ctx, "connectmq/demo/7f3a91/sensors/+", 0)
if err != nil {
	log.Fatal(err)
}
for m := range messages {
	var reading struct{ Temperature float64 }
	if m.JSON(&reading) == nil {
		fmt.Println(m.Topic, reading.Temperature)
	}
}
```

**Private ConnectMQ broker**

```go
c, err := connectmq.Connect(ctx, &connectmq.Options{
	Host:      "YOUR_BROKER_HOST",
	Transport: connectmq.TLS,
	Username:  os.Getenv("MQTT_USERNAME"),
	Password:  os.Getenv("MQTT_PASSWORD"),
})
```

## CLI

```bash
go install github.com/connectmq/mqtt-go/cmd/connectmq@latest

connectmq test                                       # connectivity and round-trip
connectmq info                                       # endpoints and limits
connectmq prefix                                     # random topic prefix
connectmq sub "connectmq/demo/7f3a91/#"
connectmq pub -q 1 -r connectmq/demo/7f3a91/temp '{"value":21.5}'
echo hello | connectmq pub connectmq/demo/7f3a91/msg
```

Flags go before the topic: `-t tcp|tls|ws|wss`, `-host`, `-port`, `-q 0|1|2`, `-r` (retain), `-n <count>` and `-json` (sub), `-u` and `-p` (credentials).

## Public broker

| Protocol | URL |
|---|---|
| MQTT (TCP) | `mqtt://broker.connectmq.com:1883` |
| MQTT over TLS | `mqtts://broker.connectmq.com:8883` |
| WebSocket | `ws://broker.connectmq.com:8083/mqtt` |
| Secure WebSocket | `wss://broker.connectmq.com:8084/mqtt` |

Access is anonymous, but the client ID must not be empty. Limits: 64 KB per message, 10 messages per second and 10 subscriptions per client, topics up to 8 levels deep, and sessions expire after at most 30 minutes.

> **Everything on the public broker is public.** Any client can subscribe to `#`.
> Use the broker only for testing, and always under a random topic prefix.
> For production, create a private broker with its own credentials at [connectmq.com](https://connectmq.com).

More: [Free Public MQTT Broker for Testing](https://connectmq.com/blog/free-public-mqtt-broker).
Also available for Python (`pip install connectmq`), Node.js (`@connectmq/mqtt`), .NET (`ConnectMQ.Mqtt`), Node-RED (`node-red-contrib-connectmq`) and PowerShell (`MQTTBroker`).

## Testing

```bash
go test ./...          # includes live tests against broker.connectmq.com
go test -short ./...   # offline tests only
```

## License

MIT
