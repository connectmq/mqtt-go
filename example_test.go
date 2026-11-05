package connectmq_test

import (
	"context"
	"fmt"
	"log"
	"time"

	connectmq "github.com/connectmq/mqtt-go"
)

// Publish and subscribe on the public broker with one connection.
func Example() {
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

	// Values other than []byte and string are sent as JSON.
	if err := c.Publish(ctx, prefix+"/temperature", map[string]float64{"value": 21.5}, 1, false); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}

func ExampleClient_Messages() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	c, err := connectmq.Connect(ctx, &connectmq.Options{Transport: connectmq.WSS})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	messages, err := c.Messages(ctx, "connectmq/demo/7f3a91/sensors/+", 0)
	if err != nil {
		log.Fatal(err)
	}
	for m := range messages { // ends when ctx is done
		var reading struct{ Temperature float64 }
		if err := m.JSON(&reading); err == nil {
			fmt.Println(m.Topic, reading.Temperature)
		}
	}
}

func ExampleClient_WaitForMessage() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := connectmq.Connect(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	m, err := c.WaitForMessage(ctx, "connectmq/demo/7f3a91/reply")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(m.String())
}

func ExampleTest() {
	r := connectmq.Test(context.Background(), &connectmq.Options{Transport: connectmq.TLS})
	if r.OK {
		fmt.Printf("connected in %v, round-trip %v\n", r.Connect, r.RoundTrip)
	} else {
		fmt.Println("failed:", r.Err)
	}
}

// Connect to a private ConnectMQ broker.
func ExampleConnect_privateBroker() {
	c, err := connectmq.Connect(context.Background(), &connectmq.Options{
		Host:      "YOUR_BROKER_HOST",
		Transport: connectmq.TLS,
		Username:  "device01",
		Password:  "secret",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
}

func ExampleTopicMatches() {
	fmt.Println(connectmq.TopicMatches("plant/+/temp", "plant/line1/temp"))
	fmt.Println(connectmq.TopicMatches("plant/#", "plant/line1/temp"))
	fmt.Println(connectmq.TopicMatches("plant/+", "plant/line1/temp"))
	// Output:
	// true
	// true
	// false
}

func ExampleBrokerURL() {
	fmt.Println(connectmq.BrokerURL(connectmq.WSS, ""))
	// Output: wss://broker.connectmq.com:8084/mqtt
}
