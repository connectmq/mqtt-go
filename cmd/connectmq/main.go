// Command connectmq is an MQTT command-line client for the free public broker broker.connectmq.com.
//
//	go install github.com/connectmq/mqtt-go/cmd/connectmq@latest
//
//	connectmq test                                   # connectivity and round-trip
//	connectmq info                                   # endpoints and limits
//	connectmq prefix                                 # random topic prefix
//	connectmq sub -n 1 "connectmq/demo/7f3a91/#"     # subscribe
//	connectmq pub -q 1 -r connectmq/demo/7f3a91/temp '{"value":21.5}'
//
// Flags come before the topic. pub reads the message from stdin when it is omitted.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	connectmq "github.com/connectmq/mqtt-go"
)

const usage = `Usage: connectmq <command> [flags] [args]

Commands:
  test                     Check connectivity and message round-trip (default)
  info                     Show public broker endpoints and limits
  prefix                   Print a random topic prefix
  sub [flags] <topic>      Subscribe and print messages (Ctrl+C to stop)
  pub [flags] <topic> [message]
                           Publish a message (reads stdin if message is omitted)

Flags (before the topic):
  -t tcp|tls|ws|wss        Transport (default tls)
  -host <host>             Broker host (default broker.connectmq.com)
  -port <port>             Port override
  -q 0|1|2                 QoS (default 0)
  -r                       Retained message (pub)
  -n <count>               Exit after n messages (sub)
  -json                    JSON line output (sub)
  -u <user> -p <password>  Credentials for private brokers

Public area: anyone can read your messages. Private brokers: https://connectmq.com
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command := "test"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}

	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	transport := fs.String("t", "tls", "")
	host := fs.String("host", "", "")
	port := fs.Int("port", 0, "")
	qos := fs.Int("q", 0, "")
	retain := fs.Bool("r", false, "")
	count := fs.Int("n", 0, "")
	asJSON := fs.Bool("json", false, "")
	user := fs.String("u", "", "")
	password := fs.String("p", "", "")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *qos < 0 || *qos > 2 {
		return fmt.Errorf("invalid QoS %d", *qos)
	}
	opts := &connectmq.Options{Transport: connectmq.Transport(*transport), Host: *host, Port: *port, Username: *user, Password: *password}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch command {
	case "info":
		urls := map[string]string{}
		for _, t := range []connectmq.Transport{connectmq.TCP, connectmq.TLS, connectmq.WS, connectmq.WSS} {
			urls[string(t)] = connectmq.BrokerURL(t, "")
		}
		out, _ := json.MarshalIndent(map[string]any{
			"host":           connectmq.BrokerHost,
			"urls":           urls,
			"authentication": "Anonymous (non-empty client ID required)",
			"limits":         map[string]any{"maxMessageSize": "64 KB", "publishRate": "10 messages/second per client", "subscriptions": 10, "maxTopicDepth": 8, "qos": []int{0, 1, 2}},
		}, "", "  ")
		fmt.Println(string(out))
	case "prefix":
		fmt.Println(connectmq.TopicPrefix())
	case "test":
		r := connectmq.Test(ctx, opts)
		if !r.OK {
			fmt.Printf("FAIL  %s  %v\n", r.URL, r.Err)
			os.Exit(1)
		}
		fmt.Printf("OK  %s  connect %d ms, round-trip %d ms\n", r.URL, r.Connect.Milliseconds(), r.RoundTrip.Milliseconds())
	case "pub":
		if fs.NArg() < 1 {
			return fmt.Errorf("missing topic: connectmq pub [flags] <topic> [message]")
		}
		message := strings.Join(fs.Args()[1:], " ")
		if fs.NArg() == 1 {
			data, err := io.ReadAll(os.Stdin)
			if err != nil {
				return err
			}
			message = strings.TrimRight(string(data), "\r\n")
		}
		if err := connectmq.Publish(ctx, fs.Arg(0), message, byte(*qos), *retain, opts); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "Published to", fs.Arg(0))
	case "sub":
		if fs.NArg() != 1 {
			return fmt.Errorf("usage: connectmq sub [flags] <topic>")
		}
		c, err := connectmq.Connect(ctx, opts)
		if err != nil {
			return err
		}
		defer c.Close()
		messages, err := c.Messages(ctx, fs.Arg(0), byte(*qos))
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Subscribed to %s on %s. Press Ctrl+C to stop.\n", fs.Arg(0), c.URL())
		received := 0
		for m := range messages {
			if *asJSON {
				line, _ := json.Marshal(map[string]any{"topic": m.Topic, "payload": m.String(), "qos": m.QoS, "retain": m.Retained, "received": m.Received})
				fmt.Println(string(line))
			} else {
				fmt.Printf("%s %s %s\n", m.Received.Format("15:04:05"), m.Topic, m.String())
			}
			if received++; *count > 0 && received >= *count {
				break
			}
		}
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", command)
	}
	return nil
}
