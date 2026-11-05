// Package connectmq is an MQTT client preconfigured for the free public broker
// broker.connectmq.com, built on Eclipse Paho (github.com/eclipse/paho.mqtt.golang).
//
// It connects anonymously over TCP (1883), TLS (8883), WebSocket (8083) or secure
// WebSocket (8084), and adds context-aware Publish, Subscribe, Messages and
// WaitForMessage helpers on top of Paho.
//
// Everything on the public broker can be read by anyone: use it for testing only,
// and put your topics under a random prefix (see [TopicPrefix]). For production,
// create a private broker at https://connectmq.com and set [Options.Host],
// [Options.Username] and [Options.Password].
package connectmq

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// BrokerHost is the host name of the ConnectMQ public broker.
const BrokerHost = "broker.connectmq.com"

// Transport selects how the client reaches the broker.
type Transport string

// Supported transports.
const (
	TCP Transport = "tcp" // MQTT over TCP, port 1883
	TLS Transport = "tls" // MQTT over TLS, port 8883
	WS  Transport = "ws"  // MQTT over WebSocket, port 8083, path /mqtt
	WSS Transport = "wss" // MQTT over secure WebSocket, port 8084, path /mqtt
)

var endpoints = map[Transport]struct {
	scheme string
	port   int
	path   string
}{
	TCP: {"tcp", 1883, ""},
	TLS: {"ssl", 8883, ""},
	WS:  {"ws", 8083, "/mqtt"},
	WSS: {"wss", 8084, "/mqtt"},
}

var displaySchemes = map[string]string{"tcp": "mqtt", "ssl": "mqtts", "ws": "ws", "wss": "wss"}

// BrokerURL returns the URL of a broker endpoint, for example
// "mqtts://broker.connectmq.com:8883". An empty host means [BrokerHost].
func BrokerURL(t Transport, host string) string {
	e, ok := endpoints[t]
	if !ok {
		return ""
	}
	if host == "" {
		host = BrokerHost
	}
	return fmt.Sprintf("%s://%s:%d%s", displaySchemes[e.scheme], host, e.port, e.path)
}

// TopicPrefix returns a random topic prefix such as "connectmq/demo/7f3a91".
// The public area is shared, so a random prefix keeps your topics apart from other testers.
func TopicPrefix() string {
	return "connectmq/demo/" + randomHex(3)
}

// TopicMatches reports whether topic matches an MQTT topic filter with the
// wildcards + and #, including shared subscriptions ($share/group/filter).
func TopicMatches(filter, topic string) bool {
	if strings.HasPrefix(filter, "$share/") {
		parts := strings.SplitN(filter, "/", 3)
		if len(parts) < 3 {
			return false
		}
		filter = parts[2]
	}
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i, level := range f {
		if level == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if level != "+" && level != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// Options configures [Connect]. The zero value connects anonymously to the
// public broker over TLS with a random client ID.
type Options struct {
	Transport Transport     // default TLS
	Host      string        // default broker.connectmq.com
	Port      int           // default: the standard port of Transport
	ClientID  string        // default "go-" plus a random suffix; must be unique
	Username  string        // for private brokers
	Password  string        // for private brokers
	KeepAlive time.Duration // default 60s
	Timeout   time.Duration // connect and acknowledgement timeout, default 10s

	// Configure, when set, is called with the Paho options before connecting,
	// for settings such as a last will or custom TLS certificates.
	Configure func(*paho.ClientOptions)
}

// Message is a received MQTT message.
type Message struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retained bool
	Received time.Time
}

// String returns the payload as text.
func (m Message) String() string { return string(m.Payload) }

// JSON decodes the payload into v.
func (m Message) JSON(v any) error { return json.Unmarshal(m.Payload, v) }

// Client is a connected MQTT client. Close it when done.
type Client struct {
	paho    paho.Client
	url     string
	timeout time.Duration
	mu      sync.Mutex
	subs    map[string]int // topic filter -> number of active subscriptions
}

// Connect opens a connection to the broker described by opts (nil means the public broker over TLS).
func Connect(ctx context.Context, opts *Options) (*Client, error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	if o.Transport == "" {
		o.Transport = TLS
	}
	e, ok := endpoints[o.Transport]
	if !ok {
		return nil, fmt.Errorf("connectmq: unknown transport %q (use tcp, tls, ws or wss)", o.Transport)
	}
	if o.Host == "" {
		o.Host = BrokerHost
	}
	if o.Port == 0 {
		o.Port = e.port
	}
	if o.ClientID == "" {
		o.ClientID = "go-" + randomHex(4)
	}
	if o.KeepAlive == 0 {
		o.KeepAlive = 60 * time.Second
	}
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}

	po := paho.NewClientOptions().
		AddBroker(fmt.Sprintf("%s://%s:%d%s", e.scheme, o.Host, o.Port, e.path)).
		SetClientID(o.ClientID).
		SetKeepAlive(o.KeepAlive).
		SetConnectTimeout(o.Timeout).
		SetWriteTimeout(o.Timeout).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOrderMatters(false)
	if e.scheme == "ssl" || e.scheme == "wss" {
		po.SetTLSConfig(&tls.Config{ServerName: o.Host, MinVersion: tls.VersionTLS12})
	}
	if o.Username != "" {
		po.SetUsername(o.Username).SetPassword(o.Password)
	}
	if o.Configure != nil {
		o.Configure(po)
	}

	c := &Client{
		paho:    paho.NewClient(po),
		url:     fmt.Sprintf("%s://%s:%d%s", displaySchemes[e.scheme], o.Host, o.Port, e.path),
		timeout: o.Timeout,
		subs:    map[string]int{},
	}
	if err := c.wait(ctx, c.paho.Connect()); err != nil {
		c.paho.Disconnect(0)
		return nil, fmt.Errorf("connectmq: connect to %s: %w", c.url, err)
	}
	return c, nil
}

// URL returns the broker URL this client is connected to.
func (c *Client) URL() string { return c.url }

// IsConnected reports whether the connection is currently up.
func (c *Client) IsConnected() bool { return c.paho.IsConnectionOpen() }

// Paho returns the underlying Paho client, for anything not covered by this package.
func (c *Client) Paho() paho.Client { return c.paho }

// Publish sends a message. A []byte or string payload is sent as is; any other
// value is encoded as JSON. Publish an empty payload with retain=true to clear
// a retained message. For QoS 1 and 2 it waits for the broker's acknowledgement.
func (c *Client) Publish(ctx context.Context, topic string, payload any, qos byte, retain bool) error {
	data, err := encode(payload)
	if err != nil {
		return err
	}
	if err := c.wait(ctx, c.paho.Publish(topic, qos, retain, data)); err != nil {
		return fmt.Errorf("connectmq: publish to %q: %w", topic, err)
	}
	return nil
}

// Subscribe calls handler for every message matching filter, on a Paho goroutine.
// The returned function unsubscribes. Only one handler per filter is active at a time.
func (c *Client) Subscribe(ctx context.Context, filter string, qos byte, handler func(Message)) (unsubscribe func() error, err error) {
	token := c.paho.Subscribe(filter, qos, func(_ paho.Client, m paho.Message) {
		handler(Message{Topic: m.Topic(), Payload: m.Payload(), QoS: m.Qos(), Retained: m.Retained(), Received: time.Now()})
	})
	if err := c.wait(ctx, token); err != nil {
		return nil, fmt.Errorf("connectmq: subscribe to %q: %w", filter, err)
	}
	if st, ok := token.(*paho.SubscribeToken); ok {
		for f, code := range st.Result() {
			if code >= 0x80 {
				return nil, fmt.Errorf("connectmq: subscription to %q was rejected by the broker", f)
			}
		}
	}
	c.mu.Lock()
	c.subs[filter]++
	c.mu.Unlock()

	var once sync.Once
	return func() error {
		var err error
		once.Do(func() {
			c.mu.Lock()
			c.subs[filter]--
			last := c.subs[filter] == 0
			if last {
				delete(c.subs, filter)
			}
			c.mu.Unlock()
			if last && c.paho.IsConnectionOpen() {
				err = c.wait(context.Background(), c.paho.Unsubscribe(filter))
			}
		})
		return err
	}, nil
}

// Messages subscribes to filter and delivers messages on the returned channel
// until ctx is cancelled; the channel is then closed and the subscription removed.
// Messages are dropped if the channel buffer (64) is full.
func (c *Client) Messages(ctx context.Context, filter string, qos byte) (<-chan Message, error) {
	ch := make(chan Message, 64)
	var mu sync.Mutex
	closed := false
	unsubscribe, err := c.Subscribe(ctx, filter, qos, func(m Message) {
		mu.Lock()
		defer mu.Unlock()
		if closed {
			return
		}
		select {
		case ch <- m:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = unsubscribe()
		mu.Lock()
		closed = true
		close(ch)
		mu.Unlock()
	}()
	return ch, nil
}

// WaitForMessage returns the next message matching filter, or an error when ctx ends.
// Retained messages count, so it returns immediately if one is stored for the topic.
func (c *Client) WaitForMessage(ctx context.Context, filter string) (Message, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := c.Messages(ctx, filter, 0)
	if err != nil {
		return Message{}, err
	}
	select {
	case m := <-ch:
		return m, nil
	case <-ctx.Done():
		return Message{}, fmt.Errorf("connectmq: no message on %q: %w", filter, ctx.Err())
	}
}

// Close disconnects from the broker.
func (c *Client) Close() {
	c.paho.Disconnect(250)
}

// Publish sends one message using a short-lived connection (nil opts means the public broker over TLS).
func Publish(ctx context.Context, topic string, payload any, qos byte, retain bool, opts *Options) error {
	c, err := Connect(ctx, opts)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Publish(ctx, topic, payload, qos, retain)
}

// TestResult is the result of [Test].
type TestResult struct {
	URL       string
	OK        bool
	Connect   time.Duration
	RoundTrip time.Duration
	Err       error
}

// Test connects, subscribes, publishes and receives a test message.
// Errors are reported in TestResult.Err.
func Test(ctx context.Context, opts *Options) TestResult {
	var o Options
	if opts != nil {
		o = *opts
	}
	if o.Transport == "" {
		o.Transport = TLS
	}
	r := TestResult{URL: BrokerURL(o.Transport, o.Host)}
	start := time.Now()
	c, err := Connect(ctx, &o)
	if err != nil {
		r.Err = err
		return r
	}
	defer c.Close()
	r.URL = c.URL()
	r.Connect = time.Since(start)

	topic := TopicPrefix() + "/ping"
	token := randomHex(8)
	got := make(chan struct{}, 1)
	unsubscribe, err := c.Subscribe(ctx, topic, 0, func(m Message) {
		if string(m.Payload) == token {
			select {
			case got <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		r.Err = err
		return r
	}
	defer unsubscribe()
	sent := time.Now()
	if err := c.Publish(ctx, topic, token, 0, false); err != nil {
		r.Err = err
		return r
	}
	select {
	case <-got:
		r.OK, r.RoundTrip = true, time.Since(sent)
	case <-time.After(10 * time.Second):
		r.Err = errors.New("connectmq: round-trip timed out")
	case <-ctx.Done():
		r.Err = ctx.Err()
	}
	return r
}

func (c *Client) wait(ctx context.Context, t paho.Token) error {
	timer := time.NewTimer(c.timeout)
	defer timer.Stop()
	select {
	case <-t.Done():
		return t.Error()
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("timed out waiting for the broker")
	}
}

func encode(payload any) ([]byte, error) {
	switch p := payload.(type) {
	case nil:
		return []byte{}, nil
	case []byte:
		return p, nil
	case string:
		return []byte(p), nil
	default:
		data, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("connectmq: encode payload as JSON: %w", err)
		}
		return data, nil
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
