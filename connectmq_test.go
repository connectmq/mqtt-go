package connectmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTopicMatches(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/+/c", "a/b/c", true},
		{"a/#", "a", true},
		{"a/#", "a/b/c", true},
		{"a/+", "a/b/c", false},
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"$share/g/a/#", "a/x", true},
	}
	for _, c := range cases {
		if got := TopicMatches(c.filter, c.topic); got != c.want {
			t.Errorf("TopicMatches(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}

func TestEncode(t *testing.T) {
	for in, want := range map[any]string{"text": "text", 42: "42"} {
		got, err := encode(in)
		if err != nil || string(got) != want {
			t.Errorf("encode(%v) = %q, %v; want %q", in, got, err, want)
		}
	}
	if got, _ := encode(map[string]int{"a": 1}); string(got) != `{"a":1}` {
		t.Errorf("encode(map) = %q", got)
	}
}

// The tests below use the live public broker; skip them with -short.

func TestLiveTransports(t *testing.T) {
	if testing.Short() {
		t.Skip("live broker test")
	}
	for _, tr := range []Transport{TCP, TLS, WS, WSS} {
		r := Test(context.Background(), &Options{Transport: tr})
		if !r.OK {
			t.Errorf("%s: %v", r.URL, r.Err)
			continue
		}
		t.Logf("%s connect %v, round-trip %v", r.URL, r.Connect, r.RoundTrip)
	}
}

func TestLiveRetainedAndMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("live broker test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Connect(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	prefix := TopicPrefix()

	if err := c.Publish(ctx, prefix+"/r", map[string]int{"x": 1}, 2, true); err != nil {
		t.Fatal(err)
	}
	m, err := c.WaitForMessage(ctx, prefix+"/+")
	if err != nil {
		t.Fatal(err)
	}
	var v struct{ X int }
	if err := m.JSON(&v); err != nil || v.X != 1 || !m.Retained {
		t.Fatalf("retained message = %+v (%s), err %v", m, m.String(), err)
	}
	if err := c.Publish(ctx, prefix+"/r", "", 0, true); err != nil {
		t.Fatal(err)
	}

	subCtx, stop := context.WithCancel(ctx)
	ch, err := c.Messages(subCtx, prefix+"/b/#", 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Publish(ctx, prefix+"/b/c", []byte{byte('0' + i)}, 1, false); err != nil {
			t.Fatal(err)
		}
	}
	var got string
	for len(got) < 3 {
		select {
		case m := <-ch:
			got += m.String()
		case <-time.After(5 * time.Second):
			t.Fatalf("received only %q", got)
		}
	}
	stop()
	if got != "012" {
		t.Errorf("messages = %q, want 012", got)
	}
	for range ch { // drained and closed after cancel
	}

	short, cancelShort := context.WithTimeout(ctx, time.Second)
	defer cancelShort()
	if _, err := c.WaitForMessage(short, prefix+"/none"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("WaitForMessage timeout error = %v", err)
	}
}

func TestLiveBadPort(t *testing.T) {
	if testing.Short() {
		t.Skip("live broker test")
	}
	_, err := Connect(context.Background(), &Options{Transport: TCP, Port: 1999, Timeout: 3 * time.Second})
	if err == nil {
		t.Fatal("expected an error")
	}
	t.Log(err)
}
