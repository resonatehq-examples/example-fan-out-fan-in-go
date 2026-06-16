// Package main runs a fan-out / fan-in workflow: dispatch a message to N
// parallel channels, await every delivery, aggregate the result.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

type FanoutArgs struct {
	Channels []string `json:"channels"`
	Message  string   `json:"message"`
}

type FanoutResult struct {
	Delivered []Delivery `json:"delivered"`
}

type SendArgs struct {
	Channel string `json:"channel"`
	Message string `json:"message"`
}

type Delivery struct {
	Channel string `json:"channel"`
	OK      bool   `json:"ok"`
	Reason  string `json:"reason,omitempty"`
}

// fanout dispatches one ctx.RPC call per channel without awaiting in between,
// then awaits all of them. The dispatches happen in series in user code but
// run concurrently on the server side; the Awaits block until every child
// promise settles.
func fanout(ctx *resonate.Context, args FanoutArgs) (FanoutResult, error) {
	futures := make([]*resonate.Future, 0, len(args.Channels))
	for _, ch := range args.Channels {
		f, err := ctx.RPC("send", SendArgs{Channel: ch, Message: args.Message})
		if err != nil {
			return FanoutResult{}, err
		}
		futures = append(futures, f)
	}

	out := FanoutResult{Delivered: make([]Delivery, 0, len(futures))}
	for i, f := range futures {
		var d Delivery
		if err := f.Await(&d); err != nil {
			out.Delivered = append(out.Delivered, Delivery{
				Channel: args.Channels[i],
				OK:      false,
				Reason:  err.Error(),
			})
			continue
		}
		out.Delivered = append(out.Delivered, d)
	}
	return out, nil
}

// send is the child function that "delivers" a message to one channel. In a
// real system this would call a provider API (Twilio, SES, Slack, ...). Here
// it just prints and returns a Delivery record.
func send(_ *resonate.Context, args SendArgs) (Delivery, error) {
	fmt.Printf("  [send] %-8s <- %q\n", args.Channel, args.Message)
	return Delivery{Channel: args.Channel, OK: true}, nil
}

func main() {
	channels := flag.String("channels", "email,sms,slack,push", "comma-separated channel names")
	message := flag.String("message", "Hello from Resonate", "message to deliver")
	id := flag.String("id", "fanout-1", "promise ID; rerun with the same ID to get the cached result (idempotency demo)")
	flag.Parse()

	chs := strings.Split(*channels, ",")
	for i := range chs {
		chs[i] = strings.TrimSpace(chs[i])
	}

	r, err := resonate.New(resonate.Config{URL: "http://localhost:8001"})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	fanoutFn, err := resonate.Register(r, "fanout", fanout)
	if err != nil {
		log.Fatalf("Register fanout: %v", err)
	}
	if _, err := resonate.Register(r, "send", send); err != nil {
		log.Fatalf("Register send: %v", err)
	}

	ctx := context.Background()
	args := FanoutArgs{Channels: chs, Message: *message}

	fmt.Printf("[fanout] starting workflow id=%s channels=%v\n", *id, args.Channels)
	h, err := fanoutFn.Run(ctx, *id, args)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}

	fmt.Println("[fanout] done")
	for _, d := range out.Delivered {
		status := "OK"
		if !d.OK {
			status = "FAIL: " + d.Reason
		}
		fmt.Printf("  %-8s %s\n", d.Channel, status)
	}
}
