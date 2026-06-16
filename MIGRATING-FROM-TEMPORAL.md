# Coming from Temporal: Fan-out / Fan-in

This guide maps [`temporalio/samples-go/splitmerge-future`](https://github.com/temporalio/samples-go/tree/main/splitmerge-future) to this Resonate example. If you have an existing fan-out / fan-in workflow in Temporal and want to understand how to express the same pattern here, start with the side-by-side below.

## The pattern

Fan-out / fan-in is a durable-execution pattern where an orchestrator launches N units of work in parallel, waits for all of them to complete, then aggregates the results. Both Temporal and Resonate represent the "unit of work" as a future — a handle you can await separately from the dispatch — and both require the same structural discipline: start all units before awaiting any, or the work runs in series.

## Side by side

### Temporal (`samples-go/splitmerge-future`)

```go
// splitmerge-future/splitmerge_workflow.go
func SampleSplitMergeFutureWorkflow(ctx workflow.Context, processorCount int) (ChunkResult, error) {
    ao := workflow.ActivityOptions{
        StartToCloseTimeout: 10 * time.Second,
    }
    ctx = workflow.WithActivityOptions(ctx, ao)

    var results []workflow.Future
    for i := 0; i < processorCount; i++ {
        // ExecuteActivity returns Future that doesn't need to be awaited immediately.
        future := workflow.ExecuteActivity(ctx, ChunkProcessingActivity, i+1)
        results = append(results, future)
    }

    var totalItemCount, totalSum int
    for i := 0; i < processorCount; i++ {
        var result ChunkResult
        // Blocks until the activity result is available.
        err := results[i].Get(ctx, &result)
        if err != nil {
            return ChunkResult{}, err
        }
        totalItemCount += result.NumberOfItemsInChunk
        totalSum += result.SumInChunk
    }

    workflow.GetLogger(ctx).Info("Workflow completed.")
    return ChunkResult{totalItemCount, totalSum}, nil
}
```

### Resonate (this example)

```go
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
```

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `workflow.ExecuteActivity(ctx, Fn, args)` | `ctx.RPC("name", args)` | Both return a future immediately without blocking |
| `workflow.Future` | `*resonate.Future` | The handle you collect in loop 1 and await in loop 2 |
| `future.Get(ctx, &result)` | `f.Await(&result)` | Blocks the orchestrator goroutine until the child promise settles |
| `workflow.ActivityOptions{StartToCloseTimeout: ...}` | (none required) | Resonate does not require per-call timeout options; pass `RPCOpts{Timeout: d}` as an optional third argument to `ctx.RPC` for a per-call deadline |
| `workflow.WithActivityOptions(ctx, ao)` | (none) | No options-wrapping step needed |
| `@workflow` / `@activity` decorator split | (none) | Go has no decorators; both orchestrator and child are plain Go functions registered via `resonate.Register`. The distinction is `workflow.Context` (determinism rules apply) vs `context.Context` (free I/O) in the function signature — not a separate type or registration call |
| Task queue routing | `Group` on `httpnet.HTTPOptions` (passed as `cfg.Network`) | Workers subscribe to a named group; defaults to `"default"`. Example: `cfg.Network = httpnet.NewHTTP(url, httpnet.HTTPOptions{Group: "my-group"})`. The `ctx.RPC` target is a registered function name, not a task-queue + activity pair |

## Porting it, step by step

1. **Remove `workflow.ActivityOptions` and `workflow.WithActivityOptions`.** Resonate does not require activity options to be passed into the context before each dispatch. Delete that block.

2. **Replace `workflow.ExecuteActivity(ctx, Fn, i+1)` with `ctx.RPC("name", args)`.** The first argument is the string name you passed to `resonate.Register` when registering the child function. Return type is `(*resonate.Future, error)` — check the error before appending.

3. **Replace `results[i].Get(ctx, &result)` with `f.Await(&result)`.** The signature drops the `ctx` parameter; the SDK manages context internally.

4. **Replace `workflow.Context` with `*resonate.Context`** in your orchestrator signature. The child function signature is identical: `func myChild(ctx *resonate.Context, args MyArgs) (MyResult, error)`.

5. **Register both functions.** In `main`, call `resonate.Register(r, "fanout", fanout)` and `resonate.Register(r, "send", send)`. No worker/starter split is required for a single-process example; the same binary acts as orchestrator and worker.

6. **Start the workflow with `fanoutFn.Run(ctx, id, args)`.** The `id` you pass becomes the root promise ID. `fanoutFn.Run` returns a `*TypedHandle[FanoutResult]`, so `h.Result(ctx)` returns `(FanoutResult, error)` directly — no output-pointer needed. (By contrast, `r.RPC` and `r.Get` return an untyped `*Handle` whose `Result(ctx, &out)` takes a pointer argument.) Pass the same `id` on a second run and the SDK returns a handle to the already-resolved promise instead of re-executing — see the `-id` flag below and the README for a live demonstration of this idempotency.

## What's different (and why)

**The two-loop discipline is identical.** Both systems require all dispatches to happen before any await. In Temporal: build `[]workflow.Future` in loop 1, call `.Get` in loop 2. In Resonate: build `[]*resonate.Future` in loop 1, call `.Await` in loop 2. Collapse them into one loop (`ctx.RPC(...); f.Await(&d)` inline) and you serialize the children — the same footgun exists in both systems. The README puts it plainly: "Collapse them into one loop and you serialize them."

**No workflow/activity split.** In Temporal, the orchestrator is a `@workflow` and each unit of work is an `@activity`; in Go they are differentiated via `workflow.Context` vs `context.Context`, and Temporal encourages (though does not require) deploying them to separate task queues. In Resonate, both are plain Go functions registered with the same `resonate.Register` call — a single binary registers both on one worker, as this example does. The real distinction is execution environment: the function that receives a `*resonate.Context` and calls `ctx.RPC` acts as the orchestrator (subject to determinism rules); the dispatched function executes with a plain context and is free to do I/O. There is no separate activity type.

**No activity options.** Temporal requires `StartToCloseTimeout` on every activity dispatch or the SDK returns an error ("at least one of ScheduleToCloseTimeout and StartToCloseTimeout is required"). Resonate does not require a timeout at dispatch time; omitting one applies a 24-hour default. Durability comes from the promise store and the worker's heartbeat. You can supply a per-call deadline with `ctx.RPC("name", args, resonate.RPCOpts{Timeout: 10 * time.Second})` when you want one.

**Per-child promise visibility.** In Temporal, each activity execution is tracked in the workflow history. In Resonate, each `ctx.RPC` call creates a named child promise visible on the dashboard at `http://localhost:8001`. A failed delivery can be inspected and re-triggered independently without replaying the full workflow history.

**Completion-order processing.** `splitmerge-future` awaits results in dispatch order (loop index). Temporal's companion sample [`splitmerge-selector`](https://github.com/temporalio/samples-go/tree/main/splitmerge-selector) uses `workflow.NewSelector` to process results as each activity completes, regardless of which finishes first. The Resonate SDK does not currently expose a selector / race primitive; results are awaited in the order you call `f.Await`, which matches `splitmerge-future`'s dispatch-order behaviour. Processing in completion order would require spawning goroutines with channels — a pattern that works but sits outside the SDK's built-in surface today.

## Notes & coverage

- **`ctx.RPC` error at dispatch time** means the promise could not be created (e.g. server unreachable, duplicate ID conflict). It does not mean the child function failed. Check this error before appending to the futures slice.
- **`f.Await` error** means the child promise settled with a failure. In this example that produces a `Delivery{OK: false}` entry rather than aborting the orchestrator — consistent with how you'd handle a partial-failure fan-in in production.
- **Resonate SDK stability.** `resonate-sdk-go` is pre-release and has no semver tag yet. The `ctx.RPC` / `*resonate.Future` / `f.Await` API used here reflects the pinned commit in `go.mod`. Expect possible API changes before `v0.1.0`.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal sample: https://github.com/temporalio/samples-go/tree/main/splitmerge-future
- Completion-order variant: https://github.com/temporalio/samples-go/tree/main/splitmerge-selector
- This example's README
