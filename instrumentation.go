package taskhawk

import "context"

// Instrumenter defines the interface for Taskhawk's instrumentation
type Instrumenter interface {
	// OnReceive is called as soon as possible after a message is received from the backend. Caller must call
	// the returned finalized function when processing for the message is finished (typically done via defer).
	// The context must be replaced with the returned context for the remainder of the operation.
	// This is where a new span must be started.
	OnReceive(ctx context.Context, attributes map[string]string) (context.Context, func())

	// OnTask is called when a message has been received from the backend and decoded
	// This is where span attributes, such as name, may be updated.
	OnTask(ctx context.Context, taskName string)

	// OnDispatch is called right before a message is published. Caller must call
	// the returned finalized function when publishing for the message is finished (typically done via defer).
	// The attributes may be updated to include trace id for downstream consumers.
	OnDispatch(ctx context.Context, taskName string, attributes map[string]string) (context.Context, map[string]string, func())
}
