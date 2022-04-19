package taskhawk

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type consumer struct {
	backend      ConsumerBackend
	deserializer deserializer
	instrumenter Instrumenter
	getLogger    GetLoggerFunc
	hub          *Hub
}

type queueConsumer struct {
	consumer
}

func (c *consumer) processMessage(ctx context.Context, payload []byte, attributes map[string]string, providerMetadata any) {
	if c.instrumenter != nil {
		var finalize func()
		ctx, finalize = c.instrumenter.OnReceive(ctx, attributes)
		defer finalize()
	}

	var acked bool
	loggingFields := LoggingFields{"message_body": string(payload)}

	// must ack or nack message, otherwise receive call never returns even on context cancelation
	defer func() {
		if !acked {
			err := c.backend.NackMessage(ctx, providerMetadata)
			if err != nil {
				c.getLogger(ctx).Error(err, "Failed to nack message", loggingFields)
			}
		}
	}()

	m, err := c.deserializer.deserialize(c.hub, payload, attributes)
	if err != nil {
		c.getLogger(ctx).Error(err, "invalid message, unable to unmarshal", loggingFields)
		return
	}

	if c.instrumenter != nil {
		c.instrumenter.OnTask(ctx, m.TaskName)
	}

	loggingFields = LoggingFields{"message_id": m.ID, "type": m.TaskName}

	var task taskDef
	if task, err = c.hub.getTask(m.TaskName); err != nil {
		msg := fmt.Sprintf("no task found with name: %s", m.TaskName)
		c.getLogger(ctx).Error(err, msg, loggingFields)
		return
	}

	err = task.call(ctx, m, providerMetadata)
	switch err {
	case nil:
		ackErr := c.backend.AckMessage(ctx, providerMetadata)
		if ackErr != nil {
			c.getLogger(ctx).Error(ackErr, "Failed to ack message", loggingFields)
		} else {
			acked = true
		}
	case ErrRetry:
		c.getLogger(ctx).Debug("Retrying due to exception", loggingFields)
	default:
		c.getLogger(ctx).Error(err, "Retrying due to unknown exception", loggingFields)
	}
}

func (c *queueConsumer) ListenForMessages(ctx context.Context, request ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}
	if request.NumConcurrency == 0 {
		request.NumConcurrency = 1
	}

	messageCh := make(chan ReceivedMessage)

	wg := &sync.WaitGroup{}
	// start n concurrent workers to receive messages from the channel
	for i := uint32(0); i < request.NumConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					// drain channel before returning
					for receivedMessage := range messageCh {
						c.processMessage(ctx, receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
					}
					return
				case receivedMessage := <-messageCh:
					c.processMessage(ctx, receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
				}
			}
		}()
	}
	// wait for all receive goroutines to finish
	defer wg.Wait()

	// close channel to indicate no more message will be published and receive goroutines spawned above should return
	defer close(messageCh)

	return c.backend.Receive(ctx, request.Priority, request.NumMessages, request.VisibilityTimeout, messageCh)
}

// RequeueDLQ re-queues everything in the taskhawk DLQ back into the taskhawk queue
func (c *queueConsumer) RequeueDLQ(ctx context.Context, request ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	return c.backend.RequeueDLQ(ctx, request.Priority, request.NumMessages, request.VisibilityTimeout)
}

func (c *queueConsumer) initDefaults() {
	if c.getLogger == nil {
		stdLogger := &StdLogger{}
		c.getLogger = func(_ context.Context) Logger { return stdLogger }
	}
}

type deserializer interface {
	deserialize(h *Hub, messagePayload []byte, attributes map[string]string) (message, error)
}

// ErrRetry should cause the task to retry, but not treat the retry as an error
var ErrRetry = errors.New("Retry error")

// ConsumerBackend is used for consuming messages from a transport
type ConsumerBackend interface {
	// Receive messages from configured queue(s) and provide it through the channel. This should run indefinitely
	// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
	// The channel must not be closed by the backend.
	Receive(ctx context.Context, priority Priority, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- ReceivedMessage) error

	// NackMessage nacks a message on the queue
	NackMessage(ctx context.Context, providerMetadata any) error

	// AckMessage acknowledges a message on the queue
	AckMessage(ctx context.Context, providerMetadata any) error

	//HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error

	// RequeueDLQ re-queues everything in the taskhawk DLQ back into the taskhawk queue
	RequeueDLQ(ctx context.Context, priority Priority, numMessages uint32, visibilityTimeout time.Duration) error
}

// ReceivedMessage is the message as received by a transport backend.
type ReceivedMessage struct {
	Payload          []byte
	Attributes       map[string]string
	ProviderMetadata any
}

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	// Priority queue to listen to
	Priority Priority

	// How many messages to fetch at one time
	NumMessages uint32 // default 1

	// How long should the message be hidden from other consumers?
	VisibilityTimeout time.Duration // defaults to queue configuration

	// How many goroutines to spin for processing messages concurrently
	NumConcurrency uint32 // default 1
}
