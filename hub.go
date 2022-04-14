/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.
 */

package taskhawk

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Hub is the central struct used to dispatch Taskhawk tasks / run consumer
type Hub struct {
	tasks     map[string]taskDef
	config    Config
	publisher publisher
}

// Config used to configure Taskhawk Hub
type Config struct {
	// Sync changes taskhawk dispatch to synchronous mode. This is similar
	// to Celery's Eager mode and is helpful for integration testing
	Sync bool

	// Instrumenter for the consumer
	Instrumenter Instrumenter

	// GetLogger returns the logger object for given context
	GetLogger GetLoggerFunc
}

// NewHub creates a hub
func NewHub(config Config, backend PublisherBackend) *Hub {
	return &Hub{
		publisher: publisher{backend: backend, serializer: jsonifier{}, instrumenter: config.Instrumenter},
		config:    config,
		tasks:     map[string]taskDef{},
	}
}

// dispatch a task asynchronously with specified priority.
// The concrete type of input is expected to be same as the concrete type of NewInput()'s return value.
func (h *Hub) dispatch(ctx context.Context, taskName string, input any, priority Priority) error {
	headers := make(map[string]string)
	if headersCarrier, ok := input.(HeadersCarrier); ok {
		for key, value := range headersCarrier.GetHeaders() {
			headers[key] = value
		}
	}

	m, err := newMessage(
		taskName,
		input,
		headers,
		uuid.NewV4().String(),
		priority,
	)
	if err != nil {
		return err
	}

	if h.config.Sync {
		task := h.tasks[taskName]
		return task.call(ctx, m, nil)
	}

	return h.publisher.Publish(ctx, m)
}

func (h *Hub) getTask(name string) (taskDef, error) {
	task, ok := h.tasks[name]
	if !ok {
		return task, fmt.Errorf("%w: %s", ErrTaskNotFound, name)
	}
	return task, nil
}

// ListenForMessages starts a taskhawk listener for the provided message types
//
// Cancelable context may be used to cancel processing of messages
func (h *Hub) ListenForMessages(ctx context.Context, request ListenRequest, backend ConsumerBackend) error {
	c := queueConsumer{
		consumer{
			backend:      backend,
			deserializer: jsonifier{},
			getLogger:    h.config.GetLogger,
			instrumenter: h.config.Instrumenter,
			hub:          h,
		},
	}
	c.initDefaults()
	return c.ListenForMessages(ctx, request)
}

// RequeueDLQ re-queues everything in the taskhawk DLQ back into the taskhawk queue
func (h *Hub) RequeueDLQ(ctx context.Context, request ListenRequest, backend ConsumerBackend) error {
	c := queueConsumer{
		consumer{
			backend:      backend,
			deserializer: jsonifier{},
			getLogger:    h.config.GetLogger,
			instrumenter: h.config.Instrumenter,
			hub:          h,
		},
	}
	c.initDefaults()
	return c.RequeueDLQ(ctx, request)
}

// RegisterTask registers the task to the hub with priority 'default'.
// Priority may be overridden at dispatch time using `DispatchWithPriority`.
func RegisterTask[T any](h *Hub, taskName string, taskFn TaskFn[T]) (Task[T], error) {
	return RegisterTaskWithPriority(h, taskName, taskFn, PriorityDefault)
}

// RegisterTaskWithPriority registers the task to the hub with specified priority.
// This will set the default priority, and may be overridden at dispatch time using `DispatchWithPriority`.
func RegisterTaskWithPriority[T any](h *Hub, taskName string, taskFn TaskFn[T], defaultPriority Priority) (Task[T], error) {
	if taskName == "" {
		return Task[T]{}, errors.New("task name not set")
	}
	if _, found := h.tasks[taskName]; found {
		return Task[T]{}, errors.Errorf("task with name '%s' already registered", taskName)
	}
	taskFn = wrapTaskFn(taskFn)
	h.tasks[taskName] = taskDef{
		execute: func(ctx context.Context, data any) error {
			return taskFn(ctx, data.(*T))
		},
		newInput: func() any {
			return new(T)
		},
	}
	task := Task[T]{
		hub:             h,
		defaultPriority: defaultPriority,
		taskName:        taskName,
	}
	return task, nil
}
