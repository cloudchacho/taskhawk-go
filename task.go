/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.

 */

package taskhawk

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// HeadersCarrier interface needs to be implemented by the input struct if your task needs to get custom headers set
// during dispatch
type HeadersCarrier interface {
	// SetHeaders sets the headers on a task input
	SetHeaders(map[string]string)

	// GetHeaders returns the headers set on a task input
	GetHeaders() map[string]string
}

// MetadataSetter interface needs to be implemented by the input struct if your task needs to get metatada (
// message id etc)
type MetadataSetter interface {
	// SetID sets the message id
	SetID(string)

	// SetPriority sets the priority a message was dispatched with
	SetPriority(Priority)

	// SetProviderMetadata represents backend provider specific metadata, e.g. AWS receipt, or Pub/Sub ack ID
	// For concrete type of metadata, check the documentation of your backend class
	SetProviderMetadata(any)

	// SetTimestamp sets the message dispatch timestamp
	SetTimestamp(time.Time)

	// SetVersion sets the message schema version
	SetVersion(Version)
}

func (t *taskDef) call(ctx context.Context, m message, providerMetadata any) error {
	if metadataSetter, ok := m.Input.(MetadataSetter); ok {
		metadataSetter.SetID(m.ID)
		metadataSetter.SetPriority(m.Metadata.Priority)
		metadataSetter.SetProviderMetadata(providerMetadata)
		metadataSetter.SetTimestamp(time.Time(m.Metadata.Timestamp))
		metadataSetter.SetVersion(m.Metadata.Version)
	}
	if headersCarrier, ok := m.Input.(HeadersCarrier); ok {
		headersCarrier.SetHeaders(m.Headers)
	}
	return t.execute(ctx, m.Input)
}

type taskDef struct {
	execute  func(ctx context.Context, input any) error
	newInput func() any
}

type TaskFn[T any] func(ctx context.Context, input *T) error

func wrapTaskFn[T any](fn TaskFn[T]) TaskFn[T] {
	return func(ctx context.Context, input *T) (err error) {
		defer func() {
			if rErr := recover(); rErr != nil {
				if typedErr, ok := rErr.(error); ok {
					err = errors.Wrapf(typedErr, "task failed with panic")
				} else {
					err = errors.Errorf("panic: %v", rErr)
				}
			}
		}()
		err = fn(ctx, input)
		return
	}
}

type Task[T any] struct {
	hub             *Hub
	defaultPriority Priority
	taskName        string
}

func (t Task[T]) Dispatch(ctx context.Context, input *T) error {
	return t.DispatchWithPriority(ctx, input, t.defaultPriority)
}

func (t Task[T]) DispatchWithPriority(ctx context.Context, input *T, priority Priority) error {
	return t.hub.dispatch(ctx, t.taskName, input, priority)
}
