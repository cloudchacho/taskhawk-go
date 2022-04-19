package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/cloudchacho/taskhawk-go"
)

type SendEmailInput struct {
	To   string
	From string

	id               string
	priority         taskhawk.Priority
	providerMetadata any
	timestamp        time.Time
	version          taskhawk.Version
	headers          map[string]string
}

func (i *SendEmailInput) SetID(id string) {
	i.id = id
}

func (i *SendEmailInput) SetPriority(priority taskhawk.Priority) {
	i.priority = priority
}

func (i *SendEmailInput) SetProviderMetadata(pm any) {
	i.providerMetadata = pm
}

func (i *SendEmailInput) SetTimestamp(time time.Time) {
	i.timestamp = time
}

func (i *SendEmailInput) SetVersion(version taskhawk.Version) {
	i.version = version
}

func (i *SendEmailInput) GetHeaders() map[string]string {
	return i.headers
}

func (i *SendEmailInput) SetHeaders(h map[string]string) {
	i.headers = h
}

func SendEmail(ctx context.Context, input *SendEmailInput) error {
	span := trace.SpanFromContext(ctx)
	fmt.Printf("[%s/%s] Received send email task with id %s, to %s, from %s, request id %s and provider metadata %+v\n",
		span.SpanContext().TraceID(), span.SpanContext().SpanID(), input.id, input.To, input.From, input.headers["request_id"], input.providerMetadata)
	return nil
}

func SendEmailFailure(err string) func(ctx context.Context, _ *SendEmailInput) error {
	return func(ctx context.Context, _ *SendEmailInput) error {
		return errors.New(err)
	}
}

type taskRegistry struct {
	SendEmailTask taskhawk.Task[SendEmailInput]
}

func registerTasks(hub *taskhawk.Hub, fakeTaskErr string) (taskRegistry, error) {
	var t taskRegistry
	var err error
	if fakeTaskErr != "" {
		t.SendEmailTask, err = taskhawk.RegisterTask(hub, "SendEmail", SendEmailFailure(fakeTaskErr))
	} else {
		t.SendEmailTask, err = taskhawk.RegisterTask(hub, "SendEmail", SendEmail)
	}
	return t, err
}
