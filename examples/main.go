package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	uuid "github.com/satori/go.uuid"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/cloudchacho/taskhawk-go"
	taskhawkOtel "github.com/cloudchacho/taskhawk-go/otel"
)

func runConsumer(hub *taskhawk.Hub, backend taskhawk.ConsumerBackend) {
	err := hub.ListenForMessages(context.Background(), taskhawk.ListenRequest{}, backend)
	if err != nil {
		panic(fmt.Sprintf("Failed to consume messages: %v", err))
	}
}

func runPublisher(tr taskRegistry) {
	ctx := context.Background()
	tp := sdktrace.NewTracerProvider()
	tracer := tp.Tracer("github.com/cloudchacho/taskhawk-go/examples")
	ctx, span := tracer.Start(ctx, "publisher")
	defer span.End()
	input := &SendEmailInput{To: "hello@example.com", From: "spam@example.com"}
	requestID := uuid.NewV4().String()
	input.SetHeaders(map[string]string{"request_id": requestID})
	err := tr.SendEmailTask.Dispatch(ctx, input)
	if err != nil {
		panic(fmt.Sprintf("Failed to publish message: %v", err))
	}
	fmt.Printf("[%s/%s], Published message successfully with request id %s\n",
		span.SpanContext().TraceID(), span.SpanContext().SpanID(), requestID)
}

func requeueDLQ(hub *taskhawk.Hub, backend taskhawk.ConsumerBackend) {
	err := hub.RequeueDLQ(context.Background(), taskhawk.ListenRequest{}, backend)
	if err != nil {
		panic(fmt.Sprintf("Failed to requeue messages: %v", err))
	}
}

func main() {
	var consumerBackend taskhawk.ConsumerBackend
	var publisherBackend taskhawk.PublisherBackend
	var propagator propagation.TextMapPropagator

	backendName := "aws"
	if isGCPStr, found := os.LookupEnv("TASKHAWK_GCP"); found && strings.ToLower(isGCPStr) == "true" {
		backendName = "gcp"
	}
	fakeTaskErr := ""
	if fakeConsumerErrStr, found := os.LookupEnv("FAKE_TASK_ERROR"); found {
		fakeTaskErr = fakeConsumerErrStr
	}

	if backendName == "aws" {
		b := awsBackend()
		consumerBackend = b
		publisherBackend = b
		propagator = awsPropagator()
	} else {
		b := gcpBackend()
		consumerBackend = b
		publisherBackend = b
		propagator = gcpPropagator()
	}

	instrumenter := taskhawkOtel.NewInstrumenter(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	), propagator)

	hub := taskhawk.NewHub(taskhawk.Config{Instrumenter: instrumenter}, publisherBackend)

	tr, err := registerTasks(hub, fakeTaskErr)
	if err != nil {
		panic("failed to register tasks")
	}

	switch os.Args[1] {
	case "consumer":
		runConsumer(hub, consumerBackend)
	case "publisher":
		runPublisher(tr)
	case "requeue-dlq":
		requeueDLQ(hub, consumerBackend)
	default:
		panic(fmt.Sprintf("unknown command: %s", os.Args[1]))
	}
}
