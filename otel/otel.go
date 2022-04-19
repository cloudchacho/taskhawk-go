package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/cloudchacho/taskhawk-go"
)

const (
	tracerName = "github.com/cloudchacho/taskhawk-go/otel"
)

type Instrumenter struct {
	tp   trace.TracerProvider
	prop propagation.TextMapPropagator
}

var _ = taskhawk.Instrumenter(&Instrumenter{})

func (o *Instrumenter) OnTask(ctx context.Context, taskName string) {
	currentSpan := trace.SpanFromContext(ctx)
	currentSpan.SetName(taskName)
}

func (o *Instrumenter) OnDispatch(ctx context.Context, taskName string, attributes map[string]string) (context.Context, map[string]string, func()) {
	carrier := propagation.MapCarrier(attributes)
	o.prop.Inject(ctx, carrier)

	name := fmt.Sprintf("publish/%s", taskName)
	ctx, span := o.tp.Tracer(tracerName).Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer))

	return ctx, carrier, func() { span.End() }
}

func (o *Instrumenter) OnReceive(ctx context.Context, attributes map[string]string) (context.Context, func()) {
	ctx = o.prop.Extract(ctx, propagation.MapCarrier(attributes))

	name := "message_received"
	ctx, span := o.tp.Tracer(tracerName).Start(ctx, name, trace.WithSpanKind(trace.SpanKindConsumer))

	return ctx, func() { span.End() }
}

func NewInstrumenter(tracerProvider trace.TracerProvider, propagator propagation.TextMapPropagator) *Instrumenter {
	return &Instrumenter{tp: tracerProvider, prop: propagator}
}
