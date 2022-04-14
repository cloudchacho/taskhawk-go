package taskhawk

import (
	"context"
)

// publisher handles task publishing
type publisher struct {
	backend      PublisherBackend
	instrumenter Instrumenter
	serializer   serializer
}

// Publish a message on Hedwig
func (p *publisher) Publish(ctx context.Context, m message) error {
	payload, attributes, err := p.serializer.serialize(m)
	if err != nil {
		return err
	}

	if p.instrumenter != nil {
		var finalize func()
		ctx, attributes, finalize = p.instrumenter.OnDispatch(ctx, m.TaskName, attributes)
		defer finalize()
	}

	_, err = p.backend.Publish(ctx, payload, attributes, m.Metadata.Priority)
	return err
}

type serializer interface {
	serialize(m message) ([]byte, map[string]string, error)
}

// PublisherBackend is used to publish messages to a transport
type PublisherBackend interface {
	// Publish a message represented by the payload, with specified attributes to the topic with specified priority
	Publish(ctx context.Context, payload []byte, attributes map[string]string, priority Priority) (string, error)
}
