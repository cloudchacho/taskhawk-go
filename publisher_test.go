package taskhawk

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func (s *PublisherTestSuite) TestPublish() {
	ctx := context.Background()

	data := fakeTaskInput{
		To: "mail@example.com",
	}
	m, err := newMessage("main.SendEmail", &data, map[string]string{}, uuid.NewV4().String(), PriorityDefault)
	s.Require().NoError(err)

	payload := []byte(`{"task": "main.SendEmail"}`)
	headers := map[string]string{}

	s.serializer.On("serialize", m).
		Return(payload, headers, nil)

	messageID := "123"

	s.backend.On("Publish", ctx, payload, headers, PriorityDefault).
		Return(messageID, nil)

	err = s.publisher.Publish(ctx, m)
	s.NoError(err)

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestPublishSerializeError() {
	ctx := context.Background()

	data := fakeTaskInput{
		To: "mail@example.com",
	}
	m, err := newMessage("main.SendEmail", &data, map[string]string{}, uuid.NewV4().String(), PriorityDefault)
	s.Require().NoError(err)

	s.serializer.On("serialize", m).
		Return([]byte(""), map[string]string{}, errors.New("failed to serialize"))

	err = s.publisher.Publish(ctx, m)
	s.EqualError(err, "failed to serialize")

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestPublishSendsTraceID() {
	ctx := context.Background()
	instrumentedCtx := context.WithValue(ctx, contextKey("instrumented"), true)

	data := fakeTaskInput{
		To: "mail@example.com",
	}
	m, err := newMessage("main.SendEmail", &data, map[string]string{}, uuid.NewV4().String(), PriorityDefault)
	s.Require().NoError(err)

	payload := []byte(`{"task": "main.SendEmail"}`)
	headers := map[string]string{}

	instrumentedHeaders := map[string]string{"traceparent": "00-aa2ada259e917551e16da4a0ad33db24-662fd261d30ec74c-01"}

	instrumenter := &fakeInstrumenter{}
	s.publisher.instrumenter = instrumenter

	called := false

	instrumenter.On("OnDispatch", ctx, m.TaskName, headers).
		Return(instrumentedCtx, instrumentedHeaders, func() { called = true })

	s.serializer.On("serialize", m).
		Return(payload, headers, nil)

	messageID := "123"

	s.backend.On("Publish", instrumentedCtx, payload, instrumentedHeaders, PriorityDefault).
		Return(messageID, nil)

	err = s.publisher.Publish(ctx, m)
	s.NoError(err)

	s.backend.AssertExpectations(s.T())
	instrumenter.AssertExpectations(s.T())
	s.True(called)
}

func (s *PublisherTestSuite) TestNew() {
	assert.NotNil(s.T(), s.publisher)
}

type PublisherTestSuite struct {
	suite.Suite
	publisher  *publisher
	backend    *fakeBackend
	serializer *fakeSerializer
}

type fakeSerializer struct {
	mock.Mock
}

func (f *fakeSerializer) serialize(m message) ([]byte, map[string]string, error) {
	args := f.Called(m)
	return args.Get(0).([]byte), args.Get(1).(map[string]string), args.Error(2)
}

func (f *fakeSerializer) withUseTransportMessageAttributes(useTransportMessageAttributes bool) {
	f.Called(useTransportMessageAttributes)
}

func (s *PublisherTestSuite) SetupTest() {
	backend := &fakeBackend{}
	ser := &fakeSerializer{}

	s.publisher = &publisher{
		backend:      backend,
		instrumenter: nil,
		serializer:   ser,
	}
	s.backend = backend
	s.serializer = ser
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, &PublisherTestSuite{})
}
