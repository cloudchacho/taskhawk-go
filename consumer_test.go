package taskhawk

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type contextKey string

type fakeLog struct {
	level   string
	err     error
	message string
	fields  LoggingFields
}

type fakeLogger struct {
	lock sync.Mutex
	logs []fakeLog
}

func (f *fakeLogger) Error(err error, message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"error", err, message, fields})
}

func (f *fakeLogger) Warn(err error, message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"warn", err, message, fields})
}

func (f *fakeLogger) Info(message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"info", nil, message, fields})
}

func (f *fakeLogger) Debug(message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"debug", nil, message, fields})
}

type fakeTaskInput struct {
	To string
}

type fakeTask struct {
	mock.Mock
}

func (fc *fakeTask) Call(ctx context.Context, input *fakeTaskInput) error {
	args := fc.Called(ctx, input)
	return args.Error(0)
}

type fakeBackend struct {
	mock.Mock
}

func (b *fakeBackend) Receive(ctx context.Context, priority Priority, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- ReceivedMessage) error {
	args := b.Called(ctx, priority, numMessages, visibilityTimeout, messageCh)
	return args.Error(0)
}

func (b *fakeBackend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	args := b.Called(ctx, providerMetadata)
	return args.Error(0)
}

func (b *fakeBackend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	args := b.Called(ctx, providerMetadata)
	return args.Error(0)
}

func (b *fakeBackend) Publish(ctx context.Context, payload []byte, attributes map[string]string, priority Priority) (string, error) {
	args := b.Called(ctx, payload, attributes, priority)
	return args.String(0), args.Error(1)
}

func (b *fakeBackend) RequeueDLQ(ctx context.Context, priority Priority, numMessages uint32, visibilityTimeout time.Duration) error {
	args := b.Called(ctx, priority, numMessages, visibilityTimeout)
	return args.Error(0)
}

type fakeInstrumenter struct {
	mock.Mock
}

func (f *fakeInstrumenter) OnTask(ctx context.Context, taskName string) {
	f.Called(ctx, taskName)
}

func (f *fakeInstrumenter) OnDispatch(ctx context.Context, taskName string, attributes map[string]string) (context.Context, map[string]string, func()) {
	args := f.Called(ctx, taskName, attributes)
	return args.Get(0).(context.Context), args.Get(1).(map[string]string), args.Get(2).(func())
}

func (f *fakeInstrumenter) OnReceive(ctx context.Context, attributes map[string]string) (context.Context, func()) {
	args := f.Called(ctx, attributes)
	return args.Get(0).(context.Context), args.Get(1).(func())
}

func (s *ConsumerTestSuite) TestProcessMessage() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.taskFn.On("Call", ctx, input).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageDeserializeFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(message{}, errors.New("invalid message"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "invalid message, unable to unmarshal")
	s.EqualError(s.logger.logs[0].err, "invalid message")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.taskFn.On("Call", ctx, input).
		Return(errors.New("failed to process"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.EqualError(s.logger.logs[0].err, "failed to process")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackPanic() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.taskFn.On("Call", ctx, input).
		Panic("failed to process")
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.EqualError(s.logger.logs[0].err, "panic: failed to process")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackErrRetry() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.taskFn.On("Call", ctx, input).
		Return(ErrRetry)
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Retrying due to exception")
	s.NoError(s.logger.logs[0].err)
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageTaskNotFound() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	delete(s.hub.tasks, "main.SendEmail")
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Contains(s.logger.logs[0].message, "no task found")
	s.EqualError(s.logger.logs[0].err, "task not found: main.SendEmail")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessNackFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.taskFn.On("Call", ctx, input).
		Return(errors.New("failed to process"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(errors.New("failed to nack"))
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 2)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.EqualError(s.logger.logs[0].err, "failed to process")
	s.Equal(s.logger.logs[1].message, "Failed to nack message")
	s.EqualError(s.logger.logs[1].err, "failed to nack")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageAckFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.taskFn.On("Call", ctx, input).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(errors.New("failed to ack"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Failed to ack message")
	s.EqualError(s.logger.logs[0].err, "failed to ack")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageFollowsParentTrace() {
	ctx := context.Background()
	instrumentedCtx := context.WithValue(ctx, contextKey("instrumented"), true)
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123", "traceparent": "00-aa2ada259e917551e16da4a0ad33db24-662fd261d30ec74c-01"}
	providerMetadata := struct{}{}
	instrumenter := &fakeInstrumenter{}
	s.consumer.instrumenter = instrumenter
	input := &fakeTaskInput{To: "fake@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.taskFn.On("Call", instrumentedCtx, input).
		Return(nil)
	s.backend.On("AckMessage", instrumentedCtx, providerMetadata).
		Return(nil)
	called := false
	instrumenter.On("OnReceive", ctx, attributes).
		Return(instrumentedCtx, func() { called = true })
	instrumenter.On("OnTask", instrumentedCtx, m.TaskName)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
	instrumenter.AssertExpectations(s.T())
	s.True(called)
}

func (s *ConsumerTestSuite) TestListenForMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 20
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	payload2 := []byte(`foobar2`)
	attributes2 := map[string]string{"request_id": "456"}
	providerMetadata2 := struct{}{}
	input := &fakeTaskInput{To: "fake@example.com"}
	input2 := &fakeTaskInput{To: "fake2@example.com"}
	m := message{TaskName: "main.SendEmail", Input: input}
	m2 := message{TaskName: "main.SendEmail", Input: input2}
	s.deserializer.On("deserialize", s.hub, payload, attributes).
		Return(m, nil)
	s.deserializer.On("deserialize", s.hub, payload2, attributes2).
		Return(m2, nil)
	s.taskFn.On("Call", ctx, input2).
		Return(nil).
		After(time.Millisecond * 50)
	s.taskFn.On("Call", ctx, input).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata2).
		Return(nil)
	s.backend.On("Receive", ctx, PriorityDefault, numMessages, visibilityTimeout, mock.AnythingOfType("chan<- taskhawk.ReceivedMessage")).
		Return(context.Canceled).
		Run(func(args mock.Arguments) {
			ch := args.Get(4).(chan<- ReceivedMessage)
			ch <- ReceivedMessage{
				Payload:          payload,
				Attributes:       attributes,
				ProviderMetadata: providerMetadata,
			}
			ch <- ReceivedMessage{
				Payload:          payload2,
				Attributes:       attributes2,
				ProviderMetadata: providerMetadata2,
			}
		}).
		After(500 * time.Millisecond)
	err := s.consumer.ListenForMessages(ctx, ListenRequest{PriorityDefault, numMessages, visibilityTimeout, 1})
	assert.EqualError(s.T(), err, "context canceled")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.taskFn.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestNew() {
	assert.NotNil(s.T(), s.consumer)
}

type fakeDeserializer struct {
	mock.Mock
}

func (f *fakeDeserializer) deserialize(h *Hub, messagePayload []byte, attributes map[string]string) (message, error) {
	args := f.Called(h, messagePayload, attributes)
	return args.Get(0).(message), args.Error(1)
}

type ConsumerTestSuite struct {
	suite.Suite
	consumer     *queueConsumer
	backend      *fakeBackend
	taskFn       *fakeTask
	logger       *fakeLogger
	deserializer *fakeDeserializer
	hub          *Hub
}

func (s *ConsumerTestSuite) SetupTest() {
	taskFn := &fakeTask{}
	logger := &fakeLogger{}
	getLogger := func(_ context.Context) Logger {
		return logger
	}

	backend := &fakeBackend{}
	deserializer := &fakeDeserializer{}
	hub := NewHub(Config{}, backend)
	_, err := RegisterTask(hub, "main.SendEmail", taskFn.Call)
	s.Require().NoError(err)

	s.consumer = &queueConsumer{consumer{
		backend:      backend,
		deserializer: deserializer,
		instrumenter: nil,
		getLogger:    getLogger,
		hub:          hub,
	}}
	s.hub = hub
	s.consumer.deserializer = deserializer
	s.backend = backend
	s.taskFn = taskFn
	s.deserializer = deserializer
	s.logger = logger
}

func TestConsumerTestSuite(t *testing.T) {
	suite.Run(t, &ConsumerTestSuite{})
}
