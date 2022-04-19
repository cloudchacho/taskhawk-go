package gcp_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/iterator"

	"github.com/cloudchacho/taskhawk-go"
	"github.com/cloudchacho/taskhawk-go/gcp"
	"github.com/cloudchacho/taskhawk-go/internal/testutils"
)

type fakeLog struct {
	level   string
	err     error
	message string
	fields  taskhawk.LoggingFields
}

type fakeLogger struct {
	lock sync.Mutex
	logs []fakeLog
}

func (f *fakeLogger) Error(err error, message string, fields taskhawk.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"error", err, message, fields})
}

func (f *fakeLogger) Warn(err error, message string, fields taskhawk.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"warn", err, message, fields})
}

func (f *fakeLogger) Info(message string, fields taskhawk.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"info", nil, message, fields})
}

func (f *fakeLogger) Debug(message string, fields taskhawk.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"debug", nil, message, fields})
}

func (s *BackendTestSuite) publish(payload []byte, attributes map[string]string, topic, project string) error {
	if project == "" {
		project = s.settings.GoogleCloudProject
	}
	ctx := context.Background()
	_, err := s.client.TopicInProject(topic, project).Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: attributes,
	}).Get(ctx)
	return err
}

func (s *BackendTestSuite) TestReceive() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	err := s.publish(payload, attributes, "taskhawk-dev-myapp", "")
	s.Require().NoError(err)

	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo": "bar",
	}
	err = s.publish(payload2, attributes2, "taskhawk-dev-myapp", "")
	s.Require().NoError(err)

	m := mock.Mock{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for receivedMessage := range s.messageCh {
			m.MethodCalled("MessageReceived", receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
		}
	}()
	m.On("MessageReceived", payload, attributes, mock.AnythingOfType("gcp.Metadata")).
		// message must be acked or Receive never returns
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(2))
			s.Require().NoError(err)
		}).
		Return().
		Once()
	m.On("MessageReceived", payload2, attributes2, mock.AnythingOfType("gcp.Metadata")).
		// message must be acked or Receive never returns
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(2))
			s.Require().NoError(err)
			cancel()
		}).
		Return().
		Once().
		After(time.Millisecond * 50)

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.True(err.Error() == "draining" || err == context.Canceled)
		close(s.messageCh)
	})
	wg.Wait()

	if m.AssertExpectations(s.T()) {
		providerMetadata := m.Calls[0].Arguments.Get(2).(gcp.Metadata)
		s.Equal(1, providerMetadata.DeliveryAttempt)
	}
}

func (s *BackendTestSuite) TestReceiveNoMessages() {
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.True(err.Error() == "draining" || err == context.DeadlineExceeded)
	})

	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestReceiveError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	s.settings.QueueName = "does-not-exist"

	backend := gcp.NewBackend(s.settings, nil)

	testutils.RunAndWait(func() {
		err := backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.EqualError(err, "rpc error: code = NotFound desc = Subscription does not exist (resource=taskhawk-does-not-exist)")
	})

	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQ() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	err := s.publish(payload, attributes, "taskhawk-dev-myapp-dlq", "")
	s.Require().NoError(err)

	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo": "bar",
	}
	err = s.publish(payload2, attributes2, "taskhawk-dev-myapp-dlq", "")
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err = s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.True(err.Error() == "draining" || err == context.DeadlineExceeded)
	})

	received := [2]int32{}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	err = s.client.Subscription("taskhawk-dev-myapp").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
		if bytes.Equal(message.Data, payload) {
			atomic.AddInt32(&received[0], 1)
			s.Equal(message.Attributes, attributes)
		} else {
			s.Equal(message.Data, payload2)
			s.Equal(message.Attributes, attributes2)
			atomic.AddInt32(&received[1], 1)
		}
		if atomic.LoadInt32(&received[0]) >= 1 && atomic.LoadInt32(&received[1]) >= 1 {
			cancel()
		}
		message.Ack()
	})
	s.Require().NoError(err)
	s.True(atomic.LoadInt32(&received[0]) >= 1 && atomic.LoadInt32(&received[1]) >= 1)
}

func (s *BackendTestSuite) TestRequeueDLQNoMessages() {
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.True(err == context.DeadlineExceeded)
	})
}

func (s *BackendTestSuite) TestRequeueDLQReceiveError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	s.settings.QueueName = "does-not-exist"

	backend := gcp.NewBackend(s.settings, nil)

	testutils.RunAndWait(func() {
		err := backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "rpc error: code = NotFound desc = Subscription does not exist (resource=taskhawk-does-not-exist-dlq)")
	})
}

func (s *BackendTestSuite) TestRequeueDLQPublishError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	err := s.publish(payload, attributes, "taskhawk-dev-myapp-dlq", "")
	s.Require().NoError(err)

	topic := s.client.Topic("taskhawk-dev-myapp")
	err = topic.Delete(ctx)
	s.Require().NoError(err)

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "rpc error: code = NotFound desc = Topic not found")
	})
}

func (s *BackendTestSuite) TestPublish() {
	ctx, cancel := context.WithCancel(context.Background())

	messageID, err := s.backend.Publish(ctx, s.payload, s.attributes, taskhawk.PriorityDefault)
	s.NoError(err)
	s.NotEmpty(messageID)

	err = s.client.Subscription("taskhawk-dev-myapp").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
		cancel()
		s.Equal(message.Data, s.payload)
		s.Equal(message.Attributes, s.attributes)
		message.Ack()
	})
	s.Require().NoError(err)
}

func (s *BackendTestSuite) TestAck() {
	ctx := context.Background()

	messageID, err := s.backend.Publish(ctx, s.payload, s.attributes, taskhawk.PriorityDefault)
	s.NoError(err)
	s.NotEmpty(messageID)

	ctx2, cancel2 := context.WithCancel(ctx)
	err = s.client.Subscription("taskhawk-dev-myapp").Receive(ctx2, func(_ context.Context, message *pubsub.Message) {
		defer cancel2()
		s.Equal(message.Data, s.payload)
		s.Equal(message.Attributes, s.attributes)
		message.Ack()
	})
	s.NoError(err)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.client.Subscription("taskhawk-dev-myapp").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
			s.Fail("shouldn't have received any message")
		})
		s.Require().NoError(err)
	})
}

func (s *BackendTestSuite) TestNack() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	messageID, err := s.backend.Publish(ctx, s.payload, s.attributes, taskhawk.PriorityDefault)
	s.NoError(err)
	s.NotEmpty(messageID)

	m := mock.Mock{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for receivedMessage := range s.messageCh {
			m.MethodCalled("MessageReceived", receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
		}
	}()
	m.On("MessageReceived", s.payload, s.attributes, mock.AnythingOfType("gcp.Metadata")).
		Run(func(args mock.Arguments) {
			err := s.backend.NackMessage(ctx, args.Get(2))
			s.Require().NoError(err)
			cancel()
		}).
		Return()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.True(err.Error() == "draining" || err == context.Canceled)
		close(s.messageCh)
	})
	wg.Wait()

	m.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNew() {
	assert.NotNil(s.T(), s.backend)
}

type BackendTestSuite struct {
	suite.Suite
	backend    *gcp.Backend
	client     *pubsub.Client
	settings   gcp.Settings
	payload    []byte
	attributes map[string]string
	messageCh  chan taskhawk.ReceivedMessage
}

func (s *BackendTestSuite) SetupSuite() {
	s.TearDownSuite()
	ctx := context.Background()
	if s.client == nil {
		client, err := pubsub.NewClient(ctx, "emulator-project")
		s.Require().NoError(err)
		s.client = client
	}
	dlqTopic, err := s.client.CreateTopic(ctx, "taskhawk-dev-myapp-dlq")
	s.Require().NoError(err)
	_, err = s.client.CreateSubscription(ctx, "taskhawk-dev-myapp-dlq", pubsub.SubscriptionConfig{
		Topic:       dlqTopic,
		AckDeadline: time.Second * 20,
	})
	s.Require().NoError(err)
	topic, err := s.client.CreateTopic(ctx, "taskhawk-dev-myapp")
	s.Require().NoError(err)
	_, err = s.client.CreateSubscription(ctx, "taskhawk-dev-myapp", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Second * 20,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: 5,
		},
	})
	s.Require().NoError(err)
}

func (s *BackendTestSuite) TearDownSuite() {
	ctx := context.Background()
	if s.client == nil {
		client, err := pubsub.NewClient(ctx, "emulator-project")
		s.Require().NoError(err)
		s.client = client
	}
	defer func() {
		s.Require().NoError(s.client.Close())
		s.client = nil
	}()
	subscriptions := s.client.Subscriptions(ctx)
	for {
		if subscription, err := subscriptions.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to delete subscriptions with error: %v", err))
		} else {
			err = subscription.Delete(ctx)
			s.Require().NoError(err)
		}
	}
	topics := s.client.Topics(ctx)
	for {
		if topic, err := topics.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to delete topics with error: %v", err))
		} else {
			err = topic.Delete(ctx)
			s.Require().NoError(err)
		}
	}
}

func (s *BackendTestSuite) SetupTest() {
	logger := &fakeLogger{}

	settings := gcp.Settings{
		GoogleCloudProject: "emulator-project",
		QueueName:          "dev-myapp",
	}
	getLogger := func(_ context.Context) taskhawk.Logger {
		return logger
	}

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{"foo": "bar"}

	s.backend = gcp.NewBackend(settings, getLogger)
	s.settings = settings
	s.payload = payload
	s.attributes = attributes
	s.messageCh = make(chan taskhawk.ReceivedMessage)
}

func (s *BackendTestSuite) TearDownTest() {
	ctx := context.Background()
	subscriptions := s.client.Subscriptions(ctx)
	for {
		if subscription, err := subscriptions.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to drain subscriptions with error: %v", err))
		} else {
			err = subscription.SeekToTime(ctx, time.Now())
			s.Require().NoError(err)
		}
	}
}

func TestBackendTestSuite(t *testing.T) {
	suite.Run(t, &BackendTestSuite{})
}
