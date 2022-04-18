package aws

import (
	"context"
	"encoding/base64"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cloudchacho/taskhawk-go/internal/testutils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/cloudchacho/taskhawk-go"
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

type fakeSQS struct {
	mock.Mock
	// fake interface here
	sqsiface.SQSAPI
}

func (fs *fakeSQS) SendMessageWithContext(ctx aws.Context, in *sqs.SendMessageInput, opts ...request.Option) (*sqs.SendMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
}

// revive:disable:var-naming
func (fs *fakeSQS) GetQueueUrlWithContext(ctx aws.Context, in *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.GetQueueUrlOutput), args.Error(1)
}

// revive:enable:var-naming

func (fs *fakeSQS) ReceiveMessageWithContext(ctx aws.Context, in *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (fs *fakeSQS) DeleteMessageWithContext(ctx aws.Context, in *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

func (fs *fakeSQS) DeleteMessageBatchWithContext(ctx aws.Context, in *sqs.DeleteMessageBatchInput, opts ...request.Option) (*sqs.DeleteMessageBatchOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.DeleteMessageBatchOutput), args.Error(1)
}

func (fs *fakeSQS) SendMessageBatchWithContext(ctx aws.Context, in *sqs.SendMessageBatchInput, opts ...request.Option) (*sqs.SendMessageBatchOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.SendMessageBatchOutput), args.Error(1)
}

type fakeSNS struct {
	mock.Mock
	// fake interface here
	snsiface.SNSAPI
}

func (fs *fakeSNS) PublishWithContext(ctx aws.Context, in *sns.PublishInput, opts ...request.Option) (*sns.PublishOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sns.PublishOutput), args.Error(1)
}

func (s *BackendTestSuite) TestReceive() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	firstReceiveTime, err := time.Parse(time.RFC3339Nano, "2011-01-19T22:15:10.456000000-07:00")
	s.Require().NoError(err)
	sentTime, err := time.Parse(time.RFC3339Nano, "2011-01-19T22:15:10.123000000-07:00")
	s.Require().NoError(err)
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	body2 := `vbI9vCDijJg=`
	messageID2 := "456"
	sqsMessage2 := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":               {StringValue: aws.String("bar")},
			"taskhawk_encoding": {StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body2),
		MessageId: aws.String(messageID2),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage, &sqsMessage2},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	providerMetadata := Metadata{
		ReceiptHandle:    receiptHandle,
		FirstReceiveTime: firstReceiveTime.UTC(),
		SentTime:         sentTime.UTC(),
		ReceiveCount:     receiveCount,
	}
	m := mock.Mock{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for receivedMessage := range s.messageCh {
			m.MethodCalled("MessageReceived", receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
		}
	}()
	m.On("MessageReceived", payload, attributes, providerMetadata).
		Return().
		Once()
	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo":               "bar",
		"taskhawk_encoding": "base64",
	}
	m.On("MessageReceived", payload2, attributes2, providerMetadata).
		Return().
		Once()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.EqualError(err, "context canceled")
		close(s.messageCh)
	})

	wg.Wait()

	s.fakeSQS.AssertExpectations(s.T())
	m.AssertExpectations(s.T())
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestReceiveFailedNonUTF8Decoding() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `foobar`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":               {StringValue: aws.String("bar")},
			"taskhawk_encoding": {StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{&sqsMessage}}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.EqualError(err, "context canceled")
	})

	close(s.messageCh)

	s.fakeSQS.AssertExpectations(s.T())
	s.Empty(s.messageCh)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Invalid message payload - couldn't decode using base64")
	s.Error(s.logger.logs[0].err)
}

func (s *BackendTestSuite) TestReceiveNoMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.EqualError(err, "context canceled")
	})
	close(s.messageCh)

	s.fakeSQS.AssertExpectations(s.T())
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestReceiveError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, errors.New("no internet")).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.EqualError(err, "failed to receive SQS message: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestReceiveGetQueueError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestReceiveMissingAttributes() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiptHandle := "123"
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String(""),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String(""),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(""),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	providerMetadata := Metadata{
		ReceiptHandle:    receiptHandle,
		FirstReceiveTime: time.Time{},
		SentTime:         time.Time{},
		ReceiveCount:     -1,
	}
	m := mock.Mock{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for receivedMessage := range s.messageCh {
			m.MethodCalled("MessageReceived", receivedMessage.Payload, receivedMessage.Attributes, receivedMessage.ProviderMetadata)
		}
	}()
	m.On("MessageReceived", payload, attributes, providerMetadata).
		Return().
		Once()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout, s.messageCh)
		s.EqualError(err, "context canceled")
		close(s.messageCh)
	})

	wg.Wait()

	s.fakeSQS.AssertExpectations(s.T())
	m.AssertExpectations(s.T())
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQ() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	dlqName := "TASKHAWK-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiptHandle2 := "foobar2"
	body2 := `vbI9vCDijJg=`
	messageID2 := "456"
	sqsMessage2 := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle2),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":               {StringValue: aws.String("bar")},
			"taskhawk_encoding": {StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body2),
		MessageId: aws.String(messageID2),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage, &sqsMessage2},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput2 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID2),
		MessageAttributes: sqsMessage2.MessageAttributes,
		MessageBody:       aws.String(body2),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1, &sendInput2},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.SendMessageBatchResultEntry{
			{Id: aws.String(messageID)},
			{Id: aws.String(messageID2)},
		},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil).
		Once()

	deleteInput := &sqs.DeleteMessageBatchInput{
		Entries: []*sqs.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String(messageID),
				ReceiptHandle: aws.String(receiptHandle),
			},
			{
				Id:            aws.String(messageID2),
				ReceiptHandle: aws.String(receiptHandle2),
			},
		},
		QueueUrl: aws.String(dlqURL),
	}
	deleteOutput := &sqs.DeleteMessageBatchOutput{
		Failed: nil,
		Successful: []*sqs.DeleteMessageBatchResultEntry{
			{Id: aws.String(messageID)},
			{Id: aws.String(messageID2)},
		},
	}

	s.fakeSQS.On("DeleteMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), deleteInput, []request.Option(nil)).
		Return(deleteOutput, nil).
		Once().
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "context canceled")
	})

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQNoMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	dlqName := "TASKHAWK-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.NoError(err)
	})

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQReceiveError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "TASKHAWK-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, errors.New("no internet")).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to receive SQS message: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQPublishError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "TASKHAWK-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return((*sqs.SendMessageBatchOutput)(nil), errors.New("no internet")).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to send messages: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQPublishPartialError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "TASKHAWK-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{
			{Id: aws.String(messageID)},
		},
		Successful: []*sqs.SendMessageBatchResultEntry{},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil).
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to send some messages")
	})

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQDeleteError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "TASKHAWK-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.SendMessageBatchResultEntry{
			{Id: aws.String(messageID)},
		},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil).
		Once()

	deleteInput := &sqs.DeleteMessageBatchInput{
		Entries: []*sqs.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String(messageID),
				ReceiptHandle: aws.String(receiptHandle),
			},
		},
		QueueUrl: aws.String(dlqURL),
	}

	s.fakeSQS.On("DeleteMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), deleteInput, []request.Option(nil)).
		Return((*sqs.DeleteMessageBatchOutput)(nil), errors.New("no internet")).
		Once().
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to ack messages: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQDeletePartialError() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "TASKHAWK-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.SendMessageBatchResultEntry{
			{Id: aws.String(messageID)},
		},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil).
		Once()

	deleteInput := &sqs.DeleteMessageBatchInput{
		Entries: []*sqs.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String(messageID),
				ReceiptHandle: aws.String(receiptHandle),
			},
		},
		QueueUrl: aws.String(dlqURL),
	}
	deleteOutput := &sqs.DeleteMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{
			{Id: aws.String(messageID)},
		},
		Successful: []*sqs.DeleteMessageBatchResultEntry{},
	}

	s.fakeSQS.On("DeleteMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), deleteInput, []request.Option(nil)).
		Return(deleteOutput, nil).
		Once().
		Run(func(_ mock.Arguments) {
			cancel()
		})

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to ack some messages")
	})

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestRequeueDLQGetQueueError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "TASKHAWK-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	err := s.backend.RequeueDLQ(ctx, taskhawk.PriorityDefault, numMessages, visibilityTimeout)
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
	close(s.messageCh)
	s.Empty(s.messageCh)
}

func (s *BackendTestSuite) TestPublish() {
	ctx := context.Background()

	expectedTopic := s.backend.getSNSTopic(taskhawk.PriorityDefault)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(s.payload)),
		MessageAttributes: attributes,
	}
	output := &sns.PublishOutput{
		MessageId: aws.String("123"),
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return(output, nil)

	messageID, err := s.backend.Publish(ctx, s.payload, s.attributes, taskhawk.PriorityDefault)
	s.NoError(err)
	s.Equal(messageID, "123")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublishInvalidCharacters() {
	ctx := context.Background()

	expectedTopic := s.backend.getSNSTopic(taskhawk.PriorityDefault)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
		"taskhawk_encoding": {
			DataType:    aws.String("String"),
			StringValue: aws.String("base64"),
		},
	}

	invalidPayload := []byte("\x19")

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(base64.StdEncoding.EncodeToString(invalidPayload)),
		MessageAttributes: attributes,
	}
	output := &sns.PublishOutput{
		MessageId: aws.String("123"),
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return(output, nil)

	messageID, err := s.backend.Publish(ctx, invalidPayload, s.attributes, taskhawk.PriorityDefault)
	s.NoError(err)
	s.Equal(messageID, "123")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublishFailure() {
	ctx := context.Background()

	expectedTopic := s.backend.getSNSTopic(taskhawk.PriorityDefault)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(s.payload)),
		MessageAttributes: attributes,
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return((*sns.PublishOutput)(nil), errors.New("failed"))

	_, err := s.backend.Publish(ctx, s.payload, s.attributes, taskhawk.PriorityDefault)
	s.EqualError(err, "Failed to publish message to SNS: failed")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAck() {
	ctx := context.Background()

	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"

	deleteInput := &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: aws.String(receiptHandle),
	}
	deleteOutput := &sqs.DeleteMessageOutput{}

	s.fakeSQS.On("DeleteMessageWithContext", ctx, deleteInput, mock.Anything).
		Return(deleteOutput, nil)

	err := s.backend.AckMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.NoError(err)

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAckError() {
	ctx := context.Background()

	queueName := "TASKHAWK-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"

	deleteInput := &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: aws.String(receiptHandle),
	}

	s.fakeSQS.On("DeleteMessageWithContext", ctx, deleteInput, mock.Anything).
		Return((*sqs.DeleteMessageOutput)(nil), errors.New("failed to ack"))

	err := s.backend.AckMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.EqualError(err, "failed to ack")

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAckGetQueueError() {
	ctx := context.Background()

	queueName := "TASKHAWK-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	receiptHandle := "foobar"

	err := s.backend.AckMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNack() {
	ctx := context.Background()

	receiptHandle := "foobar"

	err := s.backend.NackMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.NoError(err)

	// no calls expected
	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNew() {
	assert.NotNil(s.T(), s.backend)
}

type BackendTestSuite struct {
	suite.Suite
	backend    *Backend
	settings   Settings
	fakeSQS    *fakeSQS
	fakeSNS    *fakeSNS
	payload    []byte
	attributes map[string]string
	messageCh  chan taskhawk.ReceivedMessage
	logger     *fakeLogger
}

func (s *BackendTestSuite) SetupTest() {
	logger := &fakeLogger{}
	settings := Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "DEV-MYAPP",
	}
	getLogger := func(_ context.Context) taskhawk.Logger {
		return logger
	}
	fakeSQS := &fakeSQS{}
	fakeSNS := &fakeSNS{}

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{"foo": "bar"}

	s.backend = NewBackend(settings, getLogger)
	s.backend.sqs = fakeSQS
	s.backend.sns = fakeSNS
	s.settings = settings
	s.fakeSQS = fakeSQS
	s.fakeSNS = fakeSNS
	s.payload = payload
	s.attributes = attributes
	s.messageCh = make(chan taskhawk.ReceivedMessage)
	s.logger = logger
}

func TestBackendTestSuite(t *testing.T) {
	suite.Run(t, &BackendTestSuite{})
}
