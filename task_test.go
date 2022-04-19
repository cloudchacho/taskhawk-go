/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.
 */

package taskhawk

import (
	"context"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type SendEmailTask struct {
	mock.Mock
}

func (t *SendEmailTask) Run(ctx context.Context, input *SendEmailTaskInput) error {
	args := t.Called(ctx, input)
	return args.Error(0)
}

type SendEmailTaskInput struct {
	To   string    `json:"to"`
	From string    `json:"from"`
	At   time.Time `json:"time"`
}

func TestCall(t *testing.T) {
	ctx := context.Background()

	taskRef := &SendEmailTask{}

	hub := NewHub(Config{}, nil)
	_, err := RegisterTask(hub, "main.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	fetchedTask, err := hub.getTask("main.SendEmailTask")
	require.NoError(t, err)

	sendTime := time.Now()

	m := message{
		Headers: map[string]string{"request_id": uuid.NewV4().String()},
		ID:      "message-id",
		Input: &SendEmailTaskInput{
			To:   "mail@example.com",
			From: "spam@example.com",
			At:   sendTime,
		},
		Metadata: metadata{
			Priority:  PriorityHigh,
			Timestamp: jsonTime(time.Now()),
			Version:   CurrentVersion,
		},
	}

	provider := struct{}{}

	expectedInput := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "spam@example.com",
		At:   sendTime,
	}
	taskRef.On("Run", ctx, expectedInput).Return(nil)

	err = fetchedTask.call(ctx, m, provider)
	require.NoError(t, err)

	taskRef.AssertExpectations(t)
}

type SendEmailTaskHeaders struct {
	mock.Mock
}

func (t *SendEmailTaskHeaders) Run(ctx context.Context, input *SendEmailTaskHeadersInput) error {
	args := t.Called(ctx, input)
	return args.Error(0)
}

type SendEmailTaskHeadersInput struct {
	headers map[string]string
}

func (s *SendEmailTaskHeadersInput) GetHeaders() map[string]string {
	return s.headers
}

func (s *SendEmailTaskHeadersInput) SetHeaders(headers map[string]string) {
	s.headers = headers
}

func TestCallHeaders(t *testing.T) {
	ctx := context.Background()

	taskRef := &SendEmailTaskHeaders{}

	hub := NewHub(Config{}, nil)
	_, err := RegisterTask(hub, "main.SendEmailTaskHeaders", taskRef.Run)
	require.NoError(t, err)

	fetchedTask, err := hub.getTask("main.SendEmailTaskHeaders")
	require.NoError(t, err)

	requestID := uuid.NewV4().String()

	m := message{
		Headers: map[string]string{"request_id": requestID},
		ID:      "message-id",
		Input:   &SendEmailTaskHeadersInput{},
		Metadata: metadata{
			Priority:  PriorityHigh,
			Timestamp: jsonTime(time.Now()),
			Version:   CurrentVersion,
		},
	}

	providerHeaders := struct{}{}

	expectedInput := &SendEmailTaskHeadersInput{}
	expectedInput.SetHeaders(map[string]string{"request_id": requestID})
	taskRef.On("Run", ctx, expectedInput).Return(nil)

	err = fetchedTask.call(ctx, m, providerHeaders)
	require.NoError(t, err)

	taskRef.AssertExpectations(t)
}

type SendEmailTaskMetadata struct {
	mock.Mock
}

func (t *SendEmailTaskMetadata) Run(ctx context.Context, input *SendEmailTaskMetadataInput) error {
	args := t.Called(ctx, input)
	return args.Error(0)
}

type SendEmailTaskMetadataInput struct {
	id               string
	priority         Priority
	providerMetadata any
	timestamp        time.Time
	version          Version
}

func (s *SendEmailTaskMetadataInput) SetPriority(priority Priority) {
	s.priority = priority
}

func (s *SendEmailTaskMetadataInput) SetProviderMetadata(a any) {
	s.providerMetadata = a
}

func (s *SendEmailTaskMetadataInput) SetTimestamp(t time.Time) {
	s.timestamp = t
}

func (s *SendEmailTaskMetadataInput) SetVersion(version Version) {
	s.version = version
}

func (s *SendEmailTaskMetadataInput) SetID(id string) {
	s.id = id
}

func TestCallMetadata(t *testing.T) {
	ctx := context.Background()

	taskRef := &SendEmailTaskMetadata{}

	hub := NewHub(Config{}, nil)
	_, err := RegisterTask(hub, "main.SendEmailTaskMetadata", taskRef.Run)
	require.NoError(t, err)

	fetchedTask, err := hub.getTask("main.SendEmailTaskMetadata")
	require.NoError(t, err)

	m := message{
		Headers: map[string]string{"request_id": uuid.NewV4().String()},
		ID:      "message-id",
		Input:   &SendEmailTaskMetadataInput{},
		Metadata: metadata{
			Priority:  PriorityHigh,
			Timestamp: jsonTime(time.Now()),
			Version:   CurrentVersion,
		},
	}

	providerMetadata := struct{}{}

	expectedInput := &SendEmailTaskMetadataInput{}
	expectedInput.SetID(m.ID)
	expectedInput.SetPriority(PriorityHigh)
	expectedInput.SetProviderMetadata(providerMetadata)
	expectedInput.SetTimestamp(time.Time(m.Metadata.Timestamp))
	expectedInput.SetVersion(m.Metadata.Version)
	taskRef.On("Run", ctx, expectedInput).Return(nil)

	err = fetchedTask.call(ctx, m, providerMetadata)
	require.NoError(t, err)

	taskRef.AssertExpectations(t)
}
