/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.
 */

package taskhawk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDispatch(t *testing.T) {
	ctx := context.Background()

	backend := &fakeBackend{}
	hub := NewHub(Config{}, backend)
	taskRef := SendEmailTask{}
	task, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
	}

	expectedMessage := message{
		Headers: map[string]string{},
		ID:      "message-id",
		Input:   input,
		Metadata: metadata{
			Priority:  PriorityDefault,
			Timestamp: jsonTime(time.Now()),
			Version:   CurrentVersion,
		},
		TaskName: "task_test.SendEmailTask",
	}
	require.NoError(t, err)
	expectedAttributes := map[string]string{
		headerTask: "task_test.SendEmailTask",
	}

	backend.On("Publish", ctx, mock.Anything, mock.Anything, PriorityDefault).
		Return("message-id", nil)

	assert.NoError(t, task.Dispatch(ctx, input))

	actualPayload := backend.Calls[0].Arguments.Get(1).([]byte)
	attributes := backend.Calls[0].Arguments.Get(2).(map[string]string)
	actual, err := jsonifier{}.deserialize(hub, actualPayload, attributes)
	require.NoError(t, err)

	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	assert.Equal(t, expectedAttributes, attributes)
	backend.AssertExpectations(t)
}

func TestDispatchWithPriority(t *testing.T) {
	ctx := context.Background()

	backend := &fakeBackend{}
	hub := NewHub(Config{}, backend)
	taskRef := SendEmailTask{}
	task, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
	}

	expectedMessage := message{
		Headers: map[string]string{},
		ID:      "message-id",
		Input:   input,
		Metadata: metadata{
			Priority:  PriorityLow,
			Timestamp: jsonTime(time.Now()),
			Version:   CurrentVersion,
		},
		TaskName: "task_test.SendEmailTask",
	}
	require.NoError(t, err)
	expectedAttributes := map[string]string{
		headerTask: "task_test.SendEmailTask",
	}

	backend.On("Publish", ctx, mock.Anything, mock.Anything, PriorityLow).
		Return("message-id", nil)

	assert.NoError(t, task.DispatchWithPriority(ctx, input, PriorityLow))

	actualPayload := backend.Calls[0].Arguments.Get(1).([]byte)
	attributes := backend.Calls[0].Arguments.Get(2).(map[string]string)
	actual, err := jsonifier{}.deserialize(hub, actualPayload, attributes)
	require.NoError(t, err)

	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	assert.Equal(t, expectedAttributes, attributes)
	backend.AssertExpectations(t)
}

func TestDispatchWithDefaultPriority(t *testing.T) {
	ctx := context.Background()

	backend := &fakeBackend{}
	hub := NewHub(Config{}, backend)
	taskRef := SendEmailTask{}
	task, err := RegisterTaskWithPriority(hub, "task_test.SendEmailTask", taskRef.Run, PriorityHigh)
	require.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
	}

	expectedMessage := message{
		Headers: map[string]string{},
		ID:      "message-id",
		Input:   input,
		Metadata: metadata{
			Priority:  PriorityHigh,
			Timestamp: jsonTime(time.Now()),
			Version:   CurrentVersion,
		},
		TaskName: "task_test.SendEmailTask",
	}
	require.NoError(t, err)
	expectedAttributes := map[string]string{
		headerTask: "task_test.SendEmailTask",
	}

	backend.On("Publish", ctx, mock.Anything, mock.Anything, PriorityHigh).
		Return("message-id", nil)

	assert.NoError(t, task.Dispatch(ctx, input))

	actualPayload := backend.Calls[0].Arguments.Get(1).([]byte)
	attributes := backend.Calls[0].Arguments.Get(2).(map[string]string)
	actual, err := jsonifier{}.deserialize(hub, actualPayload, attributes)
	require.NoError(t, err)

	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	assert.Equal(t, expectedAttributes, attributes)
	backend.AssertExpectations(t)
}

func TestDispatchHeaders(t *testing.T) {
	ctx := context.Background()

	backend := &fakeBackend{}
	hub := NewHub(Config{}, backend)
	taskRef := SendEmailTaskHeaders{}
	task, err := RegisterTask(hub, "task_test.SendEmailTaskHeaders", taskRef.Run)
	require.NoError(t, err)

	input := &SendEmailTaskHeadersInput{
		headers: map[string]string{
			"request_id": "request-id",
		},
	}

	expectedMessage := message{
		Headers: map[string]string{
			"request_id": "request-id",
		},
		ID: "message-id",
		// headers are set during task.call, so it won't be set for this test
		Input: &SendEmailTaskHeadersInput{},
		Metadata: metadata{
			Priority:  PriorityLow,
			Timestamp: jsonTime(time.Now()),
			Version:   CurrentVersion,
		},
		TaskName: "task_test.SendEmailTaskHeaders",
	}
	require.NoError(t, err)
	expectedAttributes := map[string]string{
		"request_id": "request-id",
		headerTask:   "task_test.SendEmailTaskHeaders",
	}

	backend.On("Publish", ctx, mock.Anything, mock.Anything, PriorityLow).
		Return("message-id", nil)

	assert.NoError(t, task.DispatchWithPriority(ctx, input, PriorityLow))

	actualPayload := backend.Calls[0].Arguments.Get(1).([]byte)
	attributes := backend.Calls[0].Arguments.Get(2).(map[string]string)
	actual, err := jsonifier{}.deserialize(hub, actualPayload, attributes)
	require.NoError(t, err)

	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	assert.Equal(t, expectedAttributes, attributes)
	backend.AssertExpectations(t)
}

func TestDispatchSync(t *testing.T) {
	ctx := context.Background()

	backend := &fakeBackend{}
	hub := NewHub(Config{Sync: true}, backend)
	taskRef := SendEmailTask{}
	task, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
	}

	taskRef.On("Run", ctx, input).
		Return(nil)

	assert.NoError(t, task.DispatchWithPriority(ctx, input, PriorityLow))

	backend.AssertExpectations(t)
	taskRef.AssertExpectations(t)
}

func TestRegisterTask(t *testing.T) {
	backend := &fakeBackend{}
	hub := NewHub(Config{Sync: true}, backend)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)
}

func TestRegisterTaskSendEmailTaskDuplicate(t *testing.T) {
	backend := &fakeBackend{}
	hub := NewHub(Config{Sync: true}, backend)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)
	_, err = RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	assert.EqualError(t, err, "task with name 'task_test.SendEmailTask' already registered")
}

func TestRegisterTaskSendEmailTaskNoName(t *testing.T) {
	backend := &fakeBackend{}
	hub := NewHub(Config{Sync: true}, backend)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "", taskRef.Run)
	assert.EqualError(t, err, "task name not set")
}
