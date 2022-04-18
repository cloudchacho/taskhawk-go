/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.
 */

package taskhawk

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONTime_ToJson(t *testing.T) {
	epochMS := 1521493587123
	ts := jsonTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	tsAsJSON, err := json.Marshal(ts)
	assert.NoError(t, err)
	epochStr := fmt.Sprintf("%d", epochMS)
	assert.Equal(t, epochStr, string(tsAsJSON))
}

func TestJSONTime_FromJson(t *testing.T) {
	epochMS := 1521493587123
	ts := jsonTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	epochStr := fmt.Sprintf("%d", epochMS)
	ts2 := new(jsonTime)
	assert.NoError(t, json.Unmarshal([]byte(epochStr), &ts2))
	assert.Equal(t, time.Time(ts).Unix(), time.Time(*ts2).Unix())
}

func TestJSONTime_FromJson_String(t *testing.T) {
	epochMS := 1524763993123
	epochStr := `"2018-04-26T17:33:13.123Z"`
	ts := jsonTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	ts2 := new(jsonTime)
	assert.NoError(t, json.Unmarshal([]byte(epochStr), &ts2))
	assert.Equal(t, time.Time(ts).Unix(), time.Time(*ts2).Unix())
}

func TestJSONTime_FromJson_InvalidString(t *testing.T) {
	epochStr := `"2016"`
	ts2 := new(jsonTime)
	assert.Error(t, json.Unmarshal([]byte(epochStr), &ts2))
}

func TestJSONTime_FromJson_InvalidValue(t *testing.T) {
	epochStr := ``
	ts2 := new(jsonTime)
	assert.Error(t, json.Unmarshal([]byte(epochStr), &ts2))
}

func getValidMessageByTaskName[T any](t *testing.T, taskName string, hub *Hub, input *T) message {
	epochMS := 1521493587123
	ts := jsonTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	_, err := hub.getTask(taskName)
	require.NoError(t, err)
	return message{
		Headers: map[string]string{"request_id": "request-id"},
		ID:      "message-id",
		Input:   input,
		Metadata: metadata{
			Priority:  PriorityDefault,
			Timestamp: ts,
			Version:   CurrentVersion,
		},
		TaskName: taskName,
	}
}

func getValidMessage[T any](t *testing.T, hub *Hub, input *T) message {
	return getValidMessageByTaskName(t, "task_test.SendEmailTask", hub, input)
}

func TestMessageToJson(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	at := time.UnixMilli(1650303360000).In(time.UTC)
	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
		At:   at,
	}
	m := getValidMessage(t, hub, input)
	expected := `{"headers":{"request_id":"request-id"},"id":"message-id",` +
		`"kwargs":{"to":"mail@example.com","from":"mail@spammer.com","time":"2022-04-18T17:36:00Z"},` +
		`"metadata":{"priority":"default","timestamp":1521493587123,"version":"1.0"},"task":"task_test.SendEmailTask"}`
	actual, err := json.Marshal(m)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(actual))
}

func TestMessageToJsonMinimal(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	at := time.UnixMilli(1650303360000).In(time.UTC)
	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
		At:   at,
	}
	m := getValidMessage(t, hub, input)
	m.Headers = map[string]string{}
	expected := `{"headers":{},"id":"message-id",` +
		`"kwargs":{"to":"mail@example.com","from":"mail@spammer.com","time":"2022-04-18T17:36:00Z"},` +
		`"metadata":{"priority":"default","timestamp":1521493587123,"version":"1.0"},"task":"task_test.SendEmailTask"}`
	actual, err := json.Marshal(m)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(actual))
}

func TestMessageFromJson(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
	}
	expected := getValidMessage(t, hub, input)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"To":"mail@example.com",` +
		`"From":"mail@spammer.com"},"metadata":{"timestamp":1521493587123,"version":"1.0"},` +
		`"task":"task_test.SendEmailTask"}`

	actual := message{Input: &SendEmailTaskInput{}}
	err = json.Unmarshal([]byte(jsonStr), &actual)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestMessage_Validate(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	m := getValidMessage(t, hub, (*SendEmailTaskInput)(nil))
	assert.NoError(t, m.validate())
}

func TestMessage_ValidateFail_NoID(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	m := getValidMessage(t, hub, (*SendEmailTaskInput)(nil))
	m.ID = ""
	assert.EqualError(t, m.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoVersion(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	m := getValidMessage(t, hub, (*SendEmailTaskInput)(nil))
	m.Metadata.Version = ""
	assert.EqualError(t, m.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoTimestamp(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	m := getValidMessage(t, hub, (*SendEmailTaskInput)(nil))
	m.Metadata.Timestamp = jsonTime(time.Time{})
	assert.EqualError(t, m.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoHeaders(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	m := getValidMessage(t, hub, (*SendEmailTaskInput)(nil))
	m.Headers = nil
	assert.EqualError(t, m.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoTask(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	m := getValidMessage(t, hub, (*SendEmailTaskInput)(nil))
	m.TaskName = ""
	assert.EqualError(t, m.validate(), "missing required data")
}

func copyMap(d map[string]string) map[string]string {
	newMap := make(map[string]string)
	for k, v := range d {
		newMap[k] = v
	}
	return newMap
}

func TestMessage_newMessage(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
	}
	m := getValidMessage(t, hub, input)
	// copy objects that are mutable, or passed by ref
	headers := copyMap(m.Headers)
	actual, err := newMessage(m.TaskName, input, headers, m.ID, PriorityHigh)
	assert.NoError(t, err)

	m.Metadata = metadata{
		Priority:  PriorityHigh,
		Timestamp: actual.Metadata.Timestamp,
		Version:   CurrentVersion,
	}

	assert.Equal(t, m, actual)
}

func TestMessage_newMessage_Validates(t *testing.T) {
	_, err := newMessage("task_test.SendEmailTask", nil, nil, "", PriorityDefault)
	assert.EqualError(t, err, "missing required data")
}
