package taskhawk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeserialize(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"To":"mail@example.com",` +
		`"From":"mail@spammer.com"},"metadata":{"timestamp":1521493587123,"version":"1.0"},` +
		`"task":"task_test.SendEmailTask"}`
	attributes := map[string]string{
		headerTask: "task_test.SendEmailTask",
	}
	m, err := jsonifier{}.deserialize(hub, []byte(jsonStr), attributes)
	assert.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
	}
	expected := getValidMessage(t, hub, input)
	assert.Equal(t, expected, m)
}

func TestDeserialize_ErrorNoTaskAttribute(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"To":"mail@example.com",` +
		`"From":"mail@spammer.com"},"metadata":{"timestamp":1521493587123,"version":"1.0"},` +
		`"task":"task_test.SendEmailTask"}`
	attributes := map[string]string{}
	_, err = jsonifier{}.deserialize(hub, []byte(jsonStr), attributes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid attributes")
}

func TestDeserialize_ErrorNoTask(t *testing.T) {
	hub := NewHub(Config{}, nil)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"To":"mail@example.com",` +
		`"From":"mail@spammer.com"},"metadata":{"timestamp":1521493587123,"version":"1.0"},` +
		`"task":"task_test.SendEmailTask"}`
	attributes := map[string]string{
		headerTask: "task_test.SendEmailTask",
	}
	_, err := jsonifier{}.deserialize(hub, []byte(jsonStr), attributes)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrTaskNotFound)
}

func TestDeserialize_ErrorInvalidInput(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"To":"mail@example.com",` +
		`"From":1},"metadata":{"timestamp":1521493587123,"version":"1.0"},` +
		`"task":"task_test.SendEmailTask"}`
	attributes := map[string]string{
		headerTask: "task_test.SendEmailTask",
	}
	_, err = jsonifier{}.deserialize(hub, []byte(jsonStr), attributes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "json: cannot unmarshal")
}

func TestSerialize(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"to":"mail@example.com",` +
		`"from":"mail@spammer.com","time":"2022-04-18T17:36:00Z"},"metadata":{"priority":"default",` +
		`"timestamp":1521493587123,"version":"1.0"},"task":"task_test.SendEmailTask"}`
	attributes := map[string]string{
		headerTask:   "task_test.SendEmailTask",
		"request_id": "request-id",
	}
	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
		At:   time.UnixMilli(1650303360000).In(time.UTC),
	}
	m := getValidMessage(t, hub, input)
	m.Headers["request_id"] = "request-id"

	payload, actualAttributes, err := jsonifier{}.serialize(m)
	assert.NoError(t, err)
	assert.Equal(t, jsonStr, string(payload))
	assert.Equal(t, attributes, actualAttributes)
}

func TestSerialize_ErrorMarshaling(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	input := &struct {
		// unmarshalable type
		Ch chan int
	}{make(chan int)}
	m := getValidMessage(t, hub, input)
	m.Headers["request_id"] = "request-id"

	_, _, err = jsonifier{}.serialize(m)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "json: unsupported type")
}

func TestSerialize_InvalidHeaders(t *testing.T) {
	hub := NewHub(Config{}, nil)
	taskRef := SendEmailTask{}
	_, err := RegisterTask(hub, "task_test.SendEmailTask", taskRef.Run)
	require.NoError(t, err)

	input := &SendEmailTaskInput{
		To:   "mail@example.com",
		From: "mail@spammer.com",
		At:   time.UnixMilli(1650303360000),
	}
	m := getValidMessage(t, hub, input)
	m.Headers["taskhawk_foobar"] = "something"

	_, _, err = jsonifier{}.serialize(m)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid header key")
}
