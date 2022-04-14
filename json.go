package taskhawk

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type jsonifier struct{}

const (
	headerTask = "taskhawk_task"
)

func (j jsonifier) deserialize(h *Hub, messagePayload []byte, attributes map[string]string) (message, error) {
	var m message
	var ok bool
	m.TaskName, ok = attributes[headerTask]
	if !ok {
		return m, errors.Errorf("invalid attributes: %s not found", headerTask)
	}
	task, err := h.getTask(m.TaskName)
	if err != nil {
		return m, fmt.Errorf("unable to deserialize: %w", err)
	}
	m.Input = task.newInput()
	err = json.Unmarshal(messagePayload, &m)
	if err != nil {
		return m, fmt.Errorf("unable to deserialize: %w", err)
	}
	return m, nil
}

func (j jsonifier) serialize(m message) ([]byte, map[string]string, error) {
	payload, err := json.Marshal(m)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to serialize: %w", err)
	}
	headers := map[string]string{
		headerTask: m.TaskName,
	}
	for k, v := range m.Headers {
		if strings.HasPrefix(k, "taskhawk_") {
			return nil, nil, fmt.Errorf("invalid header key: '%s' - can't begin with reserved namespace 'taskhawk_'", k)
		}
		headers[k] = v
	}
	return payload, headers, nil
}
