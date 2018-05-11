/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// JSONTime is just a wrapper around time that serializes time to epoch in milliseconds
type JSONTime time.Time

// MarshalJSON changes time to epoch in milliseconds
func (t JSONTime) MarshalJSON() ([]byte, error) {
	epochMs := time.Time(t).UnixNano() / int64(time.Millisecond)
	return []byte(strconv.FormatInt(epochMs, 10)), nil
}

// UnmarshalJSON changes time from epoch in milliseconds or string (RFC3339) to time.Time.
func (t *JSONTime) UnmarshalJSON(b []byte) error {
	// may be a string timestamp
	if len(b) > 0 && b[0] == '"' {
		strTime := ""
		err := json.Unmarshal(b, &strTime)
		if err != nil {
			return err
		}
		parsedTime, err := time.Parse(time.RFC3339, strTime)
		if err != nil {
			return err
		}
		*t = JSONTime(parsedTime)
	} else {
		epochMs, err := strconv.Atoi(string(b))
		if err != nil {
			return err
		}
		duration := time.Duration(epochMs) * time.Millisecond
		epochNS := duration.Nanoseconds()
		*t = JSONTime(time.Unix(0, epochNS))
	}
	return nil
}

// metadata represents the metadata associated with an object
type metadata struct {
	Priority  Priority `json:"priority"`
	Timestamp JSONTime `json:"timestamp"`
	Version   Version  `json:"version"`
}

// message model for taskhawk messages.
type message struct {
	Headers map[string]string `json:"headers"`
	ID      string            `json:"id"`
	// the format for message is standard across all services, so use "kwargs" instead of "input"
	Input    interface{} `json:"kwargs"`
	Metadata *metadata   `json:"metadata"`
	Task     *taskDef    `json:"task"`
}

// Version represents the message format version
type Version string

const (
	// Version1_0 represents the first version of the message format schema
	Version1_0 Version = "1.0"

	// CurrentVersion represents the current version of the taskhawk message schema
	CurrentVersion = Version1_0
)

// versions lists all the valid version for a taskhawk message schema
var versions = []Version{Version1_0}

// UnmarshalJSON deserializes JSON blob into a message
func (m *message) UnmarshalJSON(b []byte) error {
	type MessageClone message
	if err := json.Unmarshal(b, (*MessageClone)(m)); err != nil {
		return err
	}

	inputContainer := struct {
		// delay de-serializing input until we know task input type
		Input json.RawMessage `json:"kwargs"`
	}{}
	if err := json.Unmarshal(b, &inputContainer); err != nil {
		return err
	}

	if m.Task == nil {
		// reset input if we couldn't determine it's type
		m.Input = nil
		return nil
	}

	input := m.Task.NewInput()
	if input == nil {
		// task doesn't accept input
		m.Input = nil
		return nil
	}

	if err := json.Unmarshal(inputContainer.Input, input); err != nil {
		return err
	}

	m.Input = input
	return nil
}

func (m *message) validateRequired() error {
	if m.ID == "" || m.Metadata == nil || m.Metadata.Version == "" || m.Metadata.Timestamp == JSONTime(time.Time{}) ||
		m.Headers == nil || m.Task == nil {

		return errors.New("missing required data")
	}
	return nil
}

func (m *message) validateVersion() error {
	validVersion := false
	for _, v := range versions {
		if v == m.Metadata.Version {
			validVersion = true
			break
		}
	}
	if !validVersion {
		return fmt.Errorf("invalid version: %s", m.Metadata.Version)
	}
	return nil
}

// validate validates that message object contains all the right things.
func (m *message) validate() error {
	if err := m.validateRequired(); err != nil {
		return err
	}

	if err := m.validateVersion(); err != nil {
		return err
	}

	_, ok := taskRegistry[m.Task.Name()]
	if !ok {
		return fmt.Errorf("invalid task, not registered: %s", m.Task.Name())
	}

	return nil
}

// CallTask calls the underlying task with given args and kwargs
func (m *message) callTask(ctx context.Context, receipt string) error {
	return m.Task.call(ctx, m, receipt)
}

// NewMessage creates a new Taskhawk message
// If metadata is nil, it will be automatically created
// If the data fails validation, error will be returned.
func newMessage(input interface{}, headers map[string]string, id string, priority Priority, task *taskDef) (
	*message, error) {

	// TODO: should probably use a sync.Pool here

	metadata := &metadata{
		Priority:  priority,
		Timestamp: JSONTime(time.Now()),
		Version:   CurrentVersion,
	}

	message := message{
		Headers:  headers,
		ID:       id,
		Input:    input,
		Metadata: metadata,
		Task:     task,
	}
	if err := message.validate(); err != nil {
		return nil, err
	}

	return &message, nil
}

// Priority of a task. This may be used to differentiate batch jobs from other tasks for example.
//
// High and low priority queues provide independent scaling knobs for your use-case.
type Priority int

// Priority for a task
const (
	// PriorityDefault is the default priority of a task if nothing is specified. In most cases,
	// using just the default queue should work fine.
	PriorityDefault Priority = iota // Keep default first so empty values automatically default
	PriorityLow
	PriorityHigh
	// PriorityBulk queue will typically have different monitoring, and may be used for bulk jobs,
	// such as sending push notifications to all users. This allows you to effectively
	// throttle the tasks.
	PriorityBulk
)

// MarshalJSON changes Priority to a JSON string
func (p Priority) MarshalJSON() ([]byte, error) {
	switch p {
	case PriorityDefault:
		return []byte(`"default"`), nil
	case PriorityHigh:
		return []byte(`"high"`), nil
	case PriorityLow:
		return []byte(`"low"`), nil
	case PriorityBulk:
		return []byte(`"bulk"`), nil
	default:
		panic(fmt.Sprintf("unhandled priority %v", p))
	}
}

// UnmarshalJSON changes priority from a JSON string to Priority
func (p *Priority) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case `"default"`:
		*p = PriorityDefault
	case `"high"`:
		*p = PriorityHigh
	case `"low"`:
		*p = PriorityLow
	case `"bulk"`:
		*p = PriorityBulk
	default:
		return errors.New("unknown priority")
	}
	return nil
}
