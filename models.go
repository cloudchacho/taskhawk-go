/*
 * Copyright 2022, Cloudchacho
 * All rights reserved.
 */

package taskhawk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// jsonTime is just a wrapper around time that serializes time to epoch in milliseconds
type jsonTime time.Time

// MarshalJSON changes time to epoch in milliseconds
func (t jsonTime) MarshalJSON() ([]byte, error) {
	epochMs := time.Time(t).UnixNano() / int64(time.Millisecond)
	return []byte(strconv.FormatInt(epochMs, 10)), nil
}

// UnmarshalJSON changes time from epoch in milliseconds or string (RFC3339) to time.Time.
func (t *jsonTime) UnmarshalJSON(b []byte) error {
	// may be a string timestamp
	if len(b) > 0 && b[0] == '"' {
		strTime := ""
		err := json.Unmarshal(b, &strTime)
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal timestamp")
		}
		parsedTime, err := time.Parse(time.RFC3339, strTime)
		if err != nil {
			return errors.Wrap(err, "unable to parse timestamp")
		}
		*t = jsonTime(parsedTime)
	} else {
		epochMs, err := strconv.Atoi(string(b))
		if err != nil {
			return errors.Wrap(err, "unable to unmarshal timestamp")
		}
		duration := time.Duration(epochMs) * time.Millisecond
		epochNS := duration.Nanoseconds()
		*t = jsonTime(time.Unix(0, epochNS))
	}
	return nil
}

// metadata represents the metadata associated with an object
type metadata struct {
	Priority  Priority `json:"priority"`
	Timestamp jsonTime `json:"timestamp"`
	Version   Version  `json:"version"`
}

// message model for taskhawk messages.
type message struct {
	Headers map[string]string `json:"headers"`
	ID      string            `json:"id"`
	// the format for message is standard across all services, so use "kwargs" instead of "input"
	Input    any      `json:"kwargs"`
	Metadata metadata `json:"metadata"`
	TaskName string   `json:"task"`
}

// Version represents the message format version
type Version string

const (
	// Version10 represents the first version of the message format schema
	Version10 Version = "1.0"

	// CurrentVersion represents the current version of the taskhawk message schema
	CurrentVersion = Version10
)

// versions lists all the valid version for a taskhawk message schema
var versions = []Version{Version10}

func (m *message) validateRequired() error {
	if m.ID == "" || m.Metadata.Version == "" || m.Metadata.Timestamp == jsonTime(time.Time{}) ||
		m.Headers == nil || m.TaskName == "" {
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
		return errors.Errorf("invalid version: %s", m.Metadata.Version)
	}
	return nil
}

// validate that message object contains all the right things.
func (m *message) validate() error {
	if err := m.validateRequired(); err != nil {
		return err
	}

	if err := m.validateVersion(); err != nil {
		return err
	}

	return nil
}

// newMessage creates a new Taskhawk message
// If metadata is nil, it will be automatically created
// If the data fails validation, error will be returned.
func newMessage(taskName string, input any, headers map[string]string, id string, priority Priority) (message, error) {

	// TODO: should probably use a sync.Pool here

	me := metadata{
		Priority:  priority,
		Timestamp: jsonTime(time.Now()),
		Version:   CurrentVersion,
	}

	m := message{
		TaskName: taskName,
		Headers:  headers,
		ID:       id,
		Input:    input,
		Metadata: me,
	}
	if err := m.validate(); err != nil {
		return m, err
	}

	return m, nil
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
