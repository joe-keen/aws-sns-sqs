package cncf

import (
	"time"

	"github.com/twinj/uuid"
)

// Event is a struct that contains the CNCF json event format
// https://github.com/cloudevents/spec/blob/v0.1/json-format.md
type Event struct {
	EventType          string            `json:"eventType"`
	EventTypeVersion   string            `json:"eventTypeVersion,omitempty"`
	CloudEventsVersion string            `json:"cloudEventsVersion"`
	Source             string            `json:"source"`
	EventID            string            `json:"eventID"`
	EventTime          string            `json:"eventTime"`
	ContentType        string            `json:"contentType,omitempty"`
	Extensions         map[string]string `json:"extensions,omitempty"`
	Data               map[string]string `json:"data,omitempty"`
}

// New creates a new cncf event structure
func New(eventType, source string, data map[string]string) Event {
	return Event{
		EventType:          eventType,
		CloudEventsVersion: version(),
		Source:             source,
		EventID:            id(),
		EventTime:          time.Now().Format(time.RFC3339),
		Data:               data,
	}
}

func id() string {
	return uuid.NewV4().String()
}

func version() string {
	return "0.1"
}
