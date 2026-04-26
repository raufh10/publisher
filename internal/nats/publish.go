package nats

import (
  "encoding/json"
  "fmt"
)

func (c *Client) PublishEvent(subject string, event any) error {
  if event == nil {
    return fmt.Errorf("cannot publish nil event to subject: %s", subject)
  }

  data, err := json.Marshal(event)
  if err != nil {
    return fmt.Errorf("failed to marshal blind event: %w", err)
  }

  return c.Conn.Publish(subject, data)
}

func (c *Client) PublishBatch(subject string, events any) error {
  data, err := json.Marshal(events)
  if err != nil {
    return fmt.Errorf("failed to marshal batch: %w", err)
  }

  return c.Conn.Publish(subject, data)
}
