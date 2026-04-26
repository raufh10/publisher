package pkg

import (
  "fmt"
  "time"

  "github.com/lib/pq"
)

type EventCallback func(payload string)

// NewDatabaseListener initializes and starts listening on a specific channel.
func NewDatabaseListener(dbURL string, channel string) (*pq.Listener, error) {
  reportErr := func(ev pq.ListenerEventType, err error) {
    if err != nil {
      fmt.Printf("[-] Postgres Listener Error (%v): %v\n", ev, err)
    }
  }

  listener := pq.NewListener(dbURL, 10*time.Second, time.Minute, reportErr)

  err := listener.Listen(channel)
  if err != nil {
    return nil, fmt.Errorf("failed to listen on channel %s: %w", channel, err)
  }

  return listener, nil
}

// HandleEvents runs the infinite loop to process notifications and maintain the connection.
func HandleEvents(listener *pq.Listener, callback EventCallback) {
  for {
    select {
    case notification := <-listener.Notify:
      if notification == nil {
        // A nil notification can happen during reconnects/instability
        continue
      }
      callback(notification.Extra)
    case <-time.After(90 * time.Second):
      // Health check to ensure the connection is still alive
      go listener.Ping()
    }
  }
}

// ListenForEvents high-level listen orchestrator.
func ListenForEvents(dbURL string, channel string, callback EventCallback) error {
  listener, err := NewDatabaseListener(dbURL, channel)
  if err != nil {
    return err
  }

  fmt.Printf("[+] Database listener active on channel: %s\n", channel)

  go HandleEvents(listener, callback)

  return nil
}
