package nats

import (
  "log"
  "math/rand"
  "time"
  "github.com/robfig/cron/v3"
)

type Orchestrator struct {
  Client *Client
  Config *EventConfig
  Cron   *cron.Cron
}

func NewOrchestrator(client *Client, cfg *EventConfig) *Orchestrator {
  return &Orchestrator{
    Client: client,
    Config: cfg,
    Cron:   cron.New(),
  }
}

// StartCron initializes the scraper trigger logic
func (o *Orchestrator) StartCron() {
  o.Cron.AddFunc(o.Config.Scraper.Cron, func() {

    delay := time.Duration(rand.Intn(26)+5) * time.Second
    log.Printf("[*] Cron triggered. Waiting %v", delay)
    time.Sleep(delay)

    err := o.Client.PublishEvent(o.Config.Scraper.Subject, o.Config.Scraper.Payload)
    if err != nil {
      log.Printf("[!] Scraper Publish error: %v", err)
    }
  })
  o.Cron.Start()
}

func (o *Orchestrator) GetFlushThreshold() time.Duration {
  d, err := time.ParseDuration(o.Config.Pipeline.FlushInterval)
  if err != nil {
    return 5 * time.Minute
  }
  return d
}
