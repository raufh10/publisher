package nats

import (
  "log"
  "math/rand"
  "os"
  "sync"
  "time"

  "github.com/robfig/cron/v3"
  "gopkg.in/yaml.v3"
)

type Orchestrator struct {
  Client     *Client
  Config     *EventConfig
  Cron       *cron.Cron
  insertPool []PipelineEvent
  updatePool []PipelineEvent
  poolMutex  sync.Mutex
  lastAdded  time.Time // Corrected: Just the type here
}

func NewOrchestrator(client *Client, cfg *EventConfig) *Orchestrator {
  return &Orchestrator{
    Client:    client,
    Config:    cfg,
    Cron:      cron.New(),
    lastAdded: time.Now(), // Initialize with the value here instead
  }
}

// LoadConfig handles reading the YAML file
func LoadConfig(path string) (*EventConfig, error) {
  cfg := &EventConfig{}
  f, err := os.Open(path)
  if err != nil {
    return nil, err
  }
  defer f.Close()

  decoder := yaml.NewDecoder(f)
  err = decoder.Decode(cfg)
  return cfg, err
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

// CollectPipelineEvent handles the pooling logic
func (o *Orchestrator) CollectPipelineEvent(opType string, payload string) {
  event := PipelineEvent{ID: payload}

  o.poolMutex.Lock()
  defer o.poolMutex.Unlock()

  if opType == "INSERT" {
    o.insertPool = append(o.insertPool, event)
  } else {
    o.updatePool = append(o.updatePool, event)
  }

  o.lastAdded = time.Now()
  log.Printf("[*] Pooled %s ID: %s (Total: %d)", opType, payload, len(o.insertPool)+len(o.updatePool))
}

// StartFlusher runs the background loop to push batches to NATS
func (o *Orchestrator) StartFlusher() {
  ticker := time.NewTicker(1 * time.Minute)
  flushThreshold := o.GetFlushThreshold()
  batchLimit := o.Config.Pipeline.BatchSize

  for range ticker.C {
    o.poolMutex.Lock()

    shouldForceFlush := time.Since(o.lastAdded) >= flushThreshold

    // Flush Insert Pool
    if len(o.insertPool) >= batchLimit || (len(o.insertPool) > 0 && shouldForceFlush) {
      batch := o.insertPool
      o.insertPool = nil
      go o.Client.PublishBatch(o.Config.Pipeline.Subjects["insert"], batch)
      log.Printf("[^] Flushed %d INSERTS", len(batch))
    }

    // Flush Update Pool
    if len(o.updatePool) >= batchLimit || (len(o.updatePool) > 0 && shouldForceFlush) {
      batch := o.updatePool
      o.updatePool = nil
      go o.Client.PublishBatch(o.Config.Pipeline.Subjects["update"], batch)
      log.Printf("[^] Flushed %d UPDATES", len(batch))
    }

    o.poolMutex.Unlock()
  }
}

func (o *Orchestrator) GetFlushThreshold() time.Duration {
  if o.Config.Pipeline.FlushInterval == "" {
    return 5 * time.Minute
  }
  d, err := time.ParseDuration(o.Config.Pipeline.FlushInterval)
  if err != nil {
    return 5 * time.Minute
  }
  return d
}

