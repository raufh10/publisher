package nats

import "encoding/json"

// EventConfig is the validation structure for the YAML
type EventConfig struct {
  Scraper struct {
    Cron    string         `yaml:"cron"`
    Subject string         `yaml:"subject"`
    Payload json.RawMessage `yaml:"payload"`
  } `yaml:"scraper"`

  Pipeline struct {
    BatchSize     int               `yaml:"batch_size"`
    FlushInterval string            `yaml:"flush_interval"`
    Subjects      map[string]string `yaml:"subjects"`
  } `yaml:"pipeline"`
}

type PipelineEvent struct {
  ID string `json:"id"`
}
