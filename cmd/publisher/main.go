package main

import (
  "fmt"
  "log"
  "os"
  "sync"
  "time"

  "github.com/joho/godotenv"
  natsPkg "publisher/internal/nats"
  pgPkg "publisher/internal/pg"
)

var (
  insertPool []natsPkg.PipelineEvent
  updatePool []natsPkg.PipelineEvent
  poolMutex  sync.Mutex
  lastAdded  = time.Now()
)

func main() {
  _ = godotenv.Load()

  // 1. Setup Connections
  natsAddr := os.Getenv("NATS_URL")
  if natsAddr == "" {
    natsAddr = "nats://127.0.0.1:4222"
  }
  dbURL := os.Getenv("DATABASE_URL")

  client, err := natsPkg.NewClient(natsAddr)
  if err != nil {
    log.Fatalf("[-] NATS connection error: %v", err)
  }
  defer client.Close()

  // 2. Load Orchestration Config (YAML)
  cfg, err := natsPkg.LoadConfig("events.yaml")
  if err != nil {
    log.Fatalf("[-] Config error: %v", err)
  }

  orch := natsPkg.NewOrchestrator(client, cfg)
  fmt.Printf("[+] Publisher service online. NATS: %s\n", natsAddr)

  // 3. Start Scraper Cron via Orchestrator
  orch.StartCron()

  // 4. Start the Background Flusher
  go startFlusher(orch)

  // 5. Listen for DB Events
  // Note: Using subjects defined in YAML via the Orchestrator/Config
  err = pgPkg.ListenForEvents(dbURL, cfg.Pipeline.Subjects["insert"], func(payload string) {
    collectEvent("INSERT", payload)
  })
  if err != nil {
    log.Printf("[-] DB Insert Listener error: %v", err)
  }

  err = pgPkg.ListenForEvents(dbURL, cfg.Pipeline.Subjects["update"], func(payload string) {
    collectEvent("UPDATE", payload)
  })
  if err != nil {
    log.Printf("[-] DB Update Listener error: %v", err)
  }

  // Keep the process alive
  select {}
}

func collectEvent(opType string, payload string) {
  event := natsPkg.PipelineEvent{
    ID: payload,
  }

  poolMutex.Lock()
  defer poolMutex.Unlock()

  if opType == "INSERT" {
    insertPool = append(insertPool, event)
  } else {
    updatePool = append(updatePool, event)
  }

  lastAdded = time.Now()
  log.Printf("[*] Pooled %s ID: %s (Total: %d)", opType, payload, len(insertPool)+len(updatePool))
}

func startFlusher(orch *natsPkg.Orchestrator) {
  ticker := time.NewTicker(1 * time.Minute)
  flushThreshold := orch.GetFlushThreshold()
  batchLimit := orch.Config.Pipeline.BatchSize

  for range ticker.C {
    poolMutex.Lock()
    
    shouldForceFlush := time.Since(lastAdded) >= flushThreshold

    // Flush Insert Pool
    if len(insertPool) >= batchLimit || (len(insertPool) > 0 && shouldForceFlush) {
      batch := insertPool
      insertPool = nil
      go orch.Client.PublishBatch(orch.Config.Pipeline.Subjects["insert"], batch)
      log.Printf("[^] Flushed %d INSERTS", len(batch))
    }

    // Flush Update Pool
    if len(updatePool) >= batchLimit || (len(updatePool) > 0 && shouldForceFlush) {
      batch := updatePool
      updatePool = nil
      go orch.Client.PublishBatch(orch.Config.Pipeline.Subjects["update"], batch)
      log.Printf("[^] Flushed %d UPDATES", len(batch))
    }

    poolMutex.Unlock()
  }
}

