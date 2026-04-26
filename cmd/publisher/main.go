package main

import (
  "fmt"
  "log"
  "os"

  "github.com/joho/godotenv"
  natsPkg "publisher/internal/nats"
  _ "publisher/internal/pg"
)

func main() {
  _ = godotenv.Load()

  // 1. Setup Connections
  natsAddr := os.Getenv("NATS_URL")
  if natsAddr == "" {
    natsAddr = "nats://127.0.0.1:4222"
  }
  dbURL := os.Getenv("DATABASE_URL")
  _ = dbURL

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

  // 3. Initialize Orchestrator
  orch := natsPkg.NewOrchestrator(client, cfg)
  fmt.Printf("[+] Publisher service online. NATS: %s\n", natsAddr)

  // 4. Start Scraper Cron (ACTIVE)
  orch.StartCron()

  // --- PIPELINE INACTIVE SECTION (Kept for future use) ---
  /*
  if dbURL != "" {
    // Start background batching logic
    go orch.StartFlusher()
  }
  */
  // ----------------------------------------------------

  // Keep the process alive
  select {}
}
