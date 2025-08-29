package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/0xsj/city-reconciliation/config"
	"github.com/0xsj/city-reconciliation/database"
	"github.com/0xsj/city-reconciliation/lightbox"
	"github.com/0xsj/city-reconciliation/logger"
	"github.com/0xsj/city-reconciliation/reconciler"
)

func main() {
	// Parse command line flags
	var (
		dryRun    = flag.Bool("dry-run", false, "Run in dry-run mode (no database changes)")
		logLevel  = flag.String("log-level", "", "Log level (debug, info, error)")
		help      = flag.Bool("help", false, "Show help")
	)
	flag.Parse()

	if *help {
		showHelp()
		return
	}

	// Load configuration
	cfg := config.Load()

	// Override config with command line flags
	if *dryRun {
		cfg.DryRun = true
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}

	fmt.Printf("=== City Reconciliation Tool ===\n")
	fmt.Printf("Mode: %s\n", getMode(cfg.DryRun))
	fmt.Printf("Log Level: %s\n", cfg.LogLevel)
	fmt.Printf("Database: %s\n", maskDatabaseURL(cfg.DatabaseURL))
	fmt.Printf("Lightbox API: %s\n", cfg.LightboxURL)
	fmt.Printf("===================================\n\n")

	if err := run(cfg); err != nil {
		log.Fatalf("Application failed: %v", err)
	}
}

func run(cfg *config.Config) error {
	// Initialize logger
	lgr, err := logger.NewLogger(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer lgr.Close()

	lgr.Info("Starting city reconciliation application")

	// Connect to database
	db, err := database.Connect(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	lgr.Info("Connected to database successfully")

	// Initialize Lightbox client
	lightboxClient := lightbox.NewClient(cfg.LightboxURL, cfg.LightboxKey)
	lgr.Info("Initialized Lightbox API client")

	// Create reconciler
	rec := reconciler.NewReconciler(db, lightboxClient, lgr, cfg.DryRun)

	// Run reconciliation
	if err := rec.Run(); err != nil {
		return fmt.Errorf("reconciliation failed: %w", err)
	}

	lgr.Info("City reconciliation completed successfully")
	return nil
}

func showHelp() {
	fmt.Println(`City Reconciliation Tool

This tool reconciles manual city entries with canonical Lightbox city data.

Usage:
  go run main.go [flags]

Flags:
  -dry-run         Run in dry-run mode (no database changes)
  -log-level       Set log level (debug, info, error)
  -help           Show this help message

Environment Variables:
  DATABASE_URL     MySQL database connection string
  LIGHTBOX_URL     Lightbox API base URL
  LIGHTBOX_KEY     Lightbox API key
  DRY_RUN         Default dry-run mode (true/false)
  CONCURRENCY     Number of concurrent API calls
  LOG_LEVEL       Default log level

Examples:
  # Dry run with debug logging
  go run main.go -dry-run -log-level=debug

  # Full run with info logging
  go run main.go -log-level=info

  # Use environment variables only
  go run main.go
`)
}

func getMode(dryRun bool) string {
	if dryRun {
		return "DRY RUN (no changes will be made)"
	}
	return "LIVE (changes will be made to database)"
}

func maskDatabaseURL(url string) string {
	if len(url) < 20 {
		return "***"
	}
	return url[:20] + "***"
}