package config

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL  string
	LightboxURL  string
	LightboxKey  string
	DryRun       bool
	Concurrency  int
	LogLevel     string
}

func Load() *Config {
	// Load .env file if it exists
	loadEnvFile()

	concurrency, err := strconv.Atoi(getEnvOrDefault("CONCURRENCY", "10"))
	if err != nil {
		concurrency = 10
	}

	dryRun, err := strconv.ParseBool(getEnvOrDefault("DRY_RUN", "true"))
	if err != nil {
		dryRun = true
	}

	config := &Config{
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		LightboxURL:  os.Getenv("LIGHTBOX_URL"),
		LightboxKey:  os.Getenv("LIGHTBOX_KEY"),
		DryRun:       dryRun,
		Concurrency:  concurrency,
		LogLevel:     getEnvOrDefault("LOG_LEVEL", "info"),
	}

	if config.DatabaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}
	if config.LightboxURL == "" {
		log.Fatal("LIGHTBOX_URL environment variable is required")
	}
	if config.LightboxKey == "" {
		log.Fatal("LIGHTBOX_KEY environment variable is required")
	}

	return config
}

func loadEnvFile() {
	file, err := os.Open(".env")
	if err != nil {
		return // .env file doesn't exist, that's OK
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			os.Setenv(key, value)
		}
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}