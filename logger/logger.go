package logger

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Logger struct {
	infoLogger    *log.Logger
	errorLogger   *log.Logger
	statsLogger   *log.Logger
	unmatchedFile *os.File
	errorFile     *os.File
	statsFile     *os.File
	logLevel      string
}

func NewLogger(logLevel string) (*Logger, error) {
	// Create logs directory if it doesn't exist
	if err := os.MkdirAll("logs", 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}

	// Open log files
	unmatchedFile, err := os.OpenFile("logs/unmatched_cities.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open unmatched cities log: %w", err)
	}

	errorFile, err := os.OpenFile("logs/errors.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		unmatchedFile.Close()
		return nil, fmt.Errorf("failed to open error log: %w", err)
	}

	statsFile, err := os.OpenFile("logs/stats.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		unmatchedFile.Close()
		errorFile.Close()
		return nil, fmt.Errorf("failed to open stats log: %w", err)
	}

	logger := &Logger{
		infoLogger:    log.New(os.Stdout, "[INFO] ", log.LstdFlags),
		errorLogger:   log.New(errorFile, "[ERROR] ", log.LstdFlags),
		statsLogger:   log.New(statsFile, "[STATS] ", log.LstdFlags),
		unmatchedFile: unmatchedFile,
		errorFile:     errorFile,
		statsFile:     statsFile,
		logLevel:      logLevel,
	}

	return logger, nil
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.logLevel == "debug" || l.logLevel == "info" {
		l.infoLogger.Printf(format, args...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.errorLogger.Printf(format, args...)
	if l.logLevel == "debug" || l.logLevel == "info" {
		l.infoLogger.Printf("[ERROR] "+format, args...)
	}
}

func (l *Logger) LogUnmatchedCity(cityID, cityName, countyID, reason string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	line := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\n", timestamp, cityID, cityName, countyID, reason)
	l.unmatchedFile.WriteString(line)
	l.Info("UNMATCHED: %s (%s) in county %s - %s", cityName, cityID, countyID, reason)
}

func (l *Logger) LogStats(stats interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	l.statsLogger.Printf("%s - %+v", timestamp, stats)
}

func (l *Logger) LogReconciliation(oldCityID, newCityID, cityName string, updatedRecords int) {
	l.Info("RECONCILED: %s -> %s (%s) - %d records updated", oldCityID, newCityID, cityName, updatedRecords)
}

func (l *Logger) LogProgress(current, total int, cityName string) {
	percentage := float64(current) / float64(total) * 100
	l.Info("Progress: %d/%d (%.1f%%) - Processing: %s", current, total, percentage, cityName)
}

func (l *Logger) Close() {
	if l.unmatchedFile != nil {
		l.unmatchedFile.Close()
	}
	if l.errorFile != nil {
		l.errorFile.Close()
	}
	if l.statsFile != nil {
		l.statsFile.Close()
	}
}