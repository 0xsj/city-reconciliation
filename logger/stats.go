package logger

import (
	"fmt"
	"sync"
	"time"
)

type Statistics struct {
	mu                    sync.Mutex
	StartTime             time.Time
	TotalCities           int
	ProcessedCities       int
	SuccessfulMatches     int
	UnmatchedCities       int
	ConsolidatedCities    int
	UpdatedRecords        int
	CreatedCities         int
	Errors                int
	APICallCount          int
	TotalAPITime          time.Duration
	UnmatchedReasons      map[string]int
}

func NewStatistics() *Statistics {
	return &Statistics{
		StartTime:        time.Now(),
		UnmatchedReasons: make(map[string]int),
	}
}

func (s *Statistics) SetTotal(total int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalCities = total
}

func (s *Statistics) IncrementProcessed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ProcessedCities++
}

func (s *Statistics) IncrementMatched() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SuccessfulMatches++
}

func (s *Statistics) IncrementUnmatched(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.UnmatchedCities++
	s.UnmatchedReasons[reason]++
}

func (s *Statistics) IncrementConsolidated() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ConsolidatedCities++
}

func (s *Statistics) IncrementCreated() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CreatedCities++
}

func (s *Statistics) AddUpdatedRecords(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.UpdatedRecords += count
}

func (s *Statistics) IncrementErrors() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Errors++
}

func (s *Statistics) RecordAPICall(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.APICallCount++
	s.TotalAPITime += duration
}

func (s *Statistics) GetSnapshot() StatisticsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	elapsed := time.Since(s.StartTime)
	avgAPITime := time.Duration(0)
	if s.APICallCount > 0 {
		avgAPITime = s.TotalAPITime / time.Duration(s.APICallCount)
	}
	
	reasonsCopy := make(map[string]int)
	for k, v := range s.UnmatchedReasons {
		reasonsCopy[k] = v
	}
	
	return StatisticsSnapshot{
		StartTime:         s.StartTime,
		ElapsedTime:       elapsed,
		TotalCities:       s.TotalCities,
		ProcessedCities:   s.ProcessedCities,
		SuccessfulMatches: s.SuccessfulMatches,
		UnmatchedCities:   s.UnmatchedCities,
		ConsolidatedCities: s.ConsolidatedCities,
		UpdatedRecords:    s.UpdatedRecords,
		CreatedCities:     s.CreatedCities,
		Errors:            s.Errors,
		APICallCount:      s.APICallCount,
		TotalAPITime:      s.TotalAPITime,
		AvgAPITime:        avgAPITime,
		UnmatchedReasons:  reasonsCopy,
	}
}

type StatisticsSnapshot struct {
	StartTime         time.Time
	ElapsedTime       time.Duration
	TotalCities       int
	ProcessedCities   int
	SuccessfulMatches int
	UnmatchedCities   int
	ConsolidatedCities int
	UpdatedRecords    int
	CreatedCities     int
	Errors            int
	APICallCount      int
	TotalAPITime      time.Duration
	AvgAPITime        time.Duration
	UnmatchedReasons  map[string]int
}

func (s StatisticsSnapshot) String() string {
	percentage := 0.0
	if s.TotalCities > 0 {
		percentage = float64(s.ProcessedCities) / float64(s.TotalCities) * 100
	}
	
	result := fmt.Sprintf(`
=== RECONCILIATION STATISTICS ===
Start Time: %s
Elapsed Time: %s
Progress: %d/%d (%.1f%%)
Successful Matches: %d
Unmatched Cities: %d
Consolidated Cities: %d
Created Cities: %d
Updated Records: %d
Errors: %d
API Calls: %d (avg: %s)
`, 
		s.StartTime.Format("2006-01-02 15:04:05"),
		s.ElapsedTime.Round(time.Second),
		s.ProcessedCities,
		s.TotalCities,
		percentage,
		s.SuccessfulMatches,
		s.UnmatchedCities,
		s.ConsolidatedCities,
		s.CreatedCities,
		s.UpdatedRecords,
		s.Errors,
		s.APICallCount,
		s.AvgAPITime.Round(time.Millisecond),
	)
	
	if len(s.UnmatchedReasons) > 0 {
		result += "\nUnmatched Reasons:\n"
		for reason, count := range s.UnmatchedReasons {
			result += fmt.Sprintf("  %s: %d\n", reason, count)
		}
	}
	
	return result
}