package reconciler

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/0xsj/city-reconciliation/database"
	"github.com/0xsj/city-reconciliation/lightbox"
	"github.com/0xsj/city-reconciliation/logger"
)

// type Reconciler struct {
// 	db             *database.DB
// 	lightboxClient *lightbox.Client
// 	logger         *logger.Logger
// 	stats          *logger.Statistics
// 	dryRun         bool
// }

// func NewReconciler(db *database.DB, lightboxClient *lightbox.Client, lgr *logger.Logger, dryRun bool) *Reconciler {
// 	return &Reconciler{
// 		db:             db,
// 		lightboxClient: lightboxClient,
// 		logger:         lgr,
// 		stats:          logger.NewStatistics(),
// 		dryRun:         dryRun,
// 	}
// }

// func (r *Reconciler) ReconcileCity(city database.City) error {
// 	r.logger.Info("Processing city: %s (%s) in county %s", city.Name, city.ID, city.CountyID)

// 	startTime := time.Now()
// 	defer func() {
// 		r.stats.RecordAPICall(time.Since(startTime))
// 		r.stats.IncrementProcessed()
// 	}()

// 	// Normalize the county ID
// 	normalizedCountyID := r.lightboxClient.NormalizeCountyFIPS(city.CountyID)

// 	// Try to find a match in Lightbox
// 	matchResult, err := r.lightboxClient.FindBestMatch(city.Name, normalizedCountyID)
// 	if err != nil {
// 		r.logger.Error("Failed to search Lightbox for city %s: %v", city.Name, err)
// 		r.stats.IncrementErrors()
// 		return fmt.Errorf("lightbox search failed: %w", err)
// 	}

// 	if !matchResult.Found {
// 		r.logger.LogUnmatchedCity(city.ID, city.Name, city.CountyID, matchResult.Error)
// 		r.stats.IncrementUnmatched(matchResult.Error)
// 		return nil
// 	}

// 	r.stats.IncrementMatched()
// 	lightboxCity := matchResult.LightboxCity

// 	r.logger.Info("Found match: %s -> Lightbox ID %s (FIPS: %s, Match: %s, Confidence: %.2f)",
// 		city.Name, lightboxCity.ID, lightboxCity.FIPS, matchResult.MatchType, matchResult.Confidence)

// 	// Check if canonical city already exists
// 	canonicalCity, err := r.db.FindCanonicalCity(lightboxCity.Location.Locality, lightboxCity.FIPS)
// 	if err != nil {
// 		r.logger.Error("Failed to find canonical city: %v", err)
// 		r.stats.IncrementErrors()
// 		return fmt.Errorf("database error: %w", err)
// 	}

// 	canonicalID := ""
// 	if canonicalCity != nil {
// 		// Use existing canonical city
// 		canonicalID = canonicalCity.ID
// 		r.logger.Info("Using existing canonical city: %s", canonicalID)
// 	} else {
// 		// Create new canonical city
// 		canonicalID = lightboxCity.ID
// 		if !r.dryRun {
// 			err = r.db.CreateCanonicalCity(lightboxCity.ID, lightboxCity.Location.Locality, lightboxCity.FIPS)
// 			if err != nil {
// 				r.logger.Error("Failed to create canonical city: %v", err)
// 				r.stats.IncrementErrors()
// 				return fmt.Errorf("failed to create canonical city: %w", err)
// 			}
// 		}
// 		r.stats.IncrementCreated()
// 		r.logger.Info("Created new canonical city: %s", canonicalID)
// 	}

// 	// If the manual city is the same as canonical, just update its properties
// 	if city.ID == canonicalID {
// 		r.logger.Info("City %s is already canonical, skipping consolidation", city.ID)
// 		return nil
// 	}

// 	// Get affected record counts
// 	affectedCounts, err := r.db.GetAffectedRecordCounts(city.ID)
// 	if err != nil {
// 		r.logger.Error("Failed to get affected record counts: %v", err)
// 		r.stats.IncrementErrors()
// 		return fmt.Errorf("failed to get affected counts: %w", err)
// 	}

// 	r.logger.Info("Records to update for city %s: %+v", city.ID, affectedCounts)

// 	if !r.dryRun {
// 		// Update all references to point to canonical city
// 		if err := r.db.UpdateCityReferences(city.ID, canonicalID); err != nil {
// 			r.logger.Error("Failed to update city references: %v", err)
// 			r.stats.IncrementErrors()
// 			return fmt.Errorf("failed to update references: %w", err)
// 		}

// 		// Delete the manual city
// 		if err := r.db.DeleteCity(city.ID); err != nil {
// 			r.logger.Error("Failed to delete manual city: %v", err)
// 			r.stats.IncrementErrors()
// 			return fmt.Errorf("failed to delete city: %w", err)
// 		}
// 	}

// 	r.stats.IncrementConsolidated()
// 	r.stats.AddUpdatedRecords(affectedCounts.Total)
// 	r.logger.LogReconciliation(city.ID, canonicalID, city.Name, affectedCounts.Total)

// 	return nil
// }

// func (r *Reconciler) GetStatistics() logger.StatisticsSnapshot {
// 	return r.stats.GetSnapshot()
// }

// func (r *Reconciler) Run() error {
// 	r.logger.Info("Starting city reconciliation process (DryRun: %v)", r.dryRun)

// 	// Get all manual cities
// 	cities, err := r.db.GetManualCities()
// 	if err != nil {
// 		return fmt.Errorf("failed to get manual cities: %w", err)
// 	}

// 	r.stats.SetTotal(len(cities))
// 	r.logger.Info("Found %d manual cities to process", len(cities))

// 	// Process each city
// 	for i, city := range cities {
// 		r.logger.LogProgress(i+1, len(cities), city.Name)

// 		if err := r.ReconcileCity(city); err != nil {
// 			r.logger.Error("Failed to reconcile city %s: %v", city.Name, err)
// 			// Continue with other cities even if one fails
// 		}

// 		// Log stats every 50 cities
// 		if (i+1)%50 == 0 {
// 			stats := r.GetStatistics()
// 			r.logger.LogStats(stats)
// 		}
// 	}

// 	// Final statistics
// 	finalStats := r.GetStatistics()
// 	r.logger.Info("Reconciliation complete!")
// 	r.logger.Info(finalStats.String())
// 	r.logger.LogStats(finalStats)

// 	return nil
// }



type Reconciler struct {
	db             *database.DB
	lightboxClient *lightbox.Client
	logger         *logger.Logger
	stats          *logger.Statistics
	dryRun         bool
}

func NewReconciler(db *database.DB, lightboxClient *lightbox.Client, lgr *logger.Logger, dryRun bool) *Reconciler {
	return &Reconciler{
		db:             db,
		lightboxClient: lightboxClient,
		logger:         lgr,
		stats:          logger.NewStatistics(),
		dryRun:         dryRun,
	}
}

// func (r *Reconciler) ReconcileCity(city database.City) error {
// 	r.logger.Info("Processing city: %s (%s) in county %s", city.Name, city.ID, city.CountyID)
	
// 	startTime := time.Now()
// 	defer func() {
// 		r.stats.RecordAPICall(time.Since(startTime))
// 		r.stats.IncrementProcessed()
// 	}()

// 	// Normalize the county ID
// 	normalizedCountyID := r.lightboxClient.NormalizeCountyFIPS(city.CountyID)
	
// 	// Try to find a match in Lightbox
// 	matchResult, err := r.lightboxClient.FindBestMatch(city.Name, normalizedCountyID)
// 	if err != nil {
// 		r.logger.Error("Failed to search Lightbox for city %s: %v", city.Name, err)
// 		r.stats.IncrementErrors()
// 		return fmt.Errorf("lightbox search failed: %w", err)
// 	}

// 	if !matchResult.Found {
// 		r.logger.LogUnmatchedCity(city.ID, city.Name, city.CountyID, matchResult.Error)
// 		r.stats.IncrementUnmatched(matchResult.Error)
		
// 		// Check if city has incident data before deciding what to do
// 		incidents, err := r.db.GetIncidentsForCity(city.ID)
// 		if err != nil {
// 			r.logger.Error("Failed to check incidents for unmatched city %s: %v", city.Name, err)
// 			// If we can't check incidents, default to deletion
// 			if !r.dryRun {
// 				if err := r.db.DeleteCity(city.ID); err != nil {
// 					r.logger.Error("Failed to delete unmatched city: %v", err)
// 				} else {
// 					r.logger.Info("DELETED unmatched city (incident check failed): %s (%s)", city.Name, city.ID)
// 					r.stats.IncrementConsolidated()
// 				}
// 			}
// 			return nil
// 		}
		
// 		if len(incidents) == 0 {
// 			// No incidents - safe to delete
// 			if !r.dryRun {
// 				if err := r.db.DeleteCity(city.ID); err != nil {
// 					r.logger.Error("Failed to delete unmatched city: %v", err)
// 				} else {
// 					r.logger.Info("DELETED unmatched city (no incidents): %s (%s)", city.Name, city.ID)
// 					r.stats.IncrementConsolidated()
// 				}
// 			}
// 			return nil
// 		}
		
// 		// Has incidents - try geographic correction
// 		r.logger.Info("City %s has %d incidents, attempting geographic correction", city.Name, len(incidents))
// 		return r.CorrectCityGeographically(city)
// 	}

// 	r.stats.IncrementMatched()
// 	lightboxCity := matchResult.LightboxCity
	
// 	r.logger.Info("Found match: %s -> Lightbox ID %s (FIPS: %s, Match: %s, Confidence: %.2f)", 
// 		city.Name, lightboxCity.ID, lightboxCity.FIPS, matchResult.MatchType, matchResult.Confidence)

// 	// Check if canonical city already exists
// 	canonicalCity, err := r.db.FindCanonicalCity(lightboxCity.Location.Locality, lightboxCity.FIPS)
// 	if err != nil {
// 		r.logger.Error("Failed to find canonical city: %v", err)
// 		r.stats.IncrementErrors()
// 		return fmt.Errorf("database error: %w", err)
// 	}

// 	canonicalID := ""
// 	if canonicalCity != nil {
// 		// Use existing canonical city
// 		canonicalID = canonicalCity.ID
// 		r.logger.Info("Using existing canonical city: %s", canonicalID)
// 	} else {
// 		// Create new canonical city with coordinates
// 		canonicalID = lightboxCity.ID
// 		if !r.dryRun {
// 			// First create the coordinate record
// 			coordinatesID := fmt.Sprintf("cities.%s", lightboxCity.ID)
// 			err = r.db.CreateCoordinateRecord(
// 				coordinatesID,
// 				lightboxCity.Location.RepresentativePoint.Latitude,
// 				lightboxCity.Location.RepresentativePoint.Longitude,
// 			)
// 			if err != nil {
// 				r.logger.Error("Failed to create coordinate record: %v", err)
// 				r.stats.IncrementErrors()
// 				return fmt.Errorf("failed to create coordinate record: %w", err)
// 			}
			
// 			// Then create the canonical city
// 			err = r.db.CreateCanonicalCity(lightboxCity.ID, lightboxCity.Location.Locality, lightboxCity.FIPS)
// 			if err != nil {
// 				r.logger.Error("Failed to create canonical city: %v", err)
// 				r.stats.IncrementErrors()
// 				return fmt.Errorf("failed to create canonical city: %w", err)
// 			}
// 		}
// 		r.stats.IncrementCreated()
// 		r.logger.Info("Created new canonical city: %s with coordinates", canonicalID)
// 	}

// 	// If the manual city is the same as canonical, just update its properties
// 	if city.ID == canonicalID {
// 		r.logger.Info("City %s is already canonical, skipping consolidation", city.ID)
// 		return nil
// 	}

// 	// Get affected record counts
// 	affectedCounts, err := r.db.GetAffectedRecordCounts(city.ID)
// 	if err != nil {
// 		r.logger.Error("Failed to get affected record counts: %v", err)
// 		r.stats.IncrementErrors()
// 		return fmt.Errorf("failed to get affected counts: %w", err)
// 	}

// 	r.logger.Info("Records to update for city %s: %+v", city.ID, affectedCounts)

// 	if !r.dryRun {
// 		// Update all references to point to canonical city
// 		if err := r.db.UpdateCityReferences(city.ID, canonicalID); err != nil {
// 			r.logger.Error("Failed to update city references: %v", err)
// 			r.stats.IncrementErrors()
// 			return fmt.Errorf("failed to update references: %w", err)
// 		}

// 		// Delete the manual city
// 		if err := r.db.DeleteCity(city.ID); err != nil {
// 			r.logger.Error("Failed to delete manual city: %v", err)
// 			r.stats.IncrementErrors()
// 			return fmt.Errorf("failed to delete city: %w", err)
// 		}
// 	}

// 	r.stats.IncrementConsolidated()
// 	r.stats.AddUpdatedRecords(affectedCounts.Total)
// 	r.logger.LogReconciliation(city.ID, canonicalID, city.Name, affectedCounts.Total)

// 	return nil
// }

func (r *Reconciler) ReconcileCity(city database.City) error {
	r.logger.Info("Processing city: %s (%s) in county %s", city.Name, city.ID, city.CountyID)
	
	startTime := time.Now()
	defer func() {
		r.stats.RecordAPICall(time.Since(startTime))
		r.stats.IncrementProcessed()
	}()

	// Normalize the county ID
	normalizedCountyID := r.lightboxClient.NormalizeCountyFIPS(city.CountyID)
	
	// Try to find a match in Lightbox
	matchResult, err := r.lightboxClient.FindBestMatch(city.Name, normalizedCountyID)
	if err != nil {
		// Check if this is an API error we should skip
		if r.isSkippableAPIError(err) {
			r.logger.Info("SKIPPING city %s due to API error: %v", city.Name, err)
			return nil // Skip this city, continue with next
		}
		
		r.logger.Error("Failed to search Lightbox for city %s: %v", city.Name, err)
		r.stats.IncrementErrors()
		return fmt.Errorf("lightbox search failed: %w", err)
	}

	if !matchResult.Found {
		r.logger.LogUnmatchedCity(city.ID, city.Name, city.CountyID, matchResult.Error)
		r.stats.IncrementUnmatched(matchResult.Error)
		
		// Check if city has incident data before deciding what to do
		incidents, err := r.db.GetIncidentsForCity(city.ID)
		if err != nil {
			r.logger.Error("Failed to check incidents for unmatched city %s: %v", city.Name, err)
			// If we can't check incidents, default to deletion
			if !r.dryRun {
				if err := r.db.DeleteCity(city.ID); err != nil {
					r.logger.Error("Failed to delete unmatched city: %v", err)
				} else {
					r.logger.Info("DELETED unmatched city (incident check failed): %s (%s)", city.Name, city.ID)
					r.stats.IncrementConsolidated()
				}
			}
			return nil
		}
		
		if len(incidents) == 0 {
			// No incidents - safe to delete
			if !r.dryRun {
				if err := r.db.DeleteCity(city.ID); err != nil {
					r.logger.Error("Failed to delete unmatched city: %v", err)
				} else {
					r.logger.Info("DELETED unmatched city (no incidents): %s (%s)", city.Name, city.ID)
					r.stats.IncrementConsolidated()
				}
			}
			return nil
		}
		
		// Has incidents - try geographic correction (but skip if API fails)
		r.logger.Info("City %s has %d incidents, attempting geographic correction", city.Name, len(incidents))
		return r.CorrectCityGeographicallyWithSkip(city)
	}

	// Rest of your existing code remains exactly the same...
	r.stats.IncrementMatched()
	lightboxCity := matchResult.LightboxCity
	
	r.logger.Info("Found match: %s -> Lightbox ID %s (FIPS: %s, Match: %s, Confidence: %.2f)", 
		city.Name, lightboxCity.ID, lightboxCity.FIPS, matchResult.MatchType, matchResult.Confidence)

	canonicalCity, err := r.db.FindCanonicalCity(lightboxCity.Location.Locality, lightboxCity.FIPS)
	if err != nil {
		r.logger.Error("Failed to find canonical city: %v", err)
		r.stats.IncrementErrors()
		return fmt.Errorf("database error: %w", err)
	}

	canonicalID := ""
	if canonicalCity != nil {
		canonicalID = canonicalCity.ID
		r.logger.Info("Using existing canonical city: %s", canonicalID)
	} else {
		canonicalID = lightboxCity.ID
		if !r.dryRun {
			coordinatesID := fmt.Sprintf("cities.%s", lightboxCity.ID)
			err = r.db.CreateCoordinateRecord(
				coordinatesID,
				lightboxCity.Location.RepresentativePoint.Latitude,
				lightboxCity.Location.RepresentativePoint.Longitude,
			)
			if err != nil {
				r.logger.Error("Failed to create coordinate record: %v", err)
				r.stats.IncrementErrors()
				return fmt.Errorf("failed to create coordinate record: %w", err)
			}
			
			err = r.db.CreateCanonicalCity(lightboxCity.ID, lightboxCity.Location.Locality, lightboxCity.FIPS)
			if err != nil {
				r.logger.Error("Failed to create canonical city: %v", err)
				r.stats.IncrementErrors()
				return fmt.Errorf("failed to create canonical city: %w", err)
			}
		}
		r.stats.IncrementCreated()
		r.logger.Info("Created new canonical city: %s with coordinates", canonicalID)
	}

	if city.ID == canonicalID {
		r.logger.Info("City %s is already canonical, skipping consolidation", city.ID)
		return nil
	}

	affectedCounts, err := r.db.GetAffectedRecordCounts(city.ID)
	if err != nil {
		r.logger.Error("Failed to get affected record counts: %v", err)
		r.stats.IncrementErrors()
		return fmt.Errorf("failed to get affected counts: %w", err)
	}

	r.logger.Info("Records to update for city %s: %+v", city.ID, affectedCounts)

	if !r.dryRun {
		if err := r.db.UpdateCityReferences(city.ID, canonicalID); err != nil {
			r.logger.Error("Failed to update city references: %v", err)
			r.stats.IncrementErrors()
			return fmt.Errorf("failed to update references: %w", err)
		}

		if err := r.db.DeleteCity(city.ID); err != nil {
			r.logger.Error("Failed to delete manual city: %v", err)
			r.stats.IncrementErrors()
			return fmt.Errorf("failed to delete city: %w", err)
		}
	}

	r.stats.IncrementConsolidated()
	r.stats.AddUpdatedRecords(affectedCounts.Total)
	r.logger.LogReconciliation(city.ID, canonicalID, city.Name, affectedCounts.Total)

	return nil
}

func (r *Reconciler) CorrectCityGeographicallyWithSkip(city database.City) error {
	r.logger.Info("Attempting geographic correction for: %s (%s)", city.Name, city.ID)
	
	// Get incidents for this bad city
	incidents, err := r.db.GetIncidentsForCity(city.ID)
	if err != nil {
		r.logger.Error("Failed to get incidents for geographic correction: %v", err)
		return fmt.Errorf("failed to get incidents: %w", err)
	}
	
	if len(incidents) == 0 {
		// No incidents, safe to delete bad city
		if !r.dryRun {
			if err := r.db.DeleteCity(city.ID); err != nil {
				r.logger.Error("Failed to delete empty bad city: %v", err)
				return err
			}
		}
		r.logger.Info("DELETED empty bad city: %s (%s)", city.Name, city.ID)
		return nil
	}
	
	r.logger.Info("Found %d incidents for geographic correction", len(incidents))
	
	// Group incidents by approximate location (round to 3 decimal places)
	locationGroups := make(map[database.LocationGroup][]database.Incident)
	for _, incident := range incidents {
		key := database.LocationGroup{
			Lat: math.Round(incident.Latitude*1000) / 1000,
			Lng: math.Round(incident.Longitude*1000) / 1000,
		}
		locationGroups[key] = append(locationGroups[key], incident)
	}
	
	r.logger.Info("Grouped incidents into %d location clusters", len(locationGroups))
	
	// Track if we had any API errors during geographic correction
	hasAPIErrors := false
	
	for location, incidentGroup := range locationGroups {
		// Find correct Lightbox city based on coordinates - FIX: add limit parameter
		correctLightboxCity, err := r.lightboxClient.FindCityByCoordinates(location.Lat, location.Lng, 1000)
		if err != nil {
			// Check if this is a skippable API error
			if r.isSkippableAPIError(err) {
				r.logger.Info("SKIPPING location (%f, %f) due to API error: %v", location.Lat, location.Lng, err)
				hasAPIErrors = true
				continue
			}
			r.logger.Error("Failed to find city by coordinates (%f, %f): %v", location.Lat, location.Lng, err)
			continue
		}
		
		if correctLightboxCity == nil {
			r.logger.Info("No city found for coordinates (%f, %f)", location.Lat, location.Lng)
			continue
		}
		
		// Check if canonical city exists
		canonicalCity, err := r.db.FindCanonicalCity(correctLightboxCity.Location.Locality, correctLightboxCity.FIPS)
		if err != nil {
			r.logger.Error("Failed to find canonical city: %v", err)
			continue
		}
		
		var targetCityID string
		if canonicalCity != nil {
			targetCityID = canonicalCity.ID
			r.logger.Info("Using existing canonical city: %s", targetCityID)
		} else {
			// Create new canonical city with proper coordinates ID
			targetCityID = correctLightboxCity.ID
			coordinatesID := fmt.Sprintf("cities.%s", correctLightboxCity.ID)
			
			if !r.dryRun {
				err = r.db.CreateCanonicalCityWithCoordinates(
					correctLightboxCity.ID, 
					correctLightboxCity.Location.Locality, 
					correctLightboxCity.FIPS,
					coordinatesID,
				)
				if err != nil {
					r.logger.Error("Failed to create canonical city: %v", err)
					continue
				}
			}
			r.stats.IncrementCreated()
			r.logger.Info("Created new canonical city: %s", targetCityID)
		}
		
		// Update incidents to correct city
		r.logger.Info("Correcting %d incidents: %s -> %s", len(incidentGroup), city.Name, correctLightboxCity.Location.Locality)
		
		for _, incident := range incidentGroup {
			if !r.dryRun {
				if err := r.db.UpdateIncidentCity(incident.ID, targetCityID); err != nil {
					r.logger.Error("Failed to update incident %s: %v", incident.ID, err)
					continue
				}
			}
		}
		r.stats.AddUpdatedRecords(len(incidentGroup))
	}
	
	// If we had API errors, skip deleting the city (leave it for retry later)
	if hasAPIErrors {
		r.logger.Info("SKIPPING deletion of city %s due to API errors during geographic correction", city.Name)
		return nil
	}
	
	// Delete the bad manual city (coordinates remain orphaned)
	if !r.dryRun {
		if err := r.db.DeleteCity(city.ID); err != nil {
			r.logger.Error("Failed to delete corrected city: %v", err)
			return err
		}
	}
	
	r.stats.IncrementConsolidated()
	r.logger.Info("DELETED geographically corrected city: %s (%s)", city.Name, city.ID)
	
	return nil
}

// Add this helper method to the reconciler
func (r *Reconciler) isSkippableAPIError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "context deadline exceeded") ||
		   strings.Contains(errStr, "Client.Timeout exceeded") ||
		   strings.Contains(errStr, "502 Server Error") ||
		   strings.Contains(errStr, "503 Service Unavailable") ||
		   strings.Contains(errStr, "504 Gateway Timeout")
}

func (r *Reconciler) CorrectCityGeographically(city database.City) error {
	r.logger.Info("Attempting geographic correction for: %s (%s)", city.Name, city.ID)
	
	// Get incidents for this bad city
	incidents, err := r.db.GetIncidentsForCity(city.ID)
	if err != nil {
		r.logger.Error("Failed to get incidents for geographic correction: %v", err)
		return fmt.Errorf("failed to get incidents: %w", err)
	}
	
	if len(incidents) == 0 {
		// No incidents, safe to delete bad city
		if !r.dryRun {
			if err := r.db.DeleteCity(city.ID); err != nil {
				r.logger.Error("Failed to delete empty bad city: %v", err)
				return err
			}
		}
		r.logger.Info("DELETED empty bad city: %s (%s)", city.Name, city.ID)
		return nil
	}
	
	r.logger.Info("Found %d incidents for geographic correction", len(incidents))
	
	// Group incidents by approximate location (round to 3 decimal places)
	locationGroups := make(map[database.LocationGroup][]database.Incident)
	for _, incident := range incidents {
		key := database.LocationGroup{
			Lat: math.Round(incident.Latitude*1000) / 1000,
			Lng: math.Round(incident.Longitude*1000) / 1000,
		}
		locationGroups[key] = append(locationGroups[key], incident)
	}
	
	r.logger.Info("Grouped incidents into %d location clusters", len(locationGroups))
	
	for location, incidentGroup := range locationGroups {
		// Find correct Lightbox city based on coordinates
		// Change this line in reconciler.go
correctLightboxCity, err := r.lightboxClient.FindCityByCoordinates(location.Lat, location.Lng, 1000)
		if err != nil {
			r.logger.Error("Failed to find city by coordinates (%f, %f): %v", location.Lat, location.Lng, err)
			continue
		}
		
		if correctLightboxCity == nil {
			r.logger.Info("No city found for coordinates (%f, %f)", location.Lat, location.Lng)
			continue
		}
		
		// Check if canonical city exists
		canonicalCity, err := r.db.FindCanonicalCity(correctLightboxCity.Location.Locality, correctLightboxCity.FIPS)
		if err != nil {
			r.logger.Error("Failed to find canonical city: %v", err)
			continue
		}
		
		var targetCityID string
		if canonicalCity != nil {
			targetCityID = canonicalCity.ID
			r.logger.Info("Using existing canonical city: %s", targetCityID)
		} else {
			// Create new canonical city with proper coordinates ID
			targetCityID = correctLightboxCity.ID
			coordinatesID := fmt.Sprintf("cities.%s", correctLightboxCity.ID)
			
			if !r.dryRun {
				err = r.db.CreateCanonicalCityWithCoordinates(
					correctLightboxCity.ID, 
					correctLightboxCity.Location.Locality, 
					correctLightboxCity.FIPS,
					coordinatesID,
				)
				if err != nil {
					r.logger.Error("Failed to create canonical city: %v", err)
					continue
				}
			}
			r.stats.IncrementCreated()
			r.logger.Info("Created new canonical city: %s", targetCityID)
		}
		
		// Update incidents to correct city
		r.logger.Info("Correcting %d incidents: %s -> %s", len(incidentGroup), city.Name, correctLightboxCity.Location.Locality)
		
		for _, incident := range incidentGroup {
			if !r.dryRun {
				if err := r.db.UpdateIncidentCity(incident.ID, targetCityID); err != nil {
					r.logger.Error("Failed to update incident %s: %v", incident.ID, err)
					continue
				}
			}
		}
		r.stats.AddUpdatedRecords(len(incidentGroup))
	}
	
	// Delete the bad manual city (coordinates remain orphaned)
	if !r.dryRun {
		if err := r.db.DeleteCity(city.ID); err != nil {
			r.logger.Error("Failed to delete corrected city: %v", err)
			return err
		}
	}
	
	r.stats.IncrementConsolidated()
	r.logger.Info("DELETED geographically corrected city: %s (%s)", city.Name, city.ID)
	
	return nil
}

func (r *Reconciler) GetStatistics() logger.StatisticsSnapshot {
	return r.stats.GetSnapshot()
}

func (r *Reconciler) Run() error {
	r.logger.Info("Starting city reconciliation process (DryRun: %v)", r.dryRun)

	// Get all manual cities
	cities, err := r.db.GetManualCities()
	if err != nil {
		return fmt.Errorf("failed to get manual cities: %w", err)
	}

	r.stats.SetTotal(len(cities))
	r.logger.Info("Found %d manual cities to process", len(cities))

	// Process each city
	for i, city := range cities {
		r.logger.LogProgress(i+1, len(cities), city.Name)
		
		if err := r.ReconcileCity(city); err != nil {
			r.logger.Error("Failed to reconcile city %s: %v", city.Name, err)
			// Continue with other cities even if one fails
		}

		// Log stats every 50 cities
		if (i+1)%50 == 0 {
			stats := r.GetStatistics()
			r.logger.LogStats(stats)
		}
	}

	// Final statistics
	finalStats := r.GetStatistics()
	r.logger.Info("Reconciliation complete!")
	r.logger.Info(finalStats.String())
	r.logger.LogStats(finalStats)

	return nil
}