// package database

// import (
// 	"database/sql"
// 	"fmt"
// )

// func (db *DB) UpdateCityReferences(oldCityID, newCityID string) error {
// 	tx, err := db.Begin()
// 	if err != nil {
// 		return fmt.Errorf("failed to begin transaction: %w", err)
// 	}
// 	defer tx.Rollback()

// 	updates := []struct {
// 		table  string
// 		column string
// 		query  string
// 	}{
// 		{"addresses", "CityId", "UPDATE addresses SET CityId = ? WHERE CityId = ?"},
// 		{"incident_metadata", "cityId", "UPDATE incident_metadata SET cityId = ? WHERE cityId = ?"},
// 		{"user_city_subscriptions", "CityId", "UPDATE user_city_subscriptions SET CityId = ? WHERE CityId = ?"},
// 		{"business_city_subscriptions", "CityId", "UPDATE business_city_subscriptions SET CityId = ? WHERE CityId = ?"},
// 		{"user_incident_access", "cityid", "UPDATE user_incident_access SET cityid = ? WHERE cityid = ?"},
// 	}

// 	totalUpdated := 0
// 	for _, update := range updates {
// 		result, err := tx.Exec(update.query, newCityID, oldCityID)
// 		if err != nil {
// 			return fmt.Errorf("failed to update %s.%s: %w", update.table, update.column, err)
// 		}

// 		rowsAffected, err := result.RowsAffected()
// 		if err != nil {
// 			return fmt.Errorf("failed to get rows affected for %s.%s: %w", update.table, update.column, err)
// 		}

// 		totalUpdated += int(rowsAffected)
// 		fmt.Printf("Updated %d records in %s.%s\n", rowsAffected, update.table, update.column)
// 	}

// 	if err := tx.Commit(); err != nil {
// 		return fmt.Errorf("failed to commit transaction: %w", err)
// 	}

// 	fmt.Printf("Total records updated: %d\n", totalUpdated)
// 	return nil
// }

// func (db *DB) DeleteCity(cityID string) error {
// 	query := "DELETE FROM cities WHERE id = ?"
// 	result, err := db.Exec(query, cityID)
// 	if err != nil {
// 		return fmt.Errorf("failed to delete city %s: %w", cityID, err)
// 	}

// 	rowsAffected, err := result.RowsAffected()
// 	if err != nil {
// 		return fmt.Errorf("failed to get rows affected for city deletion: %w", err)
// 	}

// 	if rowsAffected == 0 {
// 		return fmt.Errorf("no city found with ID %s", cityID)
// 	}

// 	fmt.Printf("Deleted city %s\n", cityID)
// 	return nil
// }

// func (db *DB) CreateCanonicalCity(lightboxID, name, countyID string) error {
// 	query := `
// 		INSERT INTO cities (id, name, CountyId, expandedName, CoordinatesId, isManual)
// 		VALUES (?, ?, ?, NULL, ?, 0)
// 	`

// 	coordinatesID := fmt.Sprintf("cities.%s", lightboxID)

// 	_, err := db.Exec(query, lightboxID, name, countyID, coordinatesID)
// 	if err != nil {
// 		return fmt.Errorf("failed to create canonical city %s: %w", lightboxID, err)
// 	}

// 	fmt.Printf("Created canonical city %s (%s) in county %s\n", lightboxID, name, countyID)
// 	return nil
// }

// func (db *DB) ExecuteInTransaction(fn func(*sql.Tx) error) error {
// 	tx, err := db.Begin()
// 	if err != nil {
// 		return fmt.Errorf("failed to begin transaction: %w", err)
// 	}
// 	defer tx.Rollback()

// 	if err := fn(tx); err != nil {
// 		return err
// 	}

// 	if err := tx.Commit(); err != nil {
// 		return fmt.Errorf("failed to commit transaction: %w", err)
// 	}

// 	return nil
// }

package database

import (
	"database/sql"
	"fmt"
)

func (db *DB) UpdateCityReferences(oldCityID, newCityID string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	updates := []struct {
		table  string
		column string
		query  string
	}{
		{"addresses", "CityId", "UPDATE addresses SET CityId = ? WHERE CityId = ?"},
		{"incident_metadata", "cityId", "UPDATE incident_metadata SET cityId = ? WHERE cityId = ?"},
		{"user_city_subscriptions", "CityId", "UPDATE user_city_subscriptions SET CityId = ? WHERE CityId = ?"},
		{"business_city_subscriptions", "CityId", "UPDATE business_city_subscriptions SET CityId = ? WHERE CityId = ?"},
		{"user_incident_access", "cityid", "UPDATE user_incident_access SET cityid = ? WHERE cityid = ?"},
	}

	totalUpdated := 0
	for _, update := range updates {
		result, err := tx.Exec(update.query, newCityID, oldCityID)
		if err != nil {
			return fmt.Errorf("failed to update %s.%s: %w", update.table, update.column, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected for %s.%s: %w", update.table, update.column, err)
		}

		totalUpdated += int(rowsAffected)
		fmt.Printf("Updated %d records in %s.%s\n", rowsAffected, update.table, update.column)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Total records updated: %d\n", totalUpdated)
	return nil
}

func (db *DB) DeleteCity(cityID string) error {
	query := "DELETE FROM cities WHERE id = ?"
	result, err := db.Exec(query, cityID)
	if err != nil {
		return fmt.Errorf("failed to delete city %s: %w", cityID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for city deletion: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no city found with ID %s", cityID)
	}

	fmt.Printf("Deleted city %s\n", cityID)
	return nil
}

// func (db *DB) CreateCanonicalCity(lightboxID, name, countyID string) error {
// 	query := `
// 		INSERT INTO cities (id, name, CountyId, expandedName, CoordinatesId, isManual) 
// 		VALUES (?, ?, ?, NULL, ?, 0)
// 	`

// 	coordinatesID := fmt.Sprintf("cities.%s", lightboxID)
	
// 	_, err := db.Exec(query, lightboxID, name, countyID, coordinatesID)
// 	if err != nil {
// 		return fmt.Errorf("failed to create canonical city %s: %w", lightboxID, err)
// 	}

// 	fmt.Printf("Created canonical city %s (%s) in county %s\n", lightboxID, name, countyID)
// 	return nil
// }

func (db *DB) CreateCanonicalCity(lightboxID, name, countyID string) error {
	coordinatesID := fmt.Sprintf("cities.%s", lightboxID)
	
	query := `
		INSERT INTO cities (id, name, CountyId, expandedName, CoordinatesId, isManual) 
		VALUES (?, ?, ?, NULL, ?, 0)
	`
	
	_, err := db.Exec(query, lightboxID, name, countyID, coordinatesID)
	if err != nil {
		return fmt.Errorf("failed to create canonical city %s: %w", lightboxID, err)
	}

	fmt.Printf("Created canonical city %s (%s) in county %s\n", lightboxID, name, countyID)
	return nil
}

func (db *DB) CreateCanonicalCityWithCoordinates(lightboxID, name, countyID, coordinatesID string) error {
	query := `
		INSERT INTO cities (id, name, CountyId, expandedName, CoordinatesId, isManual) 
		VALUES (?, ?, ?, NULL, ?, 0)
	`
	
	_, err := db.Exec(query, lightboxID, name, countyID, coordinatesID)
	if err != nil {
		return fmt.Errorf("failed to create canonical city %s with coordinates: %w", lightboxID, err)
	}

	fmt.Printf("Created canonical city %s (%s) in county %s with coordinates %s\n", 
		lightboxID, name, countyID, coordinatesID)
	return nil
}

func (db *DB) CreateCoordinateRecord(coordinatesID string, lat, lng float64) error {
	query := `INSERT IGNORE INTO coordinates (id, latitude, longitude) VALUES (?, ?, ?)`
	_, err := db.Exec(query, coordinatesID, lat, lng)
	if err != nil {
		return fmt.Errorf("failed to create coordinate record %s: %w", coordinatesID, err)
	}
	return nil
}

func (db *DB) ExecuteInTransaction(fn func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}