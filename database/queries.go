package database

import (
	"database/sql"
	"fmt"
)

func (db *DB) GetOrphanedCities() ([]OrphanedCity, error) {
    query := `
        SELECT 
            im.cityId,
            COALESCE(im.cityName, '') as cityName,
            COALESCE(im.countyName, '') as countyName,
            COALESCE(im.stateId, '') as stateId,
            COALESCE(im.stateName, '') as stateName,
            COUNT(*) as incident_count
        FROM incident_metadata im 
        LEFT JOIN cities c ON im.cityId = c.id 
        WHERE c.id IS NULL AND im.cityId IS NOT NULL
        GROUP BY im.cityId, im.cityName, im.countyName, im.stateId, im.stateName
        ORDER BY incident_count DESC
    `
    
    rows, err := db.Query(query)
    if err != nil {
        return nil, fmt.Errorf("failed to query orphaned cities: %w", err)
    }
    defer rows.Close()
    
    var cities []OrphanedCity
    for rows.Next() {
        var city OrphanedCity
        err := rows.Scan(&city.ID, &city.Name, &city.County, &city.StateCode, &city.StateName, &city.IncidentCount)
        if err != nil {
            return nil, fmt.Errorf("failed to scan orphaned city: %w", err)
        }
        cities = append(cities, city)
    }
    
    return cities, nil
}

func (db *DB) GetManualCities() ([]City, error) {
	query := `
		SELECT id, name, CountyId, COALESCE(expandedName, '') as expandedName, 
		       COALESCE(CoordinatesId, '') as CoordinatesId, isManual
		FROM cities 
		WHERE isManual = 1 AND name IS NOT NULL AND name != ''
		ORDER BY name
	`

	// query := `
	// 	SELECT id, name, CountyId, COALESCE(expandedName, '') as expandedName, 
	// 	       COALESCE(CoordinatesId, '') as CoordinatesId, isManual
	// 	FROM cities 
	// 	WHERE isManual = 1 AND name IS NOT NULL AND name != '' AND name = 'Winton'
	// 	ORDER BY name
	// `

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query manual cities: %w", err)
	}
	defer rows.Close()

	var cities []City
	for rows.Next() {
		var city City
		err := rows.Scan(&city.ID, &city.Name, &city.CountyID, &city.ExpandedName, &city.CoordinatesID, &city.IsManual)
		if err != nil {
			return nil, fmt.Errorf("failed to scan city row: %w", err)
		}
		cities = append(cities, city)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating city rows: %w", err)
	}

	return cities, nil
}

func (db *DB) GetCityByID(cityID string) (*City, error) {
	query := `
		SELECT id, name, CountyId, COALESCE(expandedName, '') as expandedName,
		       COALESCE(CoordinatesId, '') as CoordinatesId, isManual
		FROM cities 
		WHERE id = ?
	`

	var city City
	err := db.QueryRow(query, cityID).Scan(
		&city.ID, &city.Name, &city.CountyID, &city.ExpandedName, &city.CoordinatesID, &city.IsManual,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get city by ID %s: %w", cityID, err)
	}

	return &city, nil
}

func (db *DB) FindCanonicalCity(name, countyID string) (*City, error) {
	query := `
		SELECT id, name, CountyId, COALESCE(expandedName, '') as expandedName,
		       COALESCE(CoordinatesId, '') as CoordinatesId, isManual
		FROM cities 
		WHERE name = ? AND CountyId = ? AND isManual = 0
		LIMIT 1
	`

	var city City
	err := db.QueryRow(query, name, countyID).Scan(
		&city.ID, &city.Name, &city.CountyID, &city.ExpandedName, &city.CoordinatesID, &city.IsManual,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to find canonical city for %s in county %s: %w", name, countyID, err)
	}

	return &city, nil
}

func (db *DB) GetAffectedRecordCounts(cityID string) (*AffectedRecordCounts, error) {
	counts := &AffectedRecordCounts{}

	queries := []struct {
		query string
		count *int
	}{
		{"SELECT COUNT(*) FROM addresses WHERE CityId = ?", &counts.Addresses},
		{"SELECT COUNT(*) FROM incident_metadata WHERE cityId = ?", &counts.Incidents},
		{"SELECT COUNT(*) FROM user_city_subscriptions WHERE CityId = ?", &counts.UserSubscriptions},
		{"SELECT COUNT(*) FROM business_city_subscriptions WHERE CityId = ?", &counts.BusinessSubscriptions},
		{"SELECT COUNT(*) FROM user_incident_access WHERE cityid = ?", &counts.UserIncidentAccess},
	}

	for _, q := range queries {
		err := db.QueryRow(q.query, cityID).Scan(q.count)
		if err != nil {
			return nil, fmt.Errorf("failed to count records for city %s: %w", cityID, err)
		}
	}

	counts.Total = counts.Addresses + counts.Incidents + counts.UserSubscriptions + 
		counts.BusinessSubscriptions + counts.UserIncidentAccess

	return counts, nil
}

func (db *DB) GetIncidentsForCity(cityID string) ([]Incident, error) {
	query := `
		SELECT id, cityId, latitude, longitude, addressRaw
		FROM incident_metadata 
		WHERE cityId = ? AND latitude IS NOT NULL AND longitude IS NOT NULL
	`

	rows, err := db.Query(query, cityID)
	if err != nil {
		return nil, fmt.Errorf("failed to query incidents for city %s: %w", cityID, err)
	}
	defer rows.Close()

	var incidents []Incident
	for rows.Next() {
		var incident Incident
		err := rows.Scan(&incident.ID, &incident.CityID, &incident.Latitude, &incident.Longitude, &incident.Address)
		if err != nil {
			return nil, fmt.Errorf("failed to scan incident row: %w", err)
		}
		incidents = append(incidents, incident)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating incident rows: %w", err)
	}

	return incidents, nil
}

func (db *DB) UpdateIncidentCity(incidentID, newCityID string) error {
	query := "UPDATE incident_metadata SET cityId = ? WHERE id = ?"
	_, err := db.Exec(query, newCityID, incidentID)
	if err != nil {
		return fmt.Errorf("failed to update incident %s to city %s: %w", incidentID, newCityID, err)
	}
	return nil
}