package database

type OrphanedCity struct {
    ID            string `db:"cityId"`
    Name          string `db:"cityName"`
    County        string `db:"countyName"`
    StateCode     string `db:"stateId"`
    StateName     string `db:"stateName"`
    IncidentCount int    `db:"incident_count"`
}

type City struct {
	ID            string `db:"id"`
	Name          string `db:"name"`
	CountyID      string `db:"CountyId"`
	ExpandedName  string `db:"expandedName"`
	CoordinatesID string `db:"CoordinatesId"`
	IsManual      bool   `db:"isManual"`
}

type Incident struct {
	ID        string  `db:"id"`
	CityID    string  `db:"cityId"`
	Latitude  float64 `db:"latitude"`
	Longitude float64 `db:"longitude"`
	Address   string  `db:"address"`
}

type LocationGroup struct {
	Lat float64
	Lng float64
}

type AffectedRecordCounts struct {
	Addresses            int
	Incidents            int
	UserSubscriptions    int
	BusinessSubscriptions int
	UserIncidentAccess   int
	Total                int
}

type ReconciliationResult struct {
	OriginalCity    City
	CanonicalCity   City
	AffectedCounts  AffectedRecordCounts
	Success         bool
	Error           string
}

type Stats struct {
	TotalCities         int
	ProcessedCities     int
	SuccessfulMatches   int
	UnmatchedCities     int
	ConsolidatedCities  int
	UpdatedRecords      int
	Errors              int
}