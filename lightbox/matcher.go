package lightbox

import (
	"fmt"
	"strings"
)

func (c *Client) FindBestMatch(cityName, countyID string) (*MatchResult, error) {
	// Extract state from county ID (first 2 digits)
	if len(countyID) < 2 {
		return &MatchResult{
			Found:     false,
			MatchType: "none",
			Error:     "invalid county ID format",
		}, nil
	}

	stateFIPS := countyID[:2]
	
	// Search for cities with this name
	params := SearchParams{
		CityName: cityName,
		Limit:    100,
	}

	resp, err := c.SearchCities(params)
	if err != nil {
		return &MatchResult{
			Found:     false,
			MatchType: "none",
			Error:     fmt.Sprintf("API error: %v", err),
		}, nil
	}

	if len(resp.Cities) == 0 {
		return &MatchResult{
			Found:     false,
			MatchType: "none",
			Error:     "no cities found",
		}, nil
	}

	// Try to find exact matches
	exactMatches := c.findExactMatches(resp.Cities, cityName, countyID)
	if len(exactMatches) == 1 {
		return &MatchResult{
			Found:        true,
			LightboxCity: &exactMatches[0],
			MatchType:    "exact",
			Confidence:   1.0,
		}, nil
	}

	if len(exactMatches) > 1 {
		return &MatchResult{
			Found:        true,
			LightboxCity: &exactMatches[0], // Take the first one
			MatchType:    "exact_multiple",
			Confidence:   0.9,
			Error:        fmt.Sprintf("found %d exact matches, using first", len(exactMatches)),
		}, nil
	}

	// Try fuzzy matches by state
	stateMatches := c.findStateMatches(resp.Cities, cityName, stateFIPS)
	if len(stateMatches) == 1 {
		return &MatchResult{
			Found:        true,
			LightboxCity: &stateMatches[0],
			MatchType:    "state_match",
			Confidence:   0.8,
		}, nil
	}

	if len(stateMatches) > 1 {
		return &MatchResult{
			Found:        true,
			LightboxCity: &stateMatches[0], // Take the first one
			MatchType:    "state_multiple",
			Confidence:   0.7,
			Error:        fmt.Sprintf("found %d state matches, using first", len(stateMatches)),
		}, nil
	}

	// No good matches found
	return &MatchResult{
		Found:     false,
		MatchType: "none",
		Error:     fmt.Sprintf("no suitable matches found among %d results", len(resp.Cities)),
	}, nil
}

func (c *Client) findExactMatches(cities []LightboxCity, cityName, countyID string) []LightboxCity {
	var matches []LightboxCity
	
	cityNameLower := strings.ToLower(strings.TrimSpace(cityName))
	
	for _, city := range cities {
		lightboxNameLower := strings.ToLower(strings.TrimSpace(city.Location.Locality))
		
		// Check if names match and county FIPS matches
		if lightboxNameLower == cityNameLower && city.FIPS == countyID {
			matches = append(matches, city)
		}
	}
	
	return matches
}

func (c *Client) findStateMatches(cities []LightboxCity, cityName, stateFIPS string) []LightboxCity {
	var matches []LightboxCity
	
	cityNameLower := strings.ToLower(strings.TrimSpace(cityName))
	
	for _, city := range cities {
		lightboxNameLower := strings.ToLower(strings.TrimSpace(city.Location.Locality))
		
		// Check if names match and state FIPS matches
		if lightboxNameLower == cityNameLower && city.StateFIPS == stateFIPS {
			matches = append(matches, city)
		}
	}
	
	return matches
}

func (c *Client) NormalizeCountyFIPS(countyID string) string {
	// Remove any non-numeric characters and ensure 5 digits
	cleaned := ""
	for _, r := range countyID {
		if r >= '0' && r <= '9' {
			cleaned += string(r)
		}
	}
	
	// Pad with leading zeros if needed
	for len(cleaned) < 5 {
		cleaned = "0" + cleaned
	}
	
	return cleaned
}