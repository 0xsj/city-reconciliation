package lightbox

import (
	"fmt"
	"strings"
)

func (c *Client) FindBestMatch(cityName string) (*MatchResult, error) {
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

    // For orphaned cities, just take the first match if name matches exactly
    cityNameLower := strings.ToLower(strings.TrimSpace(cityName))
    
    for _, city := range resp.Cities {
        lightboxNameLower := strings.ToLower(strings.TrimSpace(city.Location.Locality))
        
        if lightboxNameLower == cityNameLower {
            return &MatchResult{
                Found:        true,
                LightboxCity: &city,
                MatchType:    "name_match",
                Confidence:   0.8,
            }, nil
        }
    }

    // If no exact match, return the first result with lower confidence
    return &MatchResult{
        Found:        true,
        LightboxCity: &resp.Cities[0],
        MatchType:    "partial_match", 
        Confidence:   0.6,
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