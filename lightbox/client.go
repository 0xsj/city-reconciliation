// package lightbox

// import (
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"net/http"
// 	"net/url"
// 	"strings"
// 	"time"
// )

// type Client struct {
// 	BaseURL    string
// 	APIKey     string
// 	HTTPClient *http.Client
// }

// func NewClient(baseURL, apiKey string) *Client {
// 	return &Client{
// 		BaseURL: strings.TrimSuffix(baseURL, "/"),
// 		APIKey:  apiKey,
// 		HTTPClient: &http.Client{
// 			Timeout: 30 * time.Second,
// 		},
// 	}
// }

// func (c *Client) SearchCities(params SearchParams) (*LightboxResponse, error) {
// 	if params.Limit == 0 {
// 		params.Limit = 100
// 	}

// 	// Build the search text - start with city name
// 	searchText := params.CityName

// 	// Build the URL
// 	endpoint := fmt.Sprintf("%s/cities/us/_autocomplete", c.BaseURL)
// 	u, err := url.Parse(endpoint)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to parse URL: %w", err)
// 	}

// 	// Add query parameters
// 	query := u.Query()
// 	query.Set("text", searchText)
// 	query.Set("limit", fmt.Sprintf("%d", params.Limit))
// 	u.RawQuery = query.Encode()

// 	// Create request
// 	req, err := http.NewRequest("GET", u.String(), nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create request: %w", err)
// 	}

// 	// Add API key header - FIXED
// 	req.Header.Set("x-api-key", c.APIKey)
// 	req.Header.Set("Content-Type", "application/json")

// 	// Make request
// 	resp, err := c.HTTPClient.Do(req)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to make request: %w", err)
// 	}
// 	defer resp.Body.Close()

// 	// Check status code
// 	if resp.StatusCode != http.StatusOK {
// 		body, _ := io.ReadAll(resp.Body)
// 		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
// 	}

// 	// Parse response
// 	var lightboxResp LightboxResponse
// 	if err := json.NewDecoder(resp.Body).Decode(&lightboxResp); err != nil {
// 		return nil, fmt.Errorf("failed to decode response: %w", err)
// 	}

// 	return &lightboxResp, nil
// }

// func (c *Client) GetCityByID(cityID string) (*LightboxCity, error) {
// 	endpoint := fmt.Sprintf("%s/cities/us/%s", c.BaseURL, cityID)

// 	req, err := http.NewRequest("GET", endpoint, nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create request: %w", err)
// 	}

// 	// Add API key header - FIXED
// 	req.Header.Set("x-api-key", c.APIKey)
// 	req.Header.Set("Content-Type", "application/json")

// 	resp, err := c.HTTPClient.Do(req)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to make request: %w", err)
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode == http.StatusNotFound {
// 		return nil, nil
// 	}

// 	if resp.StatusCode != http.StatusOK {
// 		body, _ := io.ReadAll(resp.Body)
// 		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
// 	}

// 	var city LightboxCity
// 	if err := json.NewDecoder(resp.Body).Decode(&city); err != nil {
// 		return nil, fmt.Errorf("failed to decode response: %w", err)
// 	}

// 	return &city, nil
// }

package lightbox

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
		APIKey:  apiKey,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) SearchCities(params SearchParams) (*LightboxResponse, error) {
	if params.Limit == 0 {
		params.Limit = 100
	}

	// Build the search text - start with city name
	searchText := params.CityName

	// Build the URL
	endpoint := fmt.Sprintf("%s/cities/us/_autocomplete", c.BaseURL)
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Add query parameters
	query := u.Query()
	query.Set("text", searchText)
	query.Set("limit", fmt.Sprintf("%d", params.Limit))
	u.RawQuery = query.Encode()

	// Create request
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add API key header
	req.Header.Set("x-api-key", c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Make request
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var lightboxResp LightboxResponse
	if err := json.NewDecoder(resp.Body).Decode(&lightboxResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &lightboxResp, nil
}

func (c *Client) GetCityByID(cityID string) (*LightboxCity, error) {
	endpoint := fmt.Sprintf("%s/cities/us/%s", c.BaseURL, cityID)
	
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("x-api-key", c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var city LightboxCity
	if err := json.NewDecoder(resp.Body).Decode(&city); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &city, nil
}

func (c *Client) FindCityByCoordinates(lat, lng float64, limit int) (*LightboxCity, error) {
	// Search for cities using a broad term instead of empty search
	endpoint := fmt.Sprintf("%s/cities/us/_autocomplete", c.BaseURL)
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	// Use configurable limit, default to 1000 for better geographic coverage
	if limit == 0 {
		limit = 1000
	}

	// Use a broad search term instead of empty string
	query := u.Query()
	query.Set("text", "city")
	query.Set("limit", fmt.Sprintf("%d", limit))
	u.RawQuery = query.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("x-api-key", c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var lightboxResp LightboxResponse
	if err := json.NewDecoder(resp.Body).Decode(&lightboxResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Find closest city by distance
	var closestCity *LightboxCity
	minDistance := math.MaxFloat64

	for _, city := range lightboxResp.Cities {
		distance := c.calculateDistance(lat, lng, 
			city.Location.RepresentativePoint.Latitude, 
			city.Location.RepresentativePoint.Longitude)
		
		if distance < minDistance {
			minDistance = distance
			closestCity = &city
		}
	}

	// Only return if within reasonable distance (e.g., 50km)
	if minDistance < 50.0 && closestCity != nil {
		return closestCity, nil
	}

	return nil, nil
}

func (c *Client) calculateDistance(lat1, lng1, lat2, lng2 float64) float64 {
	// Haversine formula for distance calculation
	const earthRadius = 6371 // km

	dLat := (lat2 - lat1) * math.Pi / 180
	dLng := (lng2 - lng1) * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLng/2)*math.Sin(dLng/2)

	distance := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadius * distance
}