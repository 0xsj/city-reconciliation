package lightbox

type LightboxResponse struct {
	Ref      string `json:"$ref"`
	Metadata struct {
		RecordSet struct {
			TotalRecords float64 `json:"totalRecords"`
		} `json:"recordSet"`
	} `json:"$metadata"`
	Cities []LightboxCity `json:"cities"`
}

type LightboxCity struct {
	Ref       string `json:"$ref"`
	ID        string `json:"id"`
	FIPS      string `json:"fips"`
	StateFIPS string `json:"stateFips"`
	County    string `json:"county"`
	Location  struct {
		Locality           string `json:"locality"`
		RegionCode         string `json:"regionCode"`
		CountryCode        string `json:"countryCode"`
		RepresentativePoint struct {
			Longitude float64 `json:"longitude"`
			Latitude  float64 `json:"latitude"`
			Geometry  struct {
				WKT string `json:"wkt"`
			} `json:"geometry"`
		} `json:"representativePoint"`
	} `json:"location"`
}

type MatchResult struct {
	Found         bool
	LightboxCity  *LightboxCity
	MatchType     string 
	Confidence    float64
	Error         string
}

type SearchParams struct {
	CityName string
	State    string
	County   string
	Limit    int
}