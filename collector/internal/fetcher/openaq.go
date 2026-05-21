package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"pmnt_lab14/collector/internal/schema"
)

const baseURL = "https://api.openaq.org/v3"

// locationDelay is the inter-request pause to stay within OpenAQ rate limits.
const locationDelay = 150 * time.Millisecond

type locationsResponse struct {
	Results []struct {
		ID      int64  `json:"id"`
		Name    string `json:"name"`
		City    string `json:"city"`
		Country struct {
			Code string `json:"code"`
		} `json:"country"`
		Coordinates struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"coordinates"`
	} `json:"results"`
}

type latestResponse struct {
	Results []struct {
		Datetime struct {
			UTC string `json:"utc"`
		} `json:"datetime"`
		Parameter struct {
			Name  string `json:"name"`
			Units string `json:"units"`
		} `json:"parameter"`
		Value float64 `json:"value"`
	} `json:"results"`
}

// Client fetches air quality data from the OpenAQ v3 API.
type Client struct {
	http   *http.Client
	apiKey string
}

func NewClient(apiKey string) *Client {
	return &Client{
		http:   &http.Client{Timeout: 30 * time.Second},
		apiKey: apiKey,
	}
}

func (c *Client) get(ctx context.Context, url string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// FetchMeasurements fetches the latest measurements for all locations in the given country.
func (c *Client) FetchMeasurements(ctx context.Context, countryCode string, collectorID string) ([]schema.Measurement, error) {
	url := fmt.Sprintf("%s/locations?countries_id=%s&limit=50&page=1", baseURL, countryCode)
	var locResp locationsResponse
	if err := c.get(ctx, url, &locResp); err != nil {
		return nil, fmt.Errorf("fetch locations %s: %w", countryCode, err)
	}

	var measurements []schema.Measurement
	for _, loc := range locResp.Results {
		latestURL := fmt.Sprintf("%s/locations/%d/latest", baseURL, loc.ID)
		var latest latestResponse
		if err := c.get(ctx, latestURL, &latest); err != nil {
			continue
		}
		for _, s := range latest.Results {
			ts, err := time.Parse(time.RFC3339, s.Datetime.UTC)
			if err != nil {
				ts = time.Now().UTC()
			}
			measurements = append(measurements, schema.Measurement{
				LocationID:   loc.ID,
				LocationName: loc.Name,
				CountryCode:  loc.Country.Code,
				City:         loc.City,
				Latitude:     loc.Coordinates.Latitude,
				Longitude:    loc.Coordinates.Longitude,
				Parameter:    s.Parameter.Name,
				Value:        s.Value,
				Unit:         s.Parameter.Units,
				Timestamp:    ts,
				CollectorID:  collectorID,
			})
		}
		select {
		case <-ctx.Done():
			return measurements, ctx.Err()
		case <-time.After(locationDelay):
		}
	}
	return measurements, nil
}
