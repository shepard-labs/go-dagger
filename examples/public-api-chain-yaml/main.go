package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shepard-labs/go-dagger/pkg/dag"
	"github.com/shepard-labs/go-dagger/pkg/orchestrator"
	"github.com/shepard-labs/go-dagger/pkg/task"
)

type RunState struct {
	IPAddress       string  `json:"ip_address,omitempty"`
	CountryCode     string  `json:"country_code,omitempty"`
	CountryName     string  `json:"country_name,omitempty"`
	Capital         string  `json:"capital,omitempty"`
	Population      int64   `json:"population,omitempty"`
	Region          string  `json:"region,omitempty"`
	Latitude        float64 `json:"latitude,omitempty"`
	Longitude       float64 `json:"longitude,omitempty"`
	TemperatureC    float64 `json:"temperature_c,omitempty"`
	WindSpeedKPH    float64 `json:"wind_speed_kph,omitempty"`
	WeatherTime     string  `json:"weather_time,omitempty"`
	Sunrise         string  `json:"sunrise,omitempty"`
	Sunset          string  `json:"sunset,omitempty"`
	Summary         string  `json:"summary,omitempty"`
	LocationAPIURL  string  `json:"location_api_url,omitempty"`
	CountryAPIURL   string  `json:"country_api_url,omitempty"`
	WeatherAPIURL   string  `json:"weather_api_url,omitempty"`
	AstronomyAPIURL string  `json:"astronomy_api_url,omitempty"`
}

type ipAPIResponse struct {
	IP          string `json:"ip"`
	Country     string `json:"country"`
	CountryName string `json:"country_name"`
}

type restCountryWireResponse struct {
	Name struct {
		Common string `json:"common"`
	} `json:"name"`
	Capital     []string `json:"capital"`
	CapitalInfo struct {
		LatLng []float64 `json:"latlng"`
	} `json:"capitalInfo"`
	Region     string `json:"region"`
	Population int64  `json:"population"`
}

type openMeteoResponse struct {
	Latitude       float64 `json:"latitude"`
	Longitude      float64 `json:"longitude"`
	CurrentWeather struct {
		Temperature float64 `json:"temperature"`
		WindSpeed   float64 `json:"windspeed"`
		Time        string  `json:"time"`
	} `json:"current_weather"`
}

type sunriseSunsetResponse struct {
	Results struct {
		Sunrise string `json:"sunrise"`
		Sunset  string `json:"sunset"`
	} `json:"results"`
	Status string `json:"status"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../../.env")
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}

	data, err := os.ReadFile(examplePath("dag.yaml"))
	if err != nil {
		return err
	}
	d, err := dag.ParseYAML(data, functions(), nil, nil)
	if err != nil {
		return err
	}

	orch, err := orchestrator.NewOrchestrator[RunState](ctx, orchestrator.Config{PostgresDSN: dsn, GlobalTimeout: 2 * time.Minute})
	if err != nil {
		return err
	}
	defer func() { _ = orch.Close() }()

	fmt.Println("loaded YAML DAG", d.Name, "with concurrency limit", d.ConcurrencyLimit)
	run, err := orch.Run(ctx, d)
	if err != nil {
		return err
	}
	fmt.Println("run", run.ID, "finished for", d.Name)
	return nil
}

func functions() task.FunctionRegistry[RunState] {
	return task.FunctionRegistry[RunState]{
		"examples.api_chain.lookup_current_location": lookupCurrentLocation,
		"examples.api_chain.fetch_country_profile":   fetchCountryProfile,
		"examples.api_chain.fetch_capital_weather":   fetchCapitalWeather,
		"examples.api_chain.fetch_sunrise_sunset":    fetchSunriseSunset,
		"examples.api_chain.summarize_api_chain":     summarizeAPIChain,
	}
}

func lookupCurrentLocation(ctx context.Context, state *RunState) (*RunState, error) {
	state.LocationAPIURL = "https://ipapi.co/json/"
	logStep(ctx, "GET %s", state.LocationAPIURL)
	var response ipAPIResponse
	if err := getJSON(ctx, state.LocationAPIURL, &response); err != nil {
		return state, err
	}
	if response.Country == "" {
		return state, fmt.Errorf("location API returned no country code")
	}
	state.IPAddress = response.IP
	state.CountryCode = strings.ToUpper(response.Country)
	state.CountryName = response.CountryName
	logStep(ctx, "country_code=%s country_name=%s ip=%s", state.CountryCode, state.CountryName, state.IPAddress)
	return state, nil
}

func fetchCountryProfile(ctx context.Context, state *RunState) (*RunState, error) {
	if state.CountryCode == "" {
		return state, fmt.Errorf("country code is required from lookup-current-location")
	}
	state.CountryAPIURL = fmt.Sprintf("https://restcountries.com/v3.1/alpha/%s", url.PathEscape(state.CountryCode))
	logStep(ctx, "GET %s using country_code=%s", state.CountryAPIURL, state.CountryCode)
	var response []restCountryWireResponse
	if err := getJSON(ctx, state.CountryAPIURL, &response); err != nil {
		return state, err
	}
	if len(response) == 0 {
		return state, fmt.Errorf("country API returned no records for %s", state.CountryCode)
	}
	country := response[0]
	state.CountryName = country.Name.Common
	state.Region = country.Region
	state.Population = country.Population
	if len(country.Capital) > 0 {
		state.Capital = country.Capital[0]
	}
	if len(country.CapitalInfo.LatLng) >= 2 {
		state.Latitude = country.CapitalInfo.LatLng[0]
		state.Longitude = country.CapitalInfo.LatLng[1]
	}
	if state.Capital == "" || state.Latitude == 0 && state.Longitude == 0 {
		return state, fmt.Errorf("country API did not return capital coordinates for %s", state.CountryCode)
	}
	logStep(ctx, "capital=%s lat=%.4f lon=%.4f population=%d", state.Capital, state.Latitude, state.Longitude, state.Population)
	return state, nil
}

func fetchCapitalWeather(ctx context.Context, state *RunState) (*RunState, error) {
	if state.Latitude == 0 && state.Longitude == 0 {
		return state, fmt.Errorf("capital coordinates are required from fetch-country-profile")
	}
	values := url.Values{}
	values.Set("latitude", fmt.Sprintf("%.6f", state.Latitude))
	values.Set("longitude", fmt.Sprintf("%.6f", state.Longitude))
	values.Set("current_weather", "true")
	state.WeatherAPIURL = "https://api.open-meteo.com/v1/forecast?" + values.Encode()
	logStep(ctx, "GET %s using capital=%s", state.WeatherAPIURL, state.Capital)
	var response openMeteoResponse
	if err := getJSON(ctx, state.WeatherAPIURL, &response); err != nil {
		return state, err
	}
	state.TemperatureC = response.CurrentWeather.Temperature
	state.WindSpeedKPH = response.CurrentWeather.WindSpeed
	state.WeatherTime = response.CurrentWeather.Time
	logStep(ctx, "temperature=%.1fC wind=%.1fkm/h observed_at=%s", state.TemperatureC, state.WindSpeedKPH, state.WeatherTime)
	return state, nil
}

func fetchSunriseSunset(ctx context.Context, state *RunState) (*RunState, error) {
	if state.WeatherTime == "" {
		return state, fmt.Errorf("weather output is required from fetch-capital-weather")
	}
	values := url.Values{}
	values.Set("lat", fmt.Sprintf("%.6f", state.Latitude))
	values.Set("lng", fmt.Sprintf("%.6f", state.Longitude))
	values.Set("formatted", "0")
	state.AstronomyAPIURL = "https://api.sunrise-sunset.org/json?" + values.Encode()
	logStep(ctx, "GET %s using weather_time=%s", state.AstronomyAPIURL, state.WeatherTime)
	var response sunriseSunsetResponse
	if err := getJSON(ctx, state.AstronomyAPIURL, &response); err != nil {
		return state, err
	}
	if response.Status != "OK" {
		return state, fmt.Errorf("sunrise-sunset API returned status %q", response.Status)
	}
	state.Sunrise = response.Results.Sunrise
	state.Sunset = response.Results.Sunset
	logStep(ctx, "sunrise=%s sunset=%s", state.Sunrise, state.Sunset)
	return state, nil
}

func summarizeAPIChain(ctx context.Context, state *RunState) (*RunState, error) {
	if state.Sunrise == "" || state.Sunset == "" {
		return state, fmt.Errorf("sunrise/sunset output is required from fetch-sunrise-sunset")
	}
	state.Summary = fmt.Sprintf(
		"%s, %s is in %s. Current weather is %.1fC with %.1f km/h wind. Sunrise is %s and sunset is %s.",
		state.Capital,
		state.CountryName,
		state.Region,
		state.TemperatureC,
		state.WindSpeedKPH,
		state.Sunrise,
		state.Sunset,
	)
	logStep(ctx, "summary=%s", state.Summary)
	return state, nil
}

func getJSON(ctx context.Context, requestURL string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("User-Agent", "go-dagger-example/1.0")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("GET %s returned %s: %s", requestURL, response.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(response.Body).Decode(target)
}

func logStep(ctx context.Context, format string, args ...any) {
	orchestrator.LoggerFromContext(ctx).Info(fmt.Sprintf(format, args...))
}

func examplePath(name string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return name
	}
	return filepath.Join(filepath.Dir(file), name)
}
