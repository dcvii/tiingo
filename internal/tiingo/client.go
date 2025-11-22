package tiingo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/mdcb/tiingo-tracker/pkg/models"
	"golang.org/x/time/rate"
)

const (
	baseURL        = "https://api.tiingo.com"
	cryptoBaseURL  = "https://api.tiingo.com/tiingo/crypto"
	defaultTimeout = 30 * time.Second
)

// Client wraps the Tiingo API with rate limiting and error handling
type Client struct {
	baseURL      string
	apiKey       string
	httpClient   *http.Client
	limiter      *rate.Limiter
	requestCount atomic.Int64
}

// NewClient creates a new Tiingo API client
// Rate limit: 10,000 requests/hour = ~166 requests/minute = ~2.77 requests per second
func NewClient(apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		// Allow 2.77 requests per second (10,000/hour) with burst of 50
		limiter: rate.NewLimiter(rate.Every(360*time.Millisecond), 50),
	}
}

// doRequest performs an HTTP GET request with rate limiting
func (c *Client) doRequest(ctx context.Context, url string) ([]byte, error) {
	// Wait for rate limiter
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter error: %w", err)
	}

	// Increment request counter
	c.requestCount.Add(1)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Token "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
	}

	return body, nil
}

// GetTickerMetadata fetches metadata for a ticker symbol
func (c *Client) GetTickerMetadata(ctx context.Context, ticker string) (*models.TickerMetadata, error) {
	url := fmt.Sprintf("%s/tiingo/daily/%s", c.baseURL, ticker)
	
	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata for %s: %w", ticker, err)
	}

	var metadata models.TickerMetadata
	if err := json.Unmarshal(body, &metadata); err != nil {
		return nil, fmt.Errorf("parsing metadata for %s: %w", ticker, err)
	}

	return &metadata, nil
}

// GetDailyPrices fetches historical daily prices for a ticker
func (c *Client) GetDailyPrices(ctx context.Context, ticker string, startDate, endDate time.Time) ([]models.DailyPrice, error) {
	url := fmt.Sprintf("%s/tiingo/daily/%s/prices?startDate=%s&endDate=%s",
		c.baseURL,
		ticker,
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
	)

	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching daily prices for %s: %w", ticker, err)
	}

	var prices []models.DailyPrice
	if err := json.Unmarshal(body, &prices); err != nil {
		return nil, fmt.Errorf("parsing daily prices for %s: %w", ticker, err)
	}

	return prices, nil
}

// GetCurrentQuote fetches the latest quote for a ticker
func (c *Client) GetCurrentQuote(ctx context.Context, ticker string) (*models.IEXQuote, error) {
	url := fmt.Sprintf("%s/iex/%s", c.baseURL, ticker)

	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching quote for %s: %w", ticker, err)
	}

	var quotes []models.IEXQuote
	if err := json.Unmarshal(body, &quotes); err != nil {
		return nil, fmt.Errorf("parsing quote for %s: %w", ticker, err)
	}

	if len(quotes) == 0 {
		return nil, fmt.Errorf("no quote data for %s", ticker)
	}

	return &quotes[0], nil
}

// GetCryptoPrices fetches cryptocurrency prices
func (c *Client) GetCryptoPrices(ctx context.Context, ticker string, startDate, endDate time.Time) ([]models.CryptoPrice, error) {
	url := fmt.Sprintf("%s/prices?tickers=%s&startDate=%s&endDate=%s&resampleFreq=1day",
		cryptoBaseURL,
		ticker,
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
	)

	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching crypto prices for %s: %w", ticker, err)
	}

	var response []struct {
		Ticker    string                `json:"ticker"`
		PriceData []models.CryptoPrice `json:"priceData"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("parsing crypto prices for %s: %w", ticker, err)
	}

	if len(response) == 0 || len(response[0].PriceData) == 0 {
		return nil, fmt.Errorf("no crypto price data for %s", ticker)
	}

	return response[0].PriceData, nil
}

// ValidateTicker checks if a ticker exists and is valid
func (c *Client) ValidateTicker(ctx context.Context, ticker string) (bool, error) {
	_, err := c.GetTickerMetadata(ctx, ticker)
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetRequestCount returns the total number of API requests made
func (c *Client) GetRequestCount() int64 {
	return c.requestCount.Load()
}
