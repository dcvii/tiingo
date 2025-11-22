package portfolio

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// Reader handles parsing of portfolio files
type Reader struct{}

// NewReader creates a new portfolio reader
func NewReader() *Reader {
	return &Reader{}
}

// ReadPortfolio reads and parses a portfolio file containing ticker symbols
// Expected format: one ticker per line, ignores comments (#) and blank lines
func (r *Reader) ReadPortfolio(filePath string) ([]string, error) {
	start := time.Now()
	defer func() {
		log.Printf("⏱️  Portfolio read took: %v", time.Since(start))
	}()

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening portfolio file: %w", err)
	}
	defer file.Close()

	var tickers []string
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Extract ticker (everything before a comment if present)
		if idx := strings.Index(line, "#"); idx != -1 {
			line = strings.TrimSpace(line[:idx])
		}

		// Validate ticker format
		ticker := strings.ToUpper(line)
		if !r.ValidateTicker(ticker) {
			return nil, fmt.Errorf("invalid ticker format on line %d: %s", lineNum, line)
		}

		tickers = append(tickers, ticker)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading portfolio file: %w", err)
	}

	if len(tickers) == 0 {
		return nil, fmt.Errorf("no tickers found in portfolio file")
	}

	return tickers, nil
}

// ValidateTicker performs basic validation on ticker format
// Allows alphanumeric characters, slashes (for BRK/B), dots, and hyphens
func (r *Reader) ValidateTicker(ticker string) bool {
	if len(ticker) == 0 || len(ticker) > 10 {
		return false
	}

	for _, ch := range ticker {
		if !((ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '/' || ch == '.' || ch == '-') {
			return false
		}
	}

	return true
}
