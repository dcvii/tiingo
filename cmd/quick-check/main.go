package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"
)

const (
	colorGreen  = "\033[0;32m"
	colorRed    = "\033[0;31m"
	colorYellow = "\033[1;33m"
	colorReset  = "\033[0m"
)

type CheckResult struct {
	name    string
	passed  bool
	message string
}

func main() {
	fmt.Println("Quick Kafka Check")
	fmt.Println("=================")
	fmt.Println()

	checks := []CheckResult{
		checkGoldServer(),
		checkKafkaPort(),
		checkAPIKey(),
		checkProducerBinary(),
		checkConsumerBinary(),
	}

	allPassed := true
	for _, check := range checks {
		printCheck(check)
		if !check.passed {
			allPassed = false
		}
	}

	if !allPassed {
		os.Exit(1)
	}

	fmt.Println()
	fmt.Printf("%s✓ Ready to test!%s\n", colorGreen, colorReset)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Build binaries (if needed):")
	fmt.Println("     go build -o tiingo ./cmd/tiingo")
	fmt.Println("     go build -o consumer ./cmd/consumer")
	fmt.Println()
	fmt.Println("  2. Run end-to-end test:")
	fmt.Println("     ./scripts/test-kafka-e2e.sh")
}

func checkGoldServer() CheckResult {
	cmd := exec.Command("ping", "-c", "1", "-W", "2", "gold")
	err := cmd.Run()
	return CheckResult{
		name:   "Gold server (ping).......",
		passed: err == nil,
	}
}

func checkKafkaPort() CheckResult {
	conn, err := net.DialTimeout("tcp", "gold:9092", 5*time.Second)
	if err == nil {
		conn.Close()
	}
	return CheckResult{
		name:   "Kafka port (9092)........",
		passed: err == nil,
	}
}

func checkAPIKey() CheckResult {
	apiKey := os.Getenv("TIINGO_API_KEY")
	result := CheckResult{
		name:   "TIINGO_API_KEY...........",
		passed: apiKey != "",
	}
	if !result.passed {
		result.message = " (run: direnv allow)"
	}
	return result
}

func checkProducerBinary() CheckResult {
	_, err := os.Stat("./tiingo")
	result := CheckResult{
		name:   "Producer binary..........",
		passed: err == nil,
	}
	if !result.passed {
		result.message = " (run: go build -o tiingo ./cmd/tiingo)"
	}
	return result
}

func checkConsumerBinary() CheckResult {
	_, err := os.Stat("./consumer")
	result := CheckResult{
		name:   "Consumer binary..........",
		passed: err == nil,
	}
	if !result.passed {
		result.message = " (run: go build -o consumer ./cmd/consumer)"
	}
	return result
}

func printCheck(check CheckResult) {
	fmt.Print(check.name + " ")
	if check.passed {
		fmt.Printf("%s✓%s\n", colorGreen, colorReset)
	} else {
		fmt.Printf("%s✗%s", colorRed, colorReset)
		if check.message != "" {
			fmt.Print(check.message)
		}
		fmt.Println()
	}
}
