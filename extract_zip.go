package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const outputFile = "zip_full_urls.txt"

var (
	writer     *bufio.Writer
	output     *os.File
	matches    int64
	totalFiles int
	processed  int64
)

func main() {
	indexDir := `D:\Code\cc-downloader-v0.6.1-x86_64-pc-windows-msvc\CrawlIndex\Index\cc-index\collections\CC-MAIN-2013-20\indexes`

	fmt.Println("=== Common Crawl .zip URL Extractor (JSON CDX Support) ===")
	fmt.Printf("Scanning folder: %s\n\n", indexDir)

	var err error
	output, err = os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer output.Close()

	writer = bufio.NewWriterSize(output, 16*1024*1024)
	defer writer.Flush()

	// Graceful Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n\nReceived interrupt. Flushing data...")
		shutdown()
		os.Exit(0)
	}()

	files, err := filepath.Glob(filepath.Join(indexDir, "*.gz"))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("No .gz files found!")
		return
	}

	totalFiles = len(files)
	fmt.Printf("Found %d index files.\n\n", totalFiles)

	startTime := time.Now()

	for i, file := range files {
		filename := filepath.Base(file)
		fmt.Printf("\n[%d/%d] Starting archive: %s\n", i+1, totalFiles, filename)

		totalLines := countLines(file)
		fmt.Printf("   Total entries: %d\n", totalLines)

		count := processFileWithProgress(file, totalLines)

		atomic.AddInt64(&matches, int64(count))
		atomic.AddInt64(&processed, 1)

		fmt.Printf("   Finished archive: %s → Found %d .zip files\n", filename, count)
	}

	shutdown()
	elapsed := time.Since(startTime).Round(time.Second)

	fmt.Printf("\n✅ All archives processed successfully!\n")
	fmt.Printf("Total .zip URLs found: %d\n", matches)
	fmt.Printf("Total time: %s\n", elapsed)
	fmt.Printf("Results saved to: %s\n", outputFile)
}

func countLines(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		return 0
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return 0
	}
	defer gz.Close()

	count := 0
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		count++
	}
	return count
}

// Extract URL from JSON CDX line
func extractURL(line string) string {
	// Split into at most 3 parts: prefix + timestamp + json
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return ""
	}

	jsonStr := parts[2]

	// Parse the JSON to get the "url" field
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return ""
	}

	if url, ok := data["url"].(string); ok {
		return url
	}
	return ""
}

// Improved .zip detection (removes query string, fragment, trailing slashes)
func iszipURL(rawURL string) bool {
	if rawURL == "" {
		return false
	}

	url := rawURL

	// Remove query string and fragment
	if idx := strings.IndexAny(url, "?#"); idx != -1 {
		url = url[:idx]
	}

	// Trim trailing slashes and whitespace
	url = strings.TrimRight(url, "/ \t")

	return strings.HasSuffix(strings.ToLower(url), ".zip")
}

func processFileWithProgress(filename string, totalLines int) int {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("   Error opening file: %v\n", err)
		return 0
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		fmt.Printf("   Error reading gzip: %v\n", err)
		return 0
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	processedLines := 0
	matchesInFile := 0
	lastUpdate := time.Now()

	for scanner.Scan() {
		url := extractURL(scanner.Text())

		if iszipURL(url) {
			writer.WriteString(url)
			writer.WriteByte('\n')
			matchesInFile++
		}

		processedLines++

		// Progress update every 3 seconds
		if time.Since(lastUpdate) > 3*time.Second {
			percent := float64(processedLines) / float64(totalLines) * 100
			fmt.Printf("   Progress: %.1f%% (%d/%d lines) | %d .zip so far\n",
				percent, processedLines, totalLines, matchesInFile)
			writer.Flush()
			lastUpdate = time.Now()
		}
	}

	writer.Flush()
	return matchesInFile
}

func shutdown() {
	if writer != nil {
		writer.Flush()
	}
	if output != nil {
		output.Sync()
	}
}