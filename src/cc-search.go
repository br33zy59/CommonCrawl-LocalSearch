package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Buffer between the raw file and gzip.Reader to cut syscalls when reading compressed streams.
const gzipReadBufSize = 2 * 1024 * 1024 // >= 1 MiB

func defaultWorkerCount() int {
	n := runtime.NumCPU()
	w := (n * 3) / 4
	if w < 1 {
		return 1
	}
	return w
}

var (
	writer     *bufio.Writer
	output     *os.File
	matches    int64
	mu         sync.Mutex // Protect writes to the file
	outputPath string
)

func main() {
	patternFlag := flag.String("pattern", "", "Substring to match in each URL (required), e.g. .zip")
	indexDirFlag := flag.String("indexdir", "", "Folder containing gzipped Common Crawl index files (required)")
	modeFlag := flag.String("mode", "suffix", "Match mode: suffix (URL ends with pattern) or contains (pattern anywhere)")
	stripQueryFlag := flag.Bool("stripquery", true, "If true, cut URL at first ? or # before matching")
	workersFlag := flag.Int("workers", defaultWorkerCount(), "Max concurrent index .gz files to process (default: 3/4 of logical CPUs)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Extract URLs from Common Crawl index .gz files that match a pattern.\n\n")
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n  %s -pattern=.zip -indexdir=Crawl_Data\\CC-MAIN-2013-20\\indexes -mode=suffix -stripquery=true\n", filepath.Base(os.Args[0]))
	}
	flag.Parse()

	if strings.TrimSpace(*patternFlag) == "" || strings.TrimSpace(*indexDirFlag) == "" {
		flag.Usage()
		os.Exit(2)
	}

	modeStr := strings.ToLower(strings.TrimSpace(*modeFlag))
	if modeStr != "suffix" && modeStr != "contains" {
		fmt.Fprintf(os.Stderr, "%s: -mode must be suffix or contains\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}

	numWorkers := *workersFlag
	if numWorkers < 1 {
		fmt.Fprintf(os.Stderr, "%s: -workers must be >= 1\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}

	pattern := strings.TrimSpace(*patternFlag)
	patternLower := strings.ToLower(pattern)
	indexDir := resolveIndexDir(*indexDirFlag)
	stripQuery := *stripQueryFlag

	outputPath = defaultOutputPath(pattern)

	fmt.Println("=== Fast Common Crawl URL pattern extractor ===")
	fmt.Printf("Pattern: %q (match: %s, stripquery: %v)\n", pattern, modeStr, stripQuery)
	fmt.Printf("Scanning folder: %s\n", indexDir)
	fmt.Printf("Workers: %d (logical CPUs: %d)\n", numWorkers, runtime.NumCPU())
	fmt.Printf("Output file: %s\n\n", outputPath)

	var err error
	output, err = os.Create(outputPath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer output.Close()

	writer = bufio.NewWriterSize(output, 32*1024*1024) // Large buffer
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

	fmt.Printf("Found %d index files. Starting parallel processing...\n\n", len(files))

	startTime := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, numWorkers)

	for i, file := range files {
		wg.Add(1)
		sem <- struct{}{}

		go func(file string, idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			filename := filepath.Base(file)
			fmt.Printf("[%d/%d] Starting: %s\n", idx+1, len(files), filename)

			countInFile := processFile(file, patternLower, modeStr, stripQuery)

			atomic.AddInt64(&matches, int64(countInFile))

			mu.Lock()
			writer.Flush() // Periodic flush
			mu.Unlock()

			fmt.Printf("✓ Finished %s → %d matches | Running total: %d\n", filename, countInFile, atomic.LoadInt64(&matches))
		}(file, i)
	}

	wg.Wait()
	shutdown()

	elapsed := time.Since(startTime).Round(time.Second)
	fmt.Printf("\nAll done!\n")
	fmt.Printf("Total matching URLs: %d\n", matches)
	fmt.Printf("Total time: %s\n", elapsed)
	fmt.Printf("Results saved to: %s\n", outputPath)
}

func resolveIndexDir(raw string) string {
	p := filepath.Clean(strings.TrimSpace(raw))
	if filepath.IsAbs(p) {
		return p
	}
	exeDir, err := os.Executable()
	if err != nil {
		return p // relative to current working directory
	}
	baseDir := filepath.Dir(exeDir)
	return filepath.Join(baseDir, p)
}

func defaultOutputPath(pattern string) string {
	base := strings.TrimPrefix(strings.TrimSpace(strings.ToLower(pattern)), ".")
	base = strings.TrimSpace(base)
	if base == "" {
		base = "matches"
	}
	for _, r := range `\/:*?"<>|` {
		base = strings.ReplaceAll(base, string(r), "_")
	}
	return "crawl_" + base + "_urls.txt"
}

func processFile(filename string, patternLower string, mode string, stripQuery bool) int {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("   Error opening %s: %v\n", filename, err)
		return 0
	}
	defer file.Close()

	br := bufio.NewReaderSize(file, gzipReadBufSize)
	gz, err := gzip.NewReader(br)
	if err != nil {
		fmt.Printf("   Error decompressing %s: %v\n", filename, err)
		return 0
	}
	defer gz.Close()

	// Large buffer for better performance
	scanner := bufio.NewScanner(gz)
	buf := make([]byte, 2*1024*1024) // 2MB buffer
	scanner.Buffer(buf, 2*1024*1024)

	count := 0

	for scanner.Scan() {
		url := extractURLFast(scanner.Text())
		if urlMatches(url, patternLower, mode, stripQuery) {
			mu.Lock()
			writer.WriteString(url)
			writer.WriteByte('\n')
			mu.Unlock()
			count++
		}
	}

	return count
}

// Fast URL extraction - string search first, JSON only as fallback
func extractURLFast(line string) string {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return ""
	}

	jsonStr := parts[2]

	// Very fast path
	if idx := strings.Index(jsonStr, `"url":"`); idx != -1 {
		start := idx + 7
		if end := strings.IndexByte(jsonStr[start:], '"'); end != -1 {
			return jsonStr[start : start+end]
		}
	}

	// Fallback
	var data map[string]interface{}
	if json.Unmarshal([]byte(jsonStr), &data) == nil {
		if url, ok := data["url"].(string); ok {
			return url
		}
	}
	return ""
}

func urlMatches(rawURL string, patternLower string, mode string, stripQuery bool) bool {
	if rawURL == "" || patternLower == "" {
		return false
	}
	url := rawURL
	if stripQuery {
		if idx := strings.IndexAny(url, "?#"); idx != -1 {
			url = url[:idx]
		}
	}
	url = strings.TrimRight(url, "/ \t")
	urlLower := strings.ToLower(url)
	switch mode {
	case "suffix":
		return strings.HasSuffix(urlLower, patternLower)
	case "contains":
		return strings.Contains(urlLower, patternLower)
	default:
		return false
	}
}

func shutdown() {
	mu.Lock()
	defer mu.Unlock()
	if writer != nil {
		writer.Flush()
	}
	if output != nil {
		output.Sync()
	}
}
