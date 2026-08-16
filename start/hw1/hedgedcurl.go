package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"log"
	"errors"
	"sync"
	"time"
	"flag"
	"strings"
)

func request(ctx context.Context, rawUrl string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawUrl, nil)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := fmt.Sprintf(
		"HTTP/%d.%d %s\r\n%s\r\n%s",
		resp.ProtoMajor,
		resp.ProtoMinor,
		resp.Status,
		resp.Header,
		body,
	)

	return []byte(response), nil
}

type RequestResult struct {
	Body 	 string
	Err      error
}

func sendRequestToUrl(ctx context.Context, rawUrl string, sendCh chan<- RequestResult, wg *sync.WaitGroup) {
	defer wg.Done()

	select {
	case <-ctx.Done():
		return
	default:
	}

	resp, err := request(ctx, rawUrl)
	if err != nil {
		select {
			case sendCh <- RequestResult{Err: fmt.Errorf("Ошибка запроса к '%s': %w", rawUrl, err)}:
			case <-ctx.Done():
		}
		return
	}

	select {
		case sendCh <- RequestResult{Body: string(resp)}:
		case <-ctx.Done():
	}
}

func sendRequests(config Config) error {
	sendCh := make(chan RequestResult, len(config.Urls))
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	var wg sync.WaitGroup

	for _, url := range config.Urls {
		wg.Add(1)
		go sendRequestToUrl(ctx, url, sendCh, &wg)
	}

	go func() {
		wg.Wait()
		close(sendCh)
	}()

	var errors []string

	for result := range sendCh {
		if result.Err != nil {
			errors = append(errors, result.Err.Error())
			continue
		}

		cancel()
		fmt.Println(result.Body)
		return nil
	}

	if ctx.Err() != nil {
		return fmt.Errorf("Timeout после %s: %w", config.Timeout, ctx.Err())
	}

	return fmt.Errorf("Все запросы завершились с ошибками:\n%s", strings.Join(errors, "\n"))
}

type Config struct {
	Urls    []string
	Timeout time.Duration
}

var ErrInvalidTimeout = errors.New("timeout must be greater than 0")
var ErrNoURLsProvided = errors.New("at least one URL must be specified")

func parse() (Config, error) {
	var timeoutSec int

	flag.IntVar(&timeoutSec, "t", 15, "timeout for all http requests in seconds")
	flag.IntVar(&timeoutSec, "timeout", 15, "timeout for all http requests in seconds")

	flag.Parse()

	if timeoutSec <= 0 {
		return Config{}, fmt.Errorf("Invalid command line arguments: %w", ErrInvalidTimeout)
	}

	urls := flag.Args()

	if len(urls) == 0 {
		return Config{}, fmt.Errorf("Invalid command line arguments: %w", ErrNoURLsProvided)
	}

	return Config{
		Urls:    urls,
		Timeout: time.Duration(timeoutSec) * time.Second,
	}, nil
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "hedgedcurl — CLI утилита многопоточного curl'a с хеджированием\n\n")
		fmt.Fprintf(os.Stderr, "Использование:\n")
		fmt.Fprintf(os.Stderr, "  ./hedgedcurl [флаги] url [url...]\n\n")
		fmt.Fprintf(os.Stderr, "Флаги:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nПримеры:\n")
		fmt.Fprintf(os.Stderr, "  ./hedgedcurl https://example.com\n")
		fmt.Fprintf(os.Stderr, "  ./hedgedcurl -t 5 https://a.com https://b.com\n")
		fmt.Fprintf(os.Stderr, "  ./hedgedcurl -h\n")
	}
}

func main() {
	config, err := parse()
	if err != nil {
		if errors.Is(err, ErrInvalidTimeout) || errors.Is(err, ErrNoURLsProvided) {
			log.Printf("Configuration error: %v", err)
			flag.Usage()
			os.Exit(1)
		}
		
		log.Fatalf("Critical initialization error: %v", err)
	}

	if err := sendRequests(config); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Printf("Timeout error: %v", err)
			os.Exit(228)
		}
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
