package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"log"
	"errors"
	"sync"
	"time"
	"flag"
	"net/url"
	"strings"
)

func connect(ctx context.Context, scheme, url string) (net.Conn, error) {
	var dialer net.Dialer
	if scheme == "https" {
		tlsDialer := tls.Dialer{NetDialer: &dialer}
		return tlsDialer.DialContext(ctx, "tcp", url)
	}

	return dialer.DialContext(ctx, "tcp", url)
}

func request(ctx context.Context, conn net.Conn, path string, host string) ([]byte, error) {
	httpRequest := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Connection: close\r\n" +
		"\r\n"

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}

	if _, err := conn.Write([]byte(httpRequest)); err != nil {
		return nil, err
	}

	httpResponse, err := io.ReadAll(conn)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	return httpResponse, nil
}

func prepareUrl(rawUrl string) (string, string, string, string, error) {
	if !strings.HasPrefix(rawUrl, "http://") && !strings.HasPrefix(rawUrl, "https://") {
		rawUrl = "https://" + rawUrl
	}

	parsedUrl, err := url.Parse(rawUrl)
	if err != nil {
		return "", "", "", "", err
	}

	path := parsedUrl.RequestURI()
	if path == "" {
		path = "/"
	}

	scheme := parsedUrl.Scheme

	host := parsedUrl.Hostname()

	port := parsedUrl.Port()
	if port == "" {
		if parsedUrl.Scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	}

	return path, scheme, host, port, nil
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

	path, scheme, host, port, err := prepareUrl(rawUrl)
	if err != nil {
		select {
			case sendCh <- RequestResult{Err: fmt.Errorf("Неверный URL '%s': %w", rawUrl, err)}:
			case <-ctx.Done():
		}
		return
	}

	url := net.JoinHostPort(host, port)

	conn, err := connect(ctx, scheme, url)
	if err != nil {
		select {
			case sendCh <- RequestResult{Err: fmt.Errorf("Ошибка подключения к '%s': %w", rawUrl, err)}:
			case <-ctx.Done():
		}
		return
	}
	defer conn.Close()

	resp, err := request(ctx, conn, path, host)
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

var ErrNoURLsProvided = errors.New("at least one URL must be specified")

func parse() (Config, error) {
	var timeoutSec int

	flag.IntVar(&timeoutSec, "t", 15, "timeout for all http requests in seconds")
	flag.IntVar(&timeoutSec, "timeout", 15, "timeout for all http requests in seconds")

	flag.Parse()

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
		if errors.Is(err, ErrNoURLsProvided) {
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
