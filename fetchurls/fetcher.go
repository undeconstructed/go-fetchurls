package fetchurls

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

// FetchedData encapsulates a newly found data from a URL, or an error in accessing a URL.
type FetchedData struct {
	URL  string
	Time int64
	ETag string
	Data []byte
	Err  error
}

// fetchState is internal state for a URL
type fetchState struct {
	time int64
	etag string
}

// toFetch is the instruction format for workers
type toFetch struct {
	url  string
	etag string
}

// fromFetch is the response format from workers
type fromFetch struct {
	url  string
	etag string
	time int64
	data []byte
	err  error
}

// Fetcher fetches URLs about once a minute and sends you the data if it has changed
type Fetcher struct {
	out  chan FetchedData // return new data
	in   chan []string    // accept new url sets
	work chan toFetch     // push work to workers
}

// New creates a new Fetcher
func New(out chan FetchedData) (*Fetcher, error) {
	return &Fetcher{
		out:  out,
		in:   make(chan []string, 1),
		work: make(chan toFetch),
	}, nil
}

// Start begins the fetching process and must only be called once
func (f *Fetcher) Start(ctx context.Context) {

	resultCh := make(chan fromFetch)

	for i := 0; i < 2; i++ {
		go func() {
			for {
				select {
				case work := <-f.work:
					bytes, etag, err := doFetch(work.url, work.etag)
					if err != nil {
						resultCh <- fromFetch{url: work.url, err: err}
						continue
					}
					resultCh <- fromFetch{url: work.url, etag: etag, time: time.Now().Unix(), data: bytes}
				case <-ctx.Done():
					break
				}
			}
		}()
	}

	go func() {
		data := map[string]*fetchState{}
		// TODO: only sleep until next fetch is needed
		ticker := time.NewTicker(10 * time.Second)
	loop:
		for {
			select {
			case urls := <-f.in:
				newData := map[string]*fetchState{}
				for _, url := range urls {
					if existing, ok := data[url]; ok {
						newData[url] = existing
					} else {
						state := &fetchState{time: 0}
						newData[url] = state
						instruction := toFetch{url: url}
						select {
						case f.work <- instruction:
							// now in flight
							state.time = -1
						default:
							// needs to wait for a worker
						}
					}
				}
				data = newData
			case result := <-resultCh:
				if state, ok := data[result.url]; ok {
					if result.err != nil {
						state.time = time.Now().Unix()
						f.out <- FetchedData{URL: result.url, Time: state.time, Err: result.err}
					} else if state.etag == result.etag {
						state.time = result.time
					} else {
						state.time = result.time
						state.etag = result.etag
						f.out <- FetchedData{URL: result.url, Time: state.time, ETag: result.etag, Data: result.data}
					}
				} else {
					// this is no longer an interesting URL
				}
				f.tryDispatch(data)
			case <-ticker.C:
				f.tryDispatch(data)
			case <-ctx.Done():
				ticker.Stop()
				break loop
			}
		}
	}()
}

func (f *Fetcher) tryDispatch(data map[string]*fetchState) {
	for url, state := range data {
		cutoff := time.Now().Unix() - 60
		if state.time >= 0 && state.time <= cutoff {
			instruction := toFetch{url: url, etag: state.etag}
			select {
			case f.work <- instruction:
				// now in flight
				state.time = -1
			default:
				// no workers free
				return
			}
		}
	}
}

// SetURLs replaces the set of URLs the Fetcher is fetching
func (f *Fetcher) SetURLs(urls []string) {
	f.in <- urls
}

func doFetch(urlString, etag string) ([]byte, string, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, "", err
	}

	if u.Scheme == "http" || u.Scheme == "https" {
		req, _ := http.NewRequest("GET", urlString, nil)
		if etag != "" {
			req.Header.Add("If-None-Match", etag)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, "", fmt.Errorf("failed to fetch: %s", u)
		}
		defer resp.Body.Close()

		if resp.StatusCode == 304 {
			return nil, etag, nil
		}
		if resp.StatusCode != 200 {
			return nil, "", fmt.Errorf("failed to read: %s", u)
		}

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read: %s", u)
		}

		newEtag := resp.Header.Get("ETag")
		if newEtag == "" {
			// server not supplied anything, so must assume changed
			newEtag = strconv.FormatInt(time.Now().Unix(), 10)
		}

		return b, newEtag, nil
	} else if u.Scheme == "file" {
		fileinfo, err := os.Stat(u.Path)
		if err != nil {
			return nil, "", err
		}

		timestamp := strconv.FormatInt(fileinfo.ModTime().Unix(), 10)
		if timestamp == etag {
			return nil, etag, nil
		}

		b, err := ioutil.ReadFile(u.Path)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read: %s", u)
		}
		return b, timestamp, nil
	} else {
		return nil, "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
}
