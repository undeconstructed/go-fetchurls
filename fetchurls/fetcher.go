package fetchurls

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	// ErrCancelled is the fetcher was shut down
	ErrCancelled = errors.New("cancelled")
	// ErrFailed is the URL could not be fetched
	ErrFailed = errors.New("failed_to_read")
	// ErrUnsupported is the URL scheme is not supported
	ErrUnsupported = errors.New("unsupported")
)

// Fetcher fetches URLs about once a minute and sends you the data if it has changed
type Fetcher struct {
	client *http.Client
	out    chan FetchedData // return new data
	in     chan []string    // accept new url sets
	work   chan toFetch     // push work to workers
}

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
	ctx  context.Context
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

// New creates a new Fetcher
func New(out chan FetchedData) (*Fetcher, error) {
	return &Fetcher{
		client: &http.Client{Timeout: 5 * time.Second},
		out:    out,
		in:     make(chan []string, 1),
		work:   make(chan toFetch),
	}, nil
}

// Start begins the fetching process and must only be called once
func (f *Fetcher) Start(ctx context.Context) {

	resultCh := make(chan fromFetch)

	// kick off fixed number of workers
	for i := 0; i < 2; i++ {
		go func() {
			// forever: either do a fetch or quit
			for {
				select {
				case work := <-f.work:
					res := doFetch(work.ctx, f.client, work.url, work.etag)
					res.url = work.url
					res.time = time.Now().Unix()
					resultCh <- res
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
				log.Debugf("new fetch list: %v", urls)
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
						state.time = result.time
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
				f.tryDispatch(ctx, data)
			case <-ticker.C:
				f.tryDispatch(ctx, data)
			case <-ctx.Done():
				ticker.Stop()
				break loop
			}
		}
	}()
}

// tryDispatch starts as many fetches as possible out of those due to be run
// The number dispatched is limited by how many workers are currently waiting.
func (f *Fetcher) tryDispatch(ctx context.Context, data map[string]*fetchState) {
	// TODO - there's no check against starvation of some URLs
	cutoff := time.Now().Unix() - 60
	for url, state := range data {
		if state.time >= 0 && state.time <= cutoff {
			instruction := toFetch{ctx: ctx, url: url, etag: state.etag}
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

// doFetch does the fetch, blocking until complete or cancelled
func doFetch(ctx context.Context, client *http.Client, urlString, etag string) fromFetch {
	u, err := url.Parse(urlString)
	if err != nil {
		return fromFetch{err: err}
	}

	// TODO - remove this repetition
	if u.Scheme == "http" || u.Scheme == "https" {
		resCh := make(chan fromFetch)
		go doHTTPFetch(client, u, urlString, etag, resCh)
		select {
		case res := <-resCh:
			close(resCh)
			return res
		case <-ctx.Done():
			close(resCh)
			return fromFetch{err: ErrCancelled}
		}
	} else if u.Scheme == "file" {
		resCh := make(chan fromFetch)
		go doFileFetch(u, urlString, etag, resCh)
		select {
		case res := <-resCh:
			close(resCh)
			return res
		case <-ctx.Done():
			close(resCh)
			return fromFetch{err: ErrCancelled}
		}
	} else {
		return fromFetch{err: ErrUnsupported}
	}
}

func doHTTPFetch(client *http.Client, u *url.URL, urlString, etag string, resCh chan fromFetch) {
	req, _ := http.NewRequest("GET", urlString, nil)
	if etag != "" {
		req.Header.Add("If-None-Match", etag)
	}

	resp, err := client.Do(req)
	if err != nil {
		resCh <- fromFetch{err: ErrFailed}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 304 {
		resCh <- fromFetch{etag: etag}
		return
	}
	if resp.StatusCode != 200 {
		resCh <- fromFetch{err: ErrFailed}
		return
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		resCh <- fromFetch{err: ErrFailed}
		return
	}

	newEtag := resp.Header.Get("ETag")
	if newEtag == "" {
		// server not supplied anything, so must assume changed
		newEtag = strconv.FormatInt(time.Now().Unix(), 10)
	}

	resCh <- fromFetch{data: b, etag: newEtag}
}

func doFileFetch(u *url.URL, urlString, etag string, resCh chan fromFetch) {
	fileinfo, err := os.Stat(u.Path)
	if err != nil {
		resCh <- fromFetch{err: err}
		return
	}

	timestamp := strconv.FormatInt(fileinfo.ModTime().Unix(), 10)
	if timestamp == etag {
		resCh <- fromFetch{etag: etag}
		return
	}

	b, err := ioutil.ReadFile(u.Path)
	if err != nil {
		resCh <- fromFetch{err: ErrFailed}
		return
	}

	resCh <- fromFetch{data: b, etag: timestamp}
	return
}
