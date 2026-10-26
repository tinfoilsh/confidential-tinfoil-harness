package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
)

func PostJSON(ctx context.Context, client *http.Client, url, auth string, in, out any) error {
	return PostJSONWithHeaders(ctx, client, url, auth, nil, in, out)
}

// requestBuffer lets a transport finish reading or replaying a request while
// cleanup erases its bytes. RoundTrip may return before its body reader stops.
type requestBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *requestBuffer) clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.data)
	b.data = nil
}

func (b *requestBuffer) open() (io.ReadCloser, error) {
	return &requestReader{buffer: b}, nil
}

type requestReader struct {
	buffer *requestBuffer
	offset int
}

func (r *requestReader) Read(dst []byte) (int, error) {
	r.buffer.mu.Lock()
	defer r.buffer.mu.Unlock()
	if r.offset >= len(r.buffer.data) {
		return 0, io.EOF
	}
	n := copy(dst, r.buffer.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *requestReader) Close() error { return nil }

func PostJSONWithHeaders(ctx context.Context, client *http.Client, url, auth string, headers map[string]string, in, out any) error {
	body, encoded := in.(json.RawMessage)
	if !encoded {
		body = Raw(in)
	}
	buffer := &requestBuffer{data: body}
	defer buffer.clear()
	reader, _ := buffer.open()
	req, err := http.NewRequestWithContext(ctx, "POST", url, reader)
	if err != nil {
		return err
	}
	req.ContentLength, req.GetBody = int64(len(body)), buffer.open
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		// A missing-attachment error lists every missing id.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return WithRetryAfter(DownstreamError(resp.StatusCode, b), resp.Header)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxRequestBody))
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, MaxResponseBody)).Decode(out)
}
