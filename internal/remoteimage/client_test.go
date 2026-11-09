package remoteimage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"code.dny.dev/ssrf"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
)

const testImageURL = "https://image.example.test/private-token.png"

func testClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client := New()
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.ServerName = server.Certificate().DNSNames[0]
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	client.http.Transport = transport
	t.Cleanup(transport.CloseIdleConnections)
	return client
}

func testPNG(t *testing.T, picture image.Image) []byte {
	t.Helper()
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, picture); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestFetchReencodesImagesAndStripsRedirectMetadata(t *testing.T) {
	picture := image.NewNRGBA(image.Rect(0, 0, 2, 3))
	picture.SetNRGBA(1, 2, color.NRGBA{R: 120, G: 60, B: 30, A: 255})
	encoded := append(testPNG(t, picture), []byte("trailing private metadata")...)
	var requests atomic.Int32
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		for _, header := range []string{"Authorization", "Cookie", "Referer", "Accept-Language", "X-Forwarded-For"} {
			if r.Header.Get(header) != "" {
				t.Errorf("forwarded %s", header)
			}
		}
		if r.URL.Path == "/private-token.png" {
			w.Header().Set("Set-Cookie", "tracking=private")
			http.Redirect(w, r, "https://cdn.example.test/final.png", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write(encoded)
	})
	result, err := client.Fetch(t.Context(), testImageURL)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := png.Decode(bytes.NewReader(result.Bytes))
	if err != nil || result.ContentType != imageContentType || requests.Load() != 2 {
		t.Fatalf("invalid image response: type=%s requests=%d error=%v", result.ContentType, requests.Load(), err)
	}
	if decoded.Bounds() != picture.Bounds() || color.NRGBAModel.Convert(decoded.At(1, 2)) != picture.At(1, 2) {
		t.Fatal("image pixels changed")
	}
	if bytes.Contains(result.Bytes, []byte("trailing private metadata")) {
		t.Fatal("source metadata escaped the decoder")
	}
}

func TestFetchRefusesUnsafeURLsBeforeNetwork(t *testing.T) {
	var requests atomic.Int32
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) })
	for _, target := range []string{
		"http://image.example.test/photo", "https://user:secret@image.example.test/photo",
		"https://image.example.test:8443/photo", "https://image.example.test:80/photo",
		"https://127.0.0.1/photo", "https://169.254.169.254/photo", "https://[::1]/photo",
		"https://[::ffff:127.0.0.1]/photo", "https://2130706433/photo", "https://0x7f.0.0.1/photo",
		"https://localhost./photo", "https://service.internal/photo", "file:///private/photo",
	} {
		if _, err := client.Fetch(t.Context(), target); api.AsError(err).HTTP != http.StatusBadRequest {
			t.Errorf("unsafe URL was not refused: %v", err)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("unsafe URL reached a server")
	}
}

func TestFetchPreservesJPEGDisplayOrientation(t *testing.T) {
	const width, height = 32, 16
	picture := image.NewGray(image.Rect(0, 0, width, height))
	for y := range height {
		for x := range width {
			picture.SetGray(x, y, color.Gray{Y: uint8(85 * (x/(width/2) + 2*(y/(height/2))))})
		}
	}
	var jpegBytes bytes.Buffer
	if err := jpeg.Encode(&jpegBytes, picture, &jpeg.Options{Quality: 100}); err != nil {
		t.Fatal(err)
	}
	segment := func(marker byte, payload []byte) []byte {
		header := binary.BigEndian.AppendUint16([]byte{0xff, marker}, uint16(len(payload)+2))
		return append(header, payload...)
	}
	// A little-endian EXIF directory containing orientation 6 (clockwise).
	exif := []byte{'E', 'x', 'i', 'f', 0, 0, 'I', 'I', 42, 0, 8, 0, 0, 0, 1, 0, 0x12, 1, 3, 0, 1, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0}
	xmp := segment(0xe1, []byte("http://ns.adobe.com/xap/1.0/\x00<x:xmpmeta/>"))
	check := func(t *testing.T, metadata []byte, w, h int, corners [4]uint8) {
		t.Helper()
		original := jpegBytes.Bytes()
		body := append(append(bytes.Clone(original[:2]), metadata...), original[2:]...)
		client := testClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) })
		result, err := client.Fetch(t.Context(), testImageURL)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := png.Decode(bytes.NewReader(result.Bytes))
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Bounds() != image.Rect(0, 0, w, h) {
			t.Fatalf("incorrect oriented dimensions: %v", decoded.Bounds())
		}
		for i, point := range []image.Point{{0, 0}, {w - 1, 0}, {0, h - 1}, {w - 1, h - 1}} {
			if got := color.GrayModel.Convert(decoded.At(point.X, point.Y)).(color.Gray).Y; got != corners[i] {
				t.Fatalf("corner %d: got %d, want %d", i, got, corners[i])
			}
		}
	}
	for _, tc := range []struct {
		name        string
		orientation byte
		w, h        int
		corners     [4]uint8
	}{
		{"normal", 1, width, height, [4]uint8{0, 85, 170, 255}},
		{"horizontal", 2, width, height, [4]uint8{85, 0, 255, 170}},
		{"half turn", 3, width, height, [4]uint8{255, 170, 85, 0}},
		{"vertical", 4, width, height, [4]uint8{170, 255, 0, 85}},
		{"transpose", 5, height, width, [4]uint8{0, 170, 85, 255}},
		{"clockwise", 6, height, width, [4]uint8{170, 0, 255, 85}},
		{"transverse", 7, height, width, [4]uint8{255, 85, 170, 0}},
		{"counterclockwise", 8, height, width, [4]uint8{85, 255, 0, 170}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Clone(exif)
			payload[24] = tc.orientation
			check(t, append(bytes.Clone(xmp), segment(0xe1, payload)...), tc.w, tc.h, tc.corners)
		})
	}
	t.Run("big endian without XMP", func(t *testing.T) {
		payload := []byte{'E', 'x', 'i', 'f', 0, 0, 'M', 'M', 0, 42, 0, 0, 0, 8, 0, 1, 1, 0x12, 0, 3, 0, 0, 0, 1, 0, 6, 0, 0, 0, 0, 0, 0}
		check(t, segment(0xe1, payload), height, width, [4]uint8{170, 0, 255, 85})
	})
	t.Run("orientation after another IFD entry", func(t *testing.T) {
		payload := append(bytes.Clone(exif[:16]), []byte{0, 1, 4, 0, 1, 0, 0, 0, 32, 0, 0, 0}...)
		payload[14] = 2
		payload = append(payload, exif[16:]...)
		check(t, segment(0xe1, payload), height, width, [4]uint8{170, 0, 255, 85})
	})
	t.Run("IFD outside APP1", func(t *testing.T) {
		payload := bytes.Clone(exif[:14])
		payload[10] = 12
		metadata := append(segment(0xe1, payload), segment(0xe2, exif[14:])...)
		check(t, metadata, width, height, [4]uint8{0, 85, 170, 255})
	})
	for _, tc := range []struct {
		name   string
		offset int
		value  []byte
	}{
		{"oversized IFD offset", 10, []byte{255, 255, 255, 255}},
		{"oversized entry count", 14, []byte{255, 255}},
		{"out-of-line orientation", 20, []byte{255, 255, 255, 255}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Clone(exif)
			copy(payload[tc.offset:], tc.value)
			check(t, segment(0xe1, payload), width, height, [4]uint8{0, 85, 170, 255})
		})
	}
}

func TestTransportRejectsResolvedPrivateAndReservedAddresses(t *testing.T) {
	transport := New().http.Transport.(*http.Transport)
	for _, address := range []string{"127.0.0.1:443", "10.0.0.1:443", "169.254.169.254:443", "100.64.0.1:443", "192.0.2.1:443", "[::1]:443", "[::ffff:127.0.0.1]:443", "[fc00::1]:443", "[64:ff9b::7f00:1]:443"} {
		connection, err := transport.DialContext(t.Context(), "tcp", address)
		if connection != nil {
			connection.Close()
		}
		if !errors.Is(err, ssrf.ErrProhibitedIP) {
			t.Errorf("resolved destination was not blocked by the socket guard: %s: %v", address, err)
		}
	}
}

func TestFetchRefusesUnsafeAndExcessiveRedirects(t *testing.T) {
	for _, target := range []string{"https://127.0.0.1/photo", "https://image.example.test:8443/photo", "http://image.example.test/photo", "https://user:secret@image.example.test/photo", testImageURL} {
		t.Run(target, func(t *testing.T) {
			var requests atomic.Int32
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Redirect(w, r, target, http.StatusFound)
			})
			_, err := client.Fetch(t.Context(), testImageURL)
			if err == nil || strings.Contains(err.Error(), "private-token") || strings.Contains(err.Error(), target) {
				t.Fatalf("redirect was accepted or exposed its URL: %v", err)
			}
			expected := int32(1)
			if target == testImageURL {
				expected = maxRedirects + 1
			}
			if requests.Load() != expected {
				t.Fatalf("followed %d requests, expected %d", requests.Load(), expected)
			}
		})
	}
}

func TestFetchBoundsBodiesAndDecodedPixels(t *testing.T) {
	oversized := image.NewGray(image.Rect(0, 0, 5000, 4001))
	valid := testPNG(t, image.NewGray(image.Rect(0, 0, 1, 1)))
	oversizedDownload := append(valid, bytes.Repeat([]byte("x"), maxImageBytes+1-len(valid))...)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"active SVG", []byte(`<svg xmlns="http://www.w3.org/2000/svg"><image href="https://tracker.example/pixel"/></svg>`)},
		{"oversized download", oversizedDownload},
		{"oversized decoded image", testPNG(t, oversized)},
		{"extreme image dimension", testPNG(t, image.NewGray(image.Rect(0, 0, maxImageSide+1, 1)))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "image/png")
				w.(http.Flusher).Flush()
				_, _ = w.Write(tc.body)
			})
			if _, err := client.Fetch(t.Context(), testImageURL); api.AsError(err).HTTP != http.StatusBadGateway {
				t.Fatalf("unsafe image was not refused: %v", err)
			}
		})
	}
}

func TestFetchCapacityAndCancellation(t *testing.T) {
	client := New()
	for range maxConcurrentFetches {
		client.slots <- struct{}{}
	}
	_, err := client.Fetch(t.Context(), testImageURL)
	if e := api.AsError(err); e.HTTP != http.StatusServiceUnavailable || e.RetryAfter != busyRetrySeconds {
		t.Fatalf("full fetcher was not refused: %v", err)
	}
	<-client.slots
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.Fetch(ctx, testImageURL); err == nil || len(client.slots) != maxConcurrentFetches-1 {
		t.Fatal("canceled fetch leaked a slot")
	}
	var output boundedImage
	if _, err := io.Copy(&output, io.LimitReader(strings.NewReader(strings.Repeat("x", maxImageBytes+1)), maxImageBytes+1)); err == nil || output.Len() > maxImageBytes {
		t.Fatal("encoded image exceeded the output budget")
	}
}
