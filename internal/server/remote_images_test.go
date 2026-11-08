package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/remoteimage"
)

type imageFetch func(context.Context, string) (*remoteimage.Image, error)

func (fetch imageFetch) Fetch(ctx context.Context, url string) (*remoteimage.Image, error) {
	return fetch(ctx, url)
}

func TestRemoteImagesRequireChatAccessAndReturnImagePayload(t *testing.T) {
	h, _, _ := testChatHarness(t, nil)
	const imageURL = "https://images.example.test/private.png"
	imageBytes := []byte("image bytes from the validated fetcher")
	calls := 0
	h.attachments.Images = imageFetch(func(_ context.Context, url string) (*remoteimage.Image, error) {
		calls++
		if url != imageURL {
			t.Fatalf("wrong image URL")
		}
		return &remoteimage.Image{Bytes: imageBytes, ContentType: "image/png"}, nil
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/metadata/image", bytes.NewReader(api.Raw(api.Object{"url": imageURL})))
	req.Header.Set("Authorization", "Bearer clerk-alice")
	req.Header.Set("Content-Type", "application/json")
	refused := httptest.NewRecorder()
	h.routes().ServeHTTP(refused, req)
	if refused.Code != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("caller without chat access fetched an image: status=%d calls=%d", refused.Code, calls)
	}
	for _, user := range []string{"alice", ""} {
		answer := postRoute(h, "/v1/metadata/image", api.Object{"url": imageURL}, user)
		var payload map[string]string
		if err := json.Unmarshal(answer.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		if answer.Code != http.StatusOK || payload["image_bytes"] != base64.StdEncoding.EncodeToString(imageBytes) || payload["image_content_type"] != "image/png" || len(payload) != 2 {
			t.Fatalf("incorrect image payload: HTTP %d", answer.Code)
		}
	}
	if calls != 2 {
		t.Fatalf("expected two authenticated fetches, got %d", calls)
	}
}
