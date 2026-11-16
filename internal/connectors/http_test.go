package connectors

import (
	"errors"
	"net/http"
	"testing"

	"code.dny.dev/ssrf"
)

func TestEveryRequestDialsPublicAddressesAlone(t *testing.T) {
	c := New("https://chat.example.test/oauth/client-metadata.json", "https://chat.example.test/connectors/callback")
	for name, client := range map[string]*http.Client{"fetch": c.http.fetch, "direct": c.http.direct} {
		dialer := client.Transport.(bounded).inner.(*http.Transport)
		for _, address := range []string{"127.0.0.1:443", "10.0.0.1:443", "169.254.169.254:443", "100.64.0.1:443", "[::1]:443", "[fc00::1]:443"} {
			connection, err := dialer.DialContext(t.Context(), "tcp", address)
			if connection != nil {
				connection.Close()
			}
			if !errors.Is(err, ssrf.ErrProhibitedIP) {
				t.Errorf("%s reached %s: %v", name, address, err)
			}
		}
	}
}
