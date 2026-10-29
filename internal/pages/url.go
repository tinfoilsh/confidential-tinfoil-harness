package pages

import (
	"errors"
	"net/netip"
	"net/url"
	"strings"
)

// NormalizeURL validates and canonicalizes a page's URL without resolving
// its host, which context.dev resolves. Its errors name no part of the URL.
func NormalizeURL(rawURL string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", errors.New("a URL is required")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", errors.New("the URL is malformed")
	}
	if parsed.User != nil {
		return "", errors.New("embedded credentials are not allowed")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("only http and https URLs are allowed")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "", errors.New("the URL has no host")
	}
	if port := parsed.Port(); port != "" && port != "80" && port != "443" {
		return "", errors.New("a non-standard port is not allowed")
	}
	if host == "localhost" ||
		strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local") ||
		strings.HasSuffix(host, ".internal") {
		return "", errors.New("the host is not allowed")
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return "", errors.New("IP addresses are not allowed")
	}
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Fragment = ""
	return parsed.String(), nil
}
