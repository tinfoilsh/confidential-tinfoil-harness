package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/domain"
)

func TestFormatSearchOutput(t *testing.T) {
	got := formatSearchOutput([]any{
		map[string]any{"title": "Climate Article", "url": "https://example.com/climate", "content": "Global temps are rising.", "published_date": "2026-04-17"},
		map[string]any{"title": " ", "url": "https://example.com/tech", "content": "AI is advancing rapidly."},
	})
	want := "Source: Climate Article\nURL: https://example.com/climate\nPublished: 2026-04-17\nGlobal temps are rising.\n\nSource: Search result\nURL: https://example.com/tech\nAI is advancing rapidly."
	if got != want {
		t.Fatalf("formatted search output:\n%s\nwant:\n%s", got, want)
	}
	if got := formatSearchOutput([]any{}); got != "No safe search results were found." {
		t.Errorf("unexpected empty output = %q", got)
	}
}

func TestFormatFetchFailures(t *testing.T) {
	got := formatFetchFailures([]any{
		map[string]any{"url": "https://ok.test", "status": "completed"},
		map[string]any{"url": "https://a.test", "status": "failed", "error": "timeout"},
		map[string]any{"url": "https://b.test", "status": "failed"},
		map[string]any{"status": "failed", "error": "blocked"},
	})
	if want := "Fetch failed for https://a.test: timeout\nFetch failed for https://b.test.\nFetch failed: blocked"; got != want {
		t.Fatalf("formatted failures %q, want %q", got, want)
	}
}

func TestModelReadsSearchResultsAsCitedText(t *testing.T) {
	search := `{"results":[{"title":" A ","url":"https://a.test","content":"alpha"}],"pii_checked":false,"pii_filter_requests":0}`
	for _, tc := range []struct {
		name, content, want string
		sources             []citationSource
	}{
		{"web_search", search, "Source: A\nURL: https://a.test\nalpha", []citationSource{{"https://a.test", "A"}}},
		{"web_fetch", `{"pages":[{"url":"https://a.test","content":"alpha"}],"results":[{"url":"https://a.test","status":"completed"}]}`, "Source: Fetched page\nURL: https://a.test\nalpha", []citationSource{{"https://a.test", "Fetched page"}}},
		{"web_fetch", `{"results":[{"url":"https://a.test","status":"failed","error":"timeout"}]}`, "Fetch failed for https://a.test: timeout", nil},
		{"web_fetch", `{"results":[]}`, `{"results":[]}`, nil},
		{"web_search", "plain text", "plain text", nil},
		{"render_chart", search, search, nil},
	} {
		if got, sources := modelCopy(tc.name, tc.content); got != tc.want || !reflect.DeepEqual(sources, tc.sources) {
			t.Errorf("%s read %q as %q citing %v, want %q citing %v", tc.name, tc.content, got, sources, tc.want, tc.sources)
		}
	}
}

func citationsIn(t *testing.T, text string, sources ...citationSource) []domain.URLCitation {
	t.Helper()
	var out []domain.URLCitation
	for _, annotation := range citationAnnotations(text, sources) {
		if annotation.Type != "url_citation" {
			t.Fatalf("annotation type %v", annotation.Type)
		}
		out = append(out, *annotation.URLCitation)
	}
	return out
}

func TestCitationAnnotationsSpanTheLinkLabel(t *testing.T) {
	content := "First claim [Source A](https://example.com/a). Later claim [Source B](https://example.com/b)."
	got := citationsIn(t, content, citationSource{"https://example.com/a", "Source A"}, citationSource{"https://example.com/b", "Source B"})
	if len(got) != 2 {
		t.Fatalf("expected 2 annotations, got %v", got)
	}
	for i, label := range []string{"Source A", "Source B"} {
		start, end := got[i].StartIndex, got[i].EndIndex
		if got[i].Title != label || content[start:end] != label || got[i].URL != "https://example.com/"+strings.ToLower(label[len(label)-1:]) {
			t.Errorf("annotation %d: %v", i, got[i])
		}
	}
	if got := citationsIn(t, "Real cite [Known](https://known.example/a) and fake cite [Fake](https://fabricated.example/x).", citationSource{"https://known.example/a", "Known Source"}); len(got) != 1 || got[0].URL != "https://known.example/a" {
		t.Fatalf("an unknown URL was annotated: %v", got)
	}
	repeated := citationsIn(t, "First [Same](https://example.com/same) and again [Same](https://example.com/same).", citationSource{"https://example.com/same", "Same"})
	if len(repeated) != 2 || repeated[0].StartIndex >= repeated[1].StartIndex {
		t.Fatalf("repeated citations %v", repeated)
	}
	if got := citationAnnotations("plain text without citations", []citationSource{{"https://example.com/a", "Source A"}}); got != nil {
		t.Fatalf("plain text was annotated: %v", got)
	}
}

func TestCitationAnnotationsUseCodePointOffsets(t *testing.T) {
	content := "Thin\u202fspace\u202fthen [Page](https://example.com/page)."
	got := citationsIn(t, content, citationSource{"https://example.com/page", "Page"})
	if len(got) != 1 || string([]rune(content)[got[0].StartIndex:got[0].EndIndex]) != "Page" {
		t.Fatalf("annotations %v", got)
	}
}

func TestCitationAnnotationsMatchURLsWithParentheses(t *testing.T) {
	const planet = "https://en.wikipedia.org/wiki/Mercury_(planet)"
	content := "Mercury is closest to the Sun [Wikipedia](" + planet + ") (see [A](https://a.test/x))."
	got := citationsIn(t, content, citationSource{planet, "Mercury (planet)"}, citationSource{"https://a.test/x", "A"})
	want := []domain.URLCitation{
		{URL: planet, Title: "Mercury (planet)", StartIndex: strings.Index(content, "Wikipedia"), EndIndex: strings.Index(content, "Wikipedia") + len("Wikipedia")},
		{URL: "https://a.test/x", Title: "A", StartIndex: strings.Index(content, "[A]") + 1, EndIndex: strings.Index(content, "[A]") + 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("annotations %v, want %v", got, want)
	}
}

func TestCitationAnnotationsKeepTheModelsURL(t *testing.T) {
	const recorded = "https://apnews.com/article/ukraine-druzhba-pipeline-7dfc9574bf95a69eda13b1440171e402"
	got := citationsIn(t, "Pipeline repairs are complete [Source: AP News]("+recorded+"/).", citationSource{recorded, "AP News"})
	if len(got) != 1 || got[0].URL != recorded+"/" || got[0].Title != "AP News" {
		t.Fatalf("annotations %v", got)
	}
}

func TestCitationTitles(t *testing.T) {
	const source = "https://example.com/article"
	for name, tc := range map[string]struct {
		titles []string
		want   string
	}{
		"whitespace":        {[]string{" \t\n"}, source},
		"preserve title":    {[]string{" Article title "}, " Article title "},
		"prefer real title": {[]string{"", "Article title"}, "Article title"},
	} {
		var sources []citationSource
		for _, title := range tc.titles {
			sources = append(sources, citationSource{source, title})
		}
		if got := citationsIn(t, "See [source]("+source+") for details.", sources...); len(got) != 1 || got[0].Title != tc.want {
			t.Errorf("%s: citation %v, want title %q", name, got, tc.want)
		}
	}
}

func TestNormalizeURL(t *testing.T) {
	for want, inputs := range map[string][]string{
		"https://apnews.com/article/ukraine-druzhba-pipeline-7dfc9574bf95a69eda13b1440171e402": {
			"https://apnews.com/article/ukraine-druzhba-pipeline-7dfc9574bf95a69eda13b1440171e402/",
			"HTTPS://www.Apnews.com/article/ukraine-druzhba-pipeline-7dfc9574bf95a69eda13b1440171e402/?utm_source=exa#top\u200B",
		},
		"https://example.com/search?lang=en&q=apple": {"https://example.com/search?q=apple&lang=en"},
		"https://example.com/":                       {"https://example.com/"},
		"":                                           {"   "},
	} {
		for _, input := range inputs {
			if got := normalizeURL(input); got != want {
				t.Errorf("normalizeURL(%q) = %q, want %q", input, got, want)
			}
		}
	}
}
