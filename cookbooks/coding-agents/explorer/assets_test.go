package explorer

import (
	"strings"
	"testing"
)

func TestExplorerAssetsPreserveAccessibilityAndEvidenceBoundaries(t *testing.T) {
	html := string(indexHTML)
	for required := range map[string]bool{
		"<main": true, "<nav": true, "aria-live=": true, "Skip to evidence": true,
		"Unsafe versus protected": true, "Responsibility split": true, "Falsifier": true,
		"Episode comparison table": true, "Logical and observed identity table": true,
	} {
		if !strings.Contains(html, required) {
			t.Fatalf("HTML lacks %q", required)
		}
	}
	if strings.Contains(html, "<script>") || strings.Contains(html, "<style>") {
		t.Fatal("HTML contains inline script or style")
	}

	javascript := string(appJS)
	for required := range map[string]bool{
		"textContent": true, "aria-selected": true, "ArrowRight": true,
		"ArrowLeft": true, "history": true, "raw-": true, "tabindex": true,
	} {
		if !strings.Contains(javascript, required) {
			t.Fatalf("JavaScript lacks %q", required)
		}
	}
	for forbidden := range map[string]bool{"innerHTML": true, "eval(": true, "document.write": true, "file://": true} {
		if strings.Contains(javascript, forbidden) {
			t.Fatalf("JavaScript contains forbidden %q", forbidden)
		}
	}

	css := string(stylesCSS)
	for required := range map[string]bool{
		"oklch(": true, ":focus-visible": true, "prefers-reduced-motion": true,
		"@media (max-width:": true, "color-scheme: light": true,
	} {
		if !strings.Contains(css, required) {
			t.Fatalf("CSS lacks %q", required)
		}
	}
	for forbidden := range map[string]bool{
		"backdrop-filter": true, "background-clip: text": true, "border-left: 2": true,
		"border-left: 3": true, "border-left: 4": true,
	} {
		if strings.Contains(css, forbidden) {
			t.Fatalf("CSS contains forbidden pattern %q", forbidden)
		}
	}
}
