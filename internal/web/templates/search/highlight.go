package search

import (
	"html"
	"regexp"
	"strings"
)

// HighlightText wraps occurrences of the search query in the text with <mark> tags.
// Case-insensitive matching. Returns HTML-safe string with highlights.
func HighlightText(text, query string) string {
	if query == "" || text == "" {
		return html.EscapeString(text)
	}

	// Escape query for regex, then create case-insensitive pattern
	escapedQuery := regexp.QuoteMeta(query)
	re := regexp.MustCompile(`(?i)` + escapedQuery)

	// Escape the text first for HTML safety
	safeText := html.EscapeString(text)

	// Find all matches and wrap them with <mark> tags
	highlighted := re.ReplaceAllStringFunc(safeText, func(match string) string {
		return "<mark>" + match + "</mark>"
	})

	return highlighted
}

// TruncateWithContext returns a snippet of text around the first match of the query.
// Useful for showing search results in context. Returns up to maxLen characters.
func TruncateWithContext(text, query string, maxLen int) string {
	if query == "" || text == "" {
		if len(text) <= maxLen {
			return text
		}
		return text[:maxLen] + "..."
	}

	// Find the first occurrence (case-insensitive)
	lowerText := strings.ToLower(text)
	lowerQuery := strings.ToLower(query)
	idx := strings.Index(lowerText, lowerQuery)

	if idx == -1 {
		// Query not found, return beginning
		if len(text) <= maxLen {
			return text
		}
		return text[:maxLen] + "..."
	}

	// Calculate context window around the match
	contextBefore := 50
	contextAfter := maxLen - len(query) - contextBefore

	start := idx - contextBefore
	if start < 0 {
		start = 0
	}

	end := idx + len(query) + contextAfter
	if end > len(text) {
		end = len(text)
	}

	snippet := text[start:end]
	if start > 0 {
		snippet = "..." + snippet
	}
	if end < len(text) {
		snippet = snippet + "..."
	}

	return snippet
}
