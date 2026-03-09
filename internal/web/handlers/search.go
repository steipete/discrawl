package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/web/webctx"
	searchtmpl "github.com/steipete/discrawl/internal/web/templates/search"
)

// HandleSearch renders the search page and processes queries.
func HandleSearch(registry *store.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gs := webctx.GetGuildStore(r.Context())
		if gs == nil {
			http.Error(w, "guild not found", http.StatusNotFound)
			return
		}
		guildID := chi.URLParam(r, "guildID")

		q := r.URL.Query().Get("q")
		channel := r.URL.Query().Get("channel")
		author := r.URL.Query().Get("author")

		var results []store.SearchResult
		if q != "" {
			var err error
			results, err = gs.SearchMessages(r.Context(), store.SearchOptions{
				Query:   q,
				Channel: channel,
				Author:  author,
				Limit:   50,
			})
			if err != nil {
				http.Error(w, "search failed", http.StatusInternalServerError)
				return
			}
		}

		guildName := resolveGuildName(r, registry, guildID)

		w.Header().Set("Content-Type", "text/html; charset=utf-8")

		// HTMX partial: return only results fragment.
		if r.Header.Get("HX-Request") == "true" {
			_ = searchtmpl.SearchResults(guildID, results, q).Render(r.Context(), w)
			return
		}

		_ = searchtmpl.Page(guildID, guildName, results, q, channel, author).Render(r.Context(), w)
	}
}
