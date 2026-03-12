package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/web/webctx"
	messagetmpl "github.com/steipete/discrawl/internal/web/templates/messages"
)

const messagesPerPage = 50

// SessionManager abstracts session operations.
type SessionManager interface {
	GetBool(ctx context.Context, key string) bool
	Put(ctx context.Context, key string, val interface{})
}

// HandleMessageViewer renders the message viewer page for a channel.
func HandleMessageViewer(registry *store.Registry, sm SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gs := webctx.GetGuildStore(r.Context())
		if gs == nil {
			http.Error(w, "guild not found", http.StatusNotFound)
			return
		}
		guildID := chi.URLParam(r, "guildID")
		channelID := chi.URLParam(r, "channelID")

		channelName := channelID
		channelTopic := ""
		isNSFW := false
		channels, err := gs.Channels(r.Context(), guildID)
		if err == nil {
			for _, ch := range channels {
				if ch.ID == channelID {
					channelName = ch.Name
					channelTopic = ch.Topic
					isNSFW = ch.IsNSFW
					break
				}
			}
		}

		guildName := resolveGuildName(r, registry, guildID)

		// Check NSFW opt-in preference
		nsfwAccepted := sm.GetBool(r.Context(), "nsfw_accepted")

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = messagetmpl.Viewer(guildID, guildName, channelID, channelName, channelTopic, isNSFW, nsfwAccepted).Render(r.Context(), w)
	}
}

// HandleMessageList returns an HTMX partial of paginated messages for a channel.
func HandleMessageList(registry *store.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gs := webctx.GetGuildStore(r.Context())
		if gs == nil {
			http.Error(w, "guild not found", http.StatusNotFound)
			return
		}
		guildID := chi.URLParam(r, "guildID")
		channelID := chi.URLParam(r, "channelID")
		beforeID := r.URL.Query().Get("before")

		msgs, err := gs.ListMessages(r.Context(), store.MessageListOptions{
			Channel:        channelID,
			BeforeID:       beforeID,
			Limit:          messagesPerPage,
			ExcludeDeleted: true,
		})
		if err != nil {
			http.Error(w, "failed to load messages", http.StatusInternalServerError)
			return
		}

		// When using BeforeID cursor, results come back newest-first; reverse for display.
		if beforeID != "" {
			for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
				msgs[i], msgs[j] = msgs[j], msgs[i]
			}
		}

		sections := groupMessages(msgs)

		oldestID := ""
		if len(msgs) == messagesPerPage && len(msgs) > 0 {
			oldestID = msgs[0].MessageID
		}

		_ = guildID // used in template via guildID param
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = messagetmpl.MessageList(guildID, channelID, sections, oldestID).Render(r.Context(), w)
	}
}

// groupMessages organises messages into day sections with author-collapsed groups.
func groupMessages(msgs []store.MessageRow) []messagetmpl.DaySection {
	var sections []messagetmpl.DaySection
	var currentSection *messagetmpl.DaySection
	var currentGroup *messagetmpl.MessageGroup

	for _, msg := range msgs {
		day := truncateToDay(msg.CreatedAt)

		if currentSection == nil || !currentSection.Day.Equal(day) {
			sections = append(sections, messagetmpl.DaySection{Day: day})
			currentSection = &sections[len(sections)-1]
			currentGroup = nil
		}

		collapseIntoGroup := currentGroup != nil &&
			currentGroup.AuthorID == msg.AuthorID &&
			msg.CreatedAt.Sub(lastMessageTime(*currentGroup)) < 5*time.Minute

		if !collapseIntoGroup {
			currentSection.Groups = append(currentSection.Groups, messagetmpl.MessageGroup{
				AuthorID:   msg.AuthorID,
				AuthorName: msg.AuthorName,
			})
			currentGroup = &currentSection.Groups[len(currentSection.Groups)-1]
		}

		currentGroup.Messages = append(currentGroup.Messages, msg)
	}

	return sections
}

func truncateToDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

func lastMessageTime(g messagetmpl.MessageGroup) time.Time {
	if len(g.Messages) == 0 {
		return time.Time{}
	}
	return g.Messages[len(g.Messages)-1].CreatedAt
}

// HandleNSFWAccept sets the NSFW acceptance flag in the session.
func HandleNSFWAccept(sm SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		sm.Put(r.Context(), "nsfw_accepted", true)

		// Return HTMX trigger to reload messages
		w.Header().Set("HX-Refresh", "true")
		w.WriteHeader(http.StatusOK)
	}
}
