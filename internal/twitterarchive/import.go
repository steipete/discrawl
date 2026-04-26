package twitterarchive

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/steipete/discrawl/internal/store"
)

const (
	GuildID   = "x"
	GuildName = "X / Twitter Archive"

	tweetsChannelID = "x:tweets"
	likesChannelID  = "x:likes"
	syncScope       = "twitter:last_import"
	archiveScope    = "twitter:last_archive"
)

type Options struct {
	Path   string
	DryRun bool
	Now    func() time.Time
}

type Stats struct {
	Path            string    `json:"path"`
	FilesScanned    int       `json:"files_scanned"`
	Accounts        int       `json:"accounts"`
	Tweets          int       `json:"tweets"`
	Likes           int       `json:"likes"`
	DMConversations int       `json:"dm_conversations"`
	DMMessages      int       `json:"dm_messages"`
	Skipped         int       `json:"skipped"`
	DryRun          bool      `json:"dry_run,omitempty"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
}

type account struct {
	ID          string
	Username    string
	DisplayName string
	Email       string
	CreatedAt   string
	Raw         map[string]any
}

type importSnapshot struct {
	account       account
	channels      map[string]store.ChannelRecord
	members       map[string]store.MemberRecord
	messages      []store.MessageMutation
	conversations map[string]struct{}
}

func Import(ctx context.Context, st *store.Store, opts Options) (Stats, error) {
	if st == nil && !opts.DryRun {
		return Stats{}, errors.New("store is required")
	}
	if strings.TrimSpace(opts.Path) == "" {
		return Stats{}, errors.New("archive path is required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	stats := Stats{Path: opts.Path, DryRun: opts.DryRun, StartedAt: now().UTC()}
	reader, err := zip.OpenReader(opts.Path)
	if err != nil {
		stats.FinishedAt = now().UTC()
		return stats, fmt.Errorf("open twitter archive: %w", err)
	}
	defer func() { _ = reader.Close() }()

	snap := importSnapshot{
		channels:      map[string]store.ChannelRecord{},
		members:       map[string]store.MemberRecord{},
		conversations: map[string]struct{}{},
	}
	snap.channels[tweetsChannelID] = channel(tweetsChannelID, "tweets", "tweet")
	snap.channels[likesChannelID] = channel(likesChannelID, "likes", "like")

	for _, file := range sortedFiles(reader.File) {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		kind := classify(file.Name)
		if kind == "" {
			continue
		}
		content, err := readZipText(file)
		if err != nil {
			stats.Skipped++
			continue
		}
		records, err := parseArchiveArray(content)
		if err != nil {
			stats.Skipped++
			continue
		}
		stats.FilesScanned++
		switch kind {
		case "account":
			if parseAccount(records, &snap) {
				stats.Accounts = 1
			}
		case "tweets":
			stats.Tweets += parseTweets(records, &snap)
		case "likes":
			stats.Likes += parseLikes(records, &snap, now().UTC())
		case "dms":
			conversations, messages := parseDMs(records, &snap)
			stats.DMConversations += conversations
			stats.DMMessages += messages
		}
	}
	if snap.account.ID != "" {
		snap.members[memberKey(GuildID, snap.account.ID)] = memberFromAccount(snap.account)
	}
	for i := range snap.messages {
		if snap.messages[i].Record.AuthorID == "" && snap.account.ID != "" {
			snap.messages[i].Record.AuthorID = snap.account.ID
			snap.messages[i].Record.AuthorName = snap.account.Username
		}
	}
	if !opts.DryRun {
		if err := writeSnapshot(ctx, st, snap, opts.Path); err != nil {
			return stats, err
		}
	}
	stats.FinishedAt = now().UTC()
	return stats, nil
}

func writeSnapshot(ctx context.Context, st *store.Store, snap importSnapshot, path string) error {
	if err := st.UpsertGuild(ctx, store.GuildRecord{ID: GuildID, Name: GuildName, RawJSON: rawJSON(map[string]any{"platform": "x"})}); err != nil {
		return err
	}
	for _, member := range sortedMembers(snap.members) {
		if err := st.UpsertMember(ctx, member); err != nil {
			return err
		}
	}
	for _, ch := range sortedChannels(snap.channels) {
		if err := st.UpsertChannel(ctx, ch); err != nil {
			return err
		}
	}
	const chunkSize = 1000
	for start := 0; start < len(snap.messages); start += chunkSize {
		end := start + chunkSize
		if end > len(snap.messages) {
			end = len(snap.messages)
		}
		if err := st.UpsertMessages(ctx, snap.messages[start:end]); err != nil {
			return err
		}
	}
	if err := st.SetSyncState(ctx, syncScope, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return st.SetSyncState(ctx, archiveScope, path)
}

func parseAccount(records []map[string]any, snap *importSnapshot) bool {
	for _, record := range records {
		raw, ok := record["account"].(map[string]any)
		if !ok {
			continue
		}
		acc := account{
			ID:          stringValue(raw["accountId"]),
			Username:    stringValue(raw["username"]),
			DisplayName: firstString(raw["accountDisplayName"], raw["name"]),
			Email:       stringValue(raw["email"]),
			CreatedAt:   stringValue(raw["createdAt"]),
			Raw:         raw,
		}
		if acc.ID == "" || acc.Username == "" {
			continue
		}
		snap.account = acc
		snap.members[memberKey(GuildID, acc.ID)] = memberFromAccount(acc)
		return true
	}
	return false
}

func parseTweets(records []map[string]any, snap *importSnapshot) int {
	count := 0
	for _, record := range records {
		raw, ok := record["tweet"].(map[string]any)
		if !ok {
			continue
		}
		id := firstString(raw["id_str"], raw["id"])
		text := html.UnescapeString(stringValue(raw["full_text"]))
		createdAt := parseTime(firstString(raw["created_at"], raw["createdAt"]))
		if id == "" || text == "" || createdAt.IsZero() {
			continue
		}
		authorID := firstString(raw["author_id"], raw["user_id_str"], nestedString(raw, "user", "id_str"), snap.account.ID)
		authorName := snap.account.Username
		msg := store.MessageMutation{
			Record: store.MessageRecord{
				ID:                "x:tweet:" + id,
				GuildID:           GuildID,
				ChannelID:         tweetsChannelID,
				ChannelName:       "tweets",
				AuthorID:          authorID,
				AuthorName:        authorName,
				MessageType:       0,
				CreatedAt:         createdAt.UTC().Format(time.RFC3339Nano),
				Content:           text,
				NormalizedContent: text,
				ReplyToMessageID:  prefixedTweetID(firstString(raw["in_reply_to_status_id_str"], raw["in_reply_to_status_id"])),
				HasAttachments:    hasMedia(raw),
				RawJSON:           rawJSON(map[string]any{"platform": "x", "type": "tweet", "tweet": raw}),
			},
			Mentions: tweetMentions(id, authorID, createdAt, raw),
		}
		snap.messages = append(snap.messages, msg)
		count++
	}
	return count
}

func parseLikes(records []map[string]any, snap *importSnapshot, fallback time.Time) int {
	count := 0
	for _, record := range records {
		raw, ok := record["like"].(map[string]any)
		if !ok {
			continue
		}
		tweetID := stringValue(raw["tweetId"])
		text := html.UnescapeString(stringValue(raw["fullText"]))
		if tweetID == "" || text == "" {
			continue
		}
		createdAt := twitterSnowflakeTime(tweetID)
		if createdAt.IsZero() {
			createdAt = fallback
		}
		msg := store.MessageMutation{Record: store.MessageRecord{
			ID:                "x:like:" + tweetID,
			GuildID:           GuildID,
			ChannelID:         likesChannelID,
			ChannelName:       "likes",
			AuthorID:          "x:liked",
			AuthorName:        "liked",
			MessageType:       0,
			CreatedAt:         createdAt.UTC().Format(time.RFC3339Nano),
			Content:           text,
			NormalizedContent: text,
			RawJSON:           rawJSON(map[string]any{"platform": "x", "type": "like", "like": raw}),
		}}
		snap.members[memberKey(GuildID, "x:liked")] = store.MemberRecord{GuildID: GuildID, UserID: "x:liked", Username: "liked", DisplayName: "Liked Tweets", RoleIDsJSON: `[]`, RawJSON: `{}`}
		snap.messages = append(snap.messages, msg)
		count++
	}
	return count
}

func parseDMs(records []map[string]any, snap *importSnapshot) (int, int) {
	conversations := 0
	messages := 0
	for _, record := range records {
		rawConv, ok := record["dmConversation"].(map[string]any)
		if !ok {
			continue
		}
		conversationID := stringValue(rawConv["conversationId"])
		rawMessages, ok := rawConv["messages"].([]any)
		if conversationID == "" || !ok {
			continue
		}
		channelID := "x:dm:" + conversationID
		if _, seen := snap.conversations[conversationID]; !seen {
			snap.conversations[conversationID] = struct{}{}
			snap.channels[channelID] = channel(channelID, conversationName(conversationID, snap.account.ID), "dm")
			conversations++
		}
		for _, item := range rawMessages {
			rawMessage, ok := item.(map[string]any)
			if !ok {
				continue
			}
			messageCreate, ok := rawMessage["messageCreate"].(map[string]any)
			if !ok {
				continue
			}
			id := stringValue(messageCreate["id"])
			text := html.UnescapeString(stringValue(messageCreate["text"]))
			createdAt := parseTime(stringValue(messageCreate["createdAt"]))
			senderID := stringValue(messageCreate["senderId"])
			if id == "" || createdAt.IsZero() || (text == "" && len(stringSlice(messageCreate["mediaUrls"])) == 0) {
				continue
			}
			participants := participantsForMessage(conversationID, senderID, stringValue(messageCreate["recipientId"]))
			for _, participant := range participants {
				upsertGenericMember(snap, participant)
			}
			attachments := dmAttachments(id, channelID, senderID, messageCreate)
			snap.messages = append(snap.messages, store.MessageMutation{
				Record: store.MessageRecord{
					ID:                "x:dm:" + id,
					GuildID:           GuildID,
					ChannelID:         channelID,
					ChannelName:       snap.channels[channelID].Name,
					AuthorID:          senderID,
					AuthorName:        authorLabel(senderID, snap.account),
					MessageType:       0,
					CreatedAt:         createdAt.UTC().Format(time.RFC3339Nano),
					Content:           text,
					NormalizedContent: text,
					HasAttachments:    len(attachments) > 0,
					RawJSON:           rawJSON(map[string]any{"platform": "x", "type": "dm", "conversation_id": conversationID, "message": messageCreate}),
				},
				Attachments: attachments,
			})
			messages++
		}
	}
	return conversations, messages
}

func channel(id, name, kind string) store.ChannelRecord {
	return store.ChannelRecord{ID: id, GuildID: GuildID, Kind: kind, Name: name, RawJSON: rawJSON(map[string]any{"platform": "x", "kind": kind})}
}

func memberFromAccount(acc account) store.MemberRecord {
	return store.MemberRecord{
		GuildID:     GuildID,
		UserID:      acc.ID,
		Username:    acc.Username,
		DisplayName: acc.DisplayName,
		JoinedAt:    acc.CreatedAt,
		RoleIDsJSON: `[]`,
		RawJSON:     rawJSON(map[string]any{"platform": "x", "account": acc.Raw, "email": acc.Email}),
	}
}

func upsertGenericMember(snap *importSnapshot, id string) {
	if id == "" {
		return
	}
	if snap.account.ID == id {
		snap.members[memberKey(GuildID, id)] = memberFromAccount(snap.account)
		return
	}
	key := memberKey(GuildID, id)
	if _, ok := snap.members[key]; ok {
		return
	}
	snap.members[key] = store.MemberRecord{GuildID: GuildID, UserID: id, Username: "user-" + shortID(id), RoleIDsJSON: `[]`, RawJSON: rawJSON(map[string]any{"platform": "x"})}
}

func tweetMentions(tweetID, authorID string, createdAt time.Time, raw map[string]any) []store.MentionEventRecord {
	entities, _ := raw["entities"].(map[string]any)
	rawMentions, _ := entities["user_mentions"].([]any)
	var mentions []store.MentionEventRecord
	for _, item := range rawMentions {
		mention, ok := item.(map[string]any)
		if !ok {
			continue
		}
		targetID := stringValue(firstAny(mention["id_str"], mention["id"]))
		targetName := firstString(mention["screen_name"], mention["name"])
		if targetID == "" && targetName == "" {
			continue
		}
		mentions = append(mentions, store.MentionEventRecord{
			MessageID:  "x:tweet:" + tweetID,
			GuildID:    GuildID,
			ChannelID:  tweetsChannelID,
			AuthorID:   authorID,
			TargetType: "user",
			TargetID:   targetID,
			TargetName: targetName,
			EventAt:    createdAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return mentions
}

func hasMedia(raw map[string]any) bool {
	extended, _ := raw["extended_entities"].(map[string]any)
	media, _ := extended["media"].([]any)
	return len(media) > 0
}
