package twitterarchive

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/steipete/discrawl/internal/store"
)

func classify(path string) string {
	normalized := filepath.ToSlash(path)
	base := filepath.Base(normalized)
	switch {
	case strings.HasSuffix(normalized, "/data/account.js") || normalized == "data/account.js":
		return "account"
	case (base == "tweets.js" || strings.HasPrefix(base, "tweets-part")) && strings.HasSuffix(base, ".js"):
		return "tweets"
	case base == "like.js" || base == "likes.js":
		return "likes"
	case (base == "direct-messages.js" || base == "direct-messages-group.js") && strings.HasSuffix(base, ".js"):
		return "dms"
	default:
		return ""
	}
}

func readZipText(file *zip.File) (string, error) {
	rc, err := file.Open()
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	var b strings.Builder
	if _, err := io.Copy(&b, rc); err != nil {
		return "", err
	}
	return b.String(), nil
}

func parseArchiveArray(content string) ([]map[string]any, error) {
	idx := strings.Index(content, "=")
	if idx < 0 {
		return nil, errors.New("missing assignment")
	}
	payload := strings.TrimSuffix(strings.TrimSpace(content[idx+1:]), ";")
	dec := json.NewDecoder(strings.NewReader(payload))
	dec.UseNumber()
	var records []map[string]any
	if err := dec.Decode(&records); err != nil {
		return nil, err
	}
	return records, nil
}

func dmAttachments(messageID, channelID, authorID string, raw map[string]any) []store.AttachmentRecord {
	urls := stringSlice(raw["mediaUrls"])
	attachments := make([]store.AttachmentRecord, 0, len(urls))
	for i, url := range urls {
		attachments = append(attachments, store.AttachmentRecord{
			AttachmentID: fmt.Sprintf("x:dm:%s:media:%d", messageID, i),
			MessageID:    "x:dm:" + messageID,
			GuildID:      GuildID,
			ChannelID:    channelID,
			AuthorID:     authorID,
			Filename:     filepath.Base(url),
			URL:          url,
		})
	}
	return attachments
}

func participantsForMessage(conversationID, senderID, recipientID string) []string {
	set := map[string]struct{}{}
	for _, value := range []string{senderID, recipientID} {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	for _, part := range strings.Split(conversationID, "-") {
		if part != "" {
			set[part] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func conversationName(conversationID, selfID string) string {
	parts := strings.Split(conversationID, "-")
	if len(parts) == 2 {
		for _, part := range parts {
			if part != "" && part != selfID {
				return "dm-" + shortID(part)
			}
		}
	}
	return "group-" + shortID(conversationID)
}

func authorLabel(id string, acc account) string {
	if id == acc.ID && acc.Username != "" {
		return acc.Username
	}
	return "user-" + shortID(id)
}

func parseTime(value string) time.Time {
	value = strings.TrimSpace(value)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "Mon Jan 02 15:04:05 -0700 2006"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t
		}
	}
	return time.Time{}
}

func twitterSnowflakeTime(id string) time.Time {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}
	}
	const twitterEpochMs = int64(1288834974657)
	ms := (n >> 22) + twitterEpochMs
	return time.UnixMilli(ms).UTC()
}

func sortedFiles(files []*zip.File) []*zip.File {
	out := append([]*zip.File(nil), files...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sortedChannels(channels map[string]store.ChannelRecord) []store.ChannelRecord {
	out := make([]store.ChannelRecord, 0, len(channels))
	for _, ch := range channels {
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func sortedMembers(members map[string]store.MemberRecord) []store.MemberRecord {
	out := make([]store.MemberRecord, 0, len(members))
	for _, member := range members {
		out = append(out, member)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

func memberKey(guildID, userID string) string { return guildID + "\x00" + userID }

func prefixedTweetID(id string) string {
	if id == "" {
		return ""
	}
	return "x:tweet:" + id
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[len(id)-8:]
}

func rawJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return `{}`
	}
	return string(data)
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	default:
		return ""
	}
}

func firstString(values ...any) string {
	for _, value := range values {
		if s := stringValue(value); s != "" {
			return s
		}
	}
	return ""
}

func firstAny(values ...any) any {
	for _, value := range values {
		if stringValue(value) != "" {
			return value
		}
	}
	return nil
}

func nestedString(raw map[string]any, key, nested string) string {
	child, _ := raw[key].(map[string]any)
	return stringValue(child[nested])
}

func stringSlice(value any) []string {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s := stringValue(item); s != "" {
			out = append(out, s)
		}
	}
	return out
}
