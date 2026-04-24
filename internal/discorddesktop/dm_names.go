package discorddesktop

import (
	"encoding/json"
	"sort"
	"strings"
)

type userLabel struct {
	Name     string
	Priority int
}

func collectUserLabel(snap snapshot, raw map[string]any) {
	id := stringField(raw, "id")
	if !looksSnowflake(id) || !looksUserObject(raw) {
		return
	}
	name, priority := userObjectLabel(raw)
	if name == "" {
		return
	}
	if existing, ok := snap.userLabels[id]; !ok || priority > existing.Priority || existing.Name == "" {
		snap.userLabels[id] = userLabel{Name: name, Priority: priority}
	}
}

func looksUserObject(raw map[string]any) bool {
	for _, key := range []string{"username", "global_name", "display_name", "discriminator", "avatar", "bot", "public_flags"} {
		if _, ok := raw[key]; ok {
			return true
		}
	}
	return false
}

func userObjectLabel(raw map[string]any) (string, int) {
	if name := stringField(raw, "global_name"); name != "" {
		return name, 3
	}
	if name := stringField(raw, "display_name"); name != "" {
		return name, 2
	}
	if name := stringField(raw, "username"); name != "" {
		return name, 1
	}
	return "", 0
}

func inferDirectMessageNames(snap snapshot) {
	authorChannels := map[string]map[string]struct{}{}
	channelAuthors := map[string]map[string]int{}
	for id, msg := range snap.messages {
		if label, ok := snap.userLabels[msg.Record.AuthorID]; ok && shouldUseUserLabel(msg.Record.AuthorName, label) {
			msg.Record.AuthorName = label.Name
			msg.Record.RawJSON = withRawAuthorLabel(msg.Record.RawJSON, msg.Record.AuthorID, label)
			msg.PayloadJSON = withRawAuthorLabel(msg.PayloadJSON, msg.Record.AuthorID, label)
			snap.messages[id] = msg
		}
		if msg.Record.GuildID != DirectMessageGuildID || msg.Record.AuthorID == "" {
			continue
		}
		if authorChannels[msg.Record.AuthorID] == nil {
			authorChannels[msg.Record.AuthorID] = map[string]struct{}{}
		}
		authorChannels[msg.Record.AuthorID][msg.Record.ChannelID] = struct{}{}
		if channelAuthors[msg.Record.ChannelID] == nil {
			channelAuthors[msg.Record.ChannelID] = map[string]int{}
		}
		channelAuthors[msg.Record.ChannelID][msg.Record.AuthorID]++
	}

	selfID := mostRepeatedDirectMessageAuthor(authorChannels)
	for id, channel := range snap.channels {
		if channel.GuildID != DirectMessageGuildID || !isFallbackChannelName(channel.Name, id) {
			continue
		}
		name := directMessageChannelName(channelAuthors[id], snap.userLabels, selfID)
		if name == "" {
			continue
		}
		channel.Name = name
		channel.RawJSON = withRawChannelName(channel.RawJSON, id, channel.GuildID, name, channel.Kind)
		snap.channels[id] = channel
	}
}

func shouldUseUserLabel(current string, label userLabel) bool {
	if label.Name == "" || current == label.Name {
		return false
	}
	return current == "" || label.Priority >= 2
}

func mostRepeatedDirectMessageAuthor(authorChannels map[string]map[string]struct{}) string {
	selfID := ""
	selfChannels := 1
	for authorID, channels := range authorChannels {
		if len(channels) > selfChannels {
			selfID = authorID
			selfChannels = len(channels)
		}
	}
	return selfID
}

func directMessageChannelName(authorCounts map[string]int, labels map[string]userLabel, selfID string) string {
	candidates := []string{}
	bestID := ""
	bestCount := -1
	for authorID, count := range authorCounts {
		label, ok := labels[authorID]
		if !ok || label.Name == "" {
			continue
		}
		if authorID == selfID && len(authorCounts) > 1 {
			continue
		}
		if len(authorCounts) > 2 {
			candidates = append(candidates, label.Name)
			continue
		}
		if count > bestCount || (count == bestCount && label.Priority > labels[bestID].Priority) {
			bestID = authorID
			bestCount = count
		}
	}
	if len(candidates) > 0 {
		sort.Strings(candidates)
		return strings.Join(candidates, ", ")
	}
	if bestID == "" {
		return ""
	}
	return labels[bestID].Name
}

func isFallbackChannelName(name, id string) bool {
	name = strings.TrimSpace(name)
	return name == "" || name == "channel-"+shortID(id) || name == "dm-"+shortID(id)
}

func withRawChannelName(rawJSON, id, guildID, name, kind string) string {
	raw := map[string]any{}
	if rawJSON != "" {
		_ = json.Unmarshal([]byte(rawJSON), &raw)
	}
	raw["id"] = id
	raw["guild_id"] = guildID
	raw["name"] = name
	raw["kind"] = kind
	raw["source"] = "discord_desktop"
	body, err := json.Marshal(raw)
	if err != nil {
		return rawJSON
	}
	return string(body)
}

func withRawAuthorLabel(rawJSON, authorID string, label userLabel) string {
	if rawJSON == "" || authorID == "" || label.Name == "" {
		return rawJSON
	}
	raw := map[string]any{}
	if err := json.Unmarshal([]byte(rawJSON), &raw); err != nil {
		return rawJSON
	}
	author, _ := raw["author"].(map[string]any)
	if author == nil {
		author = map[string]any{}
	}
	author["id"] = authorID
	if label.Priority >= 2 {
		author["global_name"] = label.Name
	} else {
		author["username"] = label.Name
	}
	raw["author"] = author
	body, err := json.Marshal(raw)
	if err != nil {
		return rawJSON
	}
	return string(body)
}

func sanitizedRawAuthor(raw map[string]any, authorID string) map[string]any {
	author, _ := raw["author"].(map[string]any)
	out := map[string]any{}
	if authorID != "" {
		out["id"] = authorID
	}
	for _, key := range []string{"username", "global_name", "display_name"} {
		if value := stringField(author, key); value != "" {
			out[key] = value
		}
	}
	return out
}
