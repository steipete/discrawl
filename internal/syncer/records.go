package syncer

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/steipete/discrawl/internal/store"
)

func toMemberRecord(guildID string, member *discordgo.Member) store.MemberRecord {
	raw, _ := json.Marshal(member)
	roles, _ := json.Marshal(member.Roles)
	return store.MemberRecord{
		GuildID:       guildID,
		UserID:        member.User.ID,
		Username:      member.User.Username,
		GlobalName:    member.User.GlobalName,
		DisplayName:   displayName(member),
		Nick:          member.Nick,
		Discriminator: member.User.Discriminator,
		Avatar:        member.Avatar,
		Bot:           member.User.Bot,
		JoinedAt:      member.JoinedAt.Format(time.RFC3339Nano),
		RoleIDsJSON:   string(roles),
		RawJSON:       string(raw),
	}
}

func toMessageRecord(message *discordgo.Message, channelName, normalizedContent string) store.MessageRecord {
	raw, _ := json.Marshal(message)
	authorID := ""
	authorName := ""
	if message.Author != nil {
		authorID = message.Author.ID
		authorName = strings.TrimSpace(message.Author.GlobalName)
		if authorName == "" {
			authorName = message.Author.Username
		}
	}
	replyTo := ""
	if message.MessageReference != nil {
		replyTo = message.MessageReference.MessageID
	}
	editedAt := ""
	if message.EditedTimestamp != nil {
		editedAt = message.EditedTimestamp.UTC().Format(time.RFC3339Nano)
	}
	return store.MessageRecord{
		ID:                message.ID,
		GuildID:           message.GuildID,
		ChannelID:         message.ChannelID,
		ChannelName:       channelName,
		AuthorID:          authorID,
		AuthorName:        authorName,
		MessageType:       int(message.Type),
		CreatedAt:         message.Timestamp.UTC().Format(time.RFC3339Nano),
		EditedAt:          editedAt,
		Content:           message.Content,
		NormalizedContent: normalizedContent,
		ReplyToMessageID:  replyTo,
		Pinned:            message.Pinned,
		HasAttachments:    len(message.Attachments) > 0,
		RawJSON:           string(raw),
	}
}

func normalizeMessage(message *discordgo.Message) string {
	return normalizeMessageParts(message, nil)
}

func normalizeMessageParts(message *discordgo.Message, attachmentParts []string) string {
	parts := []string{strings.TrimSpace(message.Content)}
	if len(attachmentParts) != 0 {
		parts = append(parts, attachmentParts...)
	} else {
		for _, attachment := range message.Attachments {
			if attachment != nil && attachment.Filename != "" {
				parts = append(parts, attachment.Filename)
			}
		}
	}
	for _, embed := range message.Embeds {
		if embed == nil {
			continue
		}
		if embed.Title != "" {
			parts = append(parts, embed.Title)
		}
		if embed.Description != "" {
			parts = append(parts, embed.Description)
		}
	}
	if message.ReferencedMessage != nil && message.ReferencedMessage.Content != "" {
		parts = append(parts, "reply:"+message.ReferencedMessage.Content)
	}
	if message.Poll != nil {
		parts = append(parts, message.Poll.Question.Text)
		for _, answer := range message.Poll.Answers {
			if answer.Media != nil {
				parts = append(parts, answer.Media.Text)
			}
		}
	}
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	return strings.Join(filtered, "\n")
}

func displayName(member *discordgo.Member) string {
	if member == nil || member.User == nil {
		return ""
	}
	if member.Nick != "" {
		return member.Nick
	}
	if member.User.GlobalName != "" {
		return member.User.GlobalName
	}
	return member.User.Username
}

func maxSnowflake(current, candidate string) string {
	if current == "" {
		return candidate
	}
	if candidate == "" {
		return current
	}
	a, errA := strconv.ParseUint(current, 10, 64)
	b, errB := strconv.ParseUint(candidate, 10, 64)
	if errA != nil || errB != nil {
		if candidate > current {
			return candidate
		}
		return current
	}
	if b > a {
		return candidate
	}
	return current
}

func channelLatestScope(channelID string) string {
	return "channel:" + channelID + ":latest_message_id"
}

func channelBackfillScope(channelID string) string {
	return "channel:" + channelID + ":backfill_before_id"
}

func channelHistoryCompleteScope(channelID string) string {
	return "channel:" + channelID + ":history_complete"
}

func makeGuildSet(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			set[id] = struct{}{}
		}
	}
	return set
}

func selectGuilds(all []*discordgo.UserGuild, requested []string) []*discordgo.UserGuild {
	if len(requested) == 0 {
		return all
	}
	set := makeGuildSet(requested)
	var out []*discordgo.UserGuild
	for _, guild := range all {
		if _, ok := set[guild.ID]; ok {
			out = append(out, guild)
		}
	}
	return out
}
