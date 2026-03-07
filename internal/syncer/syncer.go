package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	discordclient "github.com/steipete/discrawl/internal/discord"
	"github.com/steipete/discrawl/internal/store"
)

type Client interface {
	Self(context.Context) (*discordgo.User, error)
	Guilds(context.Context) ([]*discordgo.UserGuild, error)
	Guild(context.Context, string) (*discordgo.Guild, error)
	GuildChannels(context.Context, string) ([]*discordgo.Channel, error)
	ThreadsActive(context.Context, string) ([]*discordgo.Channel, error)
	ThreadsArchived(context.Context, string, bool) ([]*discordgo.Channel, error)
	GuildMembers(context.Context, string) ([]*discordgo.Member, error)
	ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error)
	ChannelMessage(context.Context, string, string) (*discordgo.Message, error)
	Tail(context.Context, discordclient.EventHandler) error
}

type Syncer struct {
	client Client
	store  *store.Store
	logger *slog.Logger
}

type SyncOptions struct {
	Full         bool
	GuildIDs     []string
	ChannelIDs   []string
	Concurrency  int
	Since        time.Time
	Embeddings   bool
	RepairReason string
}

type SyncStats struct {
	Guilds   int `json:"guilds"`
	Channels int `json:"channels"`
	Threads  int `json:"threads"`
	Members  int `json:"members"`
	Messages int `json:"messages"`
}

func New(client Client, store *store.Store, logger *slog.Logger) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Syncer{client: client, store: store, logger: logger}
}

func (s *Syncer) DiscoverGuilds(ctx context.Context) ([]*discordgo.UserGuild, error) {
	return s.client.Guilds(ctx)
}

func (s *Syncer) Sync(ctx context.Context, opts SyncOptions) (SyncStats, error) {
	guilds, err := s.client.Guilds(ctx)
	if err != nil {
		return SyncStats{}, fmt.Errorf("list guilds: %w", err)
	}
	targets := selectGuilds(guilds, opts.GuildIDs)
	stats := SyncStats{}
	for _, guild := range targets {
		one, err := s.syncGuild(ctx, guild.ID, opts)
		if err != nil {
			return stats, err
		}
		stats.Guilds++
		stats.Channels += one.Channels
		stats.Threads += one.Threads
		stats.Members += one.Members
		stats.Messages += one.Messages
	}
	if err := s.store.SetSyncState(ctx, "sync:last_success", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Syncer) syncGuild(ctx context.Context, guildID string, opts SyncOptions) (SyncStats, error) {
	guild, err := s.client.Guild(ctx, guildID)
	if err != nil {
		return SyncStats{}, fmt.Errorf("fetch guild %s: %w", guildID, err)
	}
	rawGuild, _ := json.Marshal(guild)
	if err := s.store.UpsertGuild(ctx, store.GuildRecord{
		ID:      guild.ID,
		Name:    guild.Name,
		Icon:    guild.Icon,
		RawJSON: string(rawGuild),
	}); err != nil {
		return SyncStats{}, err
	}
	stats := SyncStats{}
	channelList, targeted, err := s.channelList(ctx, guildID, opts.ChannelIDs)
	if err != nil {
		return stats, err
	}
	for _, channel := range channelList {
		raw, _ := json.Marshal(channel)
		record := toChannelRecord(channel, string(raw))
		if err := s.store.UpsertChannel(ctx, record); err != nil {
			return stats, err
		}
		stats.Channels++
		if strings.HasPrefix(record.Kind, "thread_") {
			stats.Threads++
		}
	}
	if !targeted {
		members, err := s.client.GuildMembers(ctx, guildID)
		if err == nil {
			converted := make([]store.MemberRecord, 0, len(members))
			for _, member := range members {
				raw, _ := json.Marshal(member)
				roles, _ := json.Marshal(member.Roles)
				converted = append(converted, store.MemberRecord{
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
				})
			}
			if err := s.store.ReplaceMembers(ctx, guildID, converted); err != nil {
				return stats, err
			}
			stats.Members = len(converted)
		} else {
			s.logger.Warn("member crawl failed", "guild_id", guildID, "err", err)
		}
	}
	for _, channel := range channelList {
		if !isMessageChannel(channel) {
			continue
		}
		if len(opts.ChannelIDs) > 0 && !slices.Contains(opts.ChannelIDs, channel.ID) {
			continue
		}
	}
	messageCount, err := s.syncMessageChannels(ctx, guildID, channelList, opts)
	if err != nil {
		return stats, err
	}
	stats.Messages += messageCount
	return stats, nil
}

func (s *Syncer) channelList(ctx context.Context, guildID string, requested []string) ([]*discordgo.Channel, bool, error) {
	if len(requested) > 0 && s.store != nil {
		rows, err := s.store.Channels(ctx, guildID)
		if err != nil {
			return nil, false, err
		}
		if selected := selectStoredChannels(rows, requested); len(selected) > 0 {
			return selected, true, nil
		}
	}
	channels, err := s.client.GuildChannels(ctx, guildID)
	if err != nil {
		return nil, false, fmt.Errorf("fetch channels for guild %s: %w", guildID, err)
	}
	allChannels := make(map[string]*discordgo.Channel, len(channels))
	for _, channel := range channels {
		allChannels[channel.ID] = channel
	}
	for _, channel := range channels {
		if !isThreadParent(channel) {
			continue
		}
		active, err := s.client.ThreadsActive(ctx, channel.ID)
		if err == nil {
			for _, thread := range active {
				allChannels[thread.ID] = thread
			}
		}
		for _, private := range []bool{false, true} {
			archived, err := s.client.ThreadsArchived(ctx, channel.ID, private)
			if err != nil {
				s.logger.Warn("thread archive crawl failed", "channel_id", channel.ID, "private", private, "err", err)
				continue
			}
			for _, thread := range archived {
				allChannels[thread.ID] = thread
			}
		}
	}
	return mapsToSlice(allChannels), false, nil
}

func (s *Syncer) syncMessageChannels(
	ctx context.Context,
	guildID string,
	channels []*discordgo.Channel,
	opts SyncOptions,
) (int, error) {
	messageChannels := make([]*discordgo.Channel, 0, len(channels))
	for _, channel := range channels {
		if !isMessageChannel(channel) {
			continue
		}
		if len(opts.ChannelIDs) > 0 && !slices.Contains(opts.ChannelIDs, channel.ID) {
			continue
		}
		messageChannels = append(messageChannels, channel)
	}
	if len(messageChannels) == 0 {
		return 0, nil
	}
	workers := opts.Concurrency
	if workers <= 1 {
		workers = 1
	}
	if workers == 1 {
		total := 0
		for _, channel := range messageChannels {
			count, err := s.syncChannelMessages(ctx, guildID, channel, opts.Full)
			if err != nil {
				if s.skipUnavailableChannel(ctx, channel, err) {
					continue
				}
				return total, fmt.Errorf("sync channel %s: %w", channel.ID, err)
			}
			total += count
		}
		return total, nil
	}

	type result struct {
		channelID string
		count     int
		err       error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan *discordgo.Channel)
	results := make(chan result, len(messageChannels))
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for channel := range jobs {
				if ctx.Err() != nil {
					return
				}
				count, err := s.syncChannelMessages(ctx, guildID, channel, opts.Full)
				if err != nil && s.skipUnavailableChannel(ctx, channel, err) {
					err = nil
				}
				select {
				case results <- result{channelID: channel.ID, count: count, err: err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					cancel()
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, channel := range messageChannels {
			select {
			case jobs <- channel:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	total := 0
	var firstErr error
	for result := range results {
		total += result.count
		if result.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("sync channel %s: %w", result.channelID, result.err)
		}
	}
	return total, firstErr
}

func (s *Syncer) skipUnavailableChannel(ctx context.Context, channel *discordgo.Channel, err error) bool {
	if !isMissingAccess(err) {
		return false
	}
	s.logger.Warn("channel message crawl skipped", "channel_id", channel.ID, "err", err)
	if s.store != nil {
		_ = s.store.SetSyncState(ctx, "channel:"+channel.ID+":unavailable", "missing_access")
	}
	return true
}

func (s *Syncer) syncChannelMessages(ctx context.Context, guildID string, channel *discordgo.Channel, full bool) (int, error) {
	scope := "channel:" + channel.ID + ":latest_message_id"
	latest, err := s.store.GetSyncState(ctx, scope)
	if err != nil {
		return 0, err
	}
	messageCount := 0
	newest := latest
	if full || latest == "" {
		before := ""
		for {
			page, err := s.client.ChannelMessages(ctx, channel.ID, 100, before, "")
			if err != nil {
				return messageCount, err
			}
			if len(page) == 0 {
				break
			}
			mutations := make([]store.MessageMutation, 0, len(page))
			if newest == "" {
				newest = maxSnowflake(newest, page[0].ID)
			}
			for _, message := range page {
				record := toMessageRecord(message, channel.Name)
				mutations = append(mutations, store.MessageMutation{
					Record:      record,
					EventType:   "upsert",
					PayloadJSON: record.RawJSON,
				})
				newest = maxSnowflake(newest, message.ID)
				messageCount++
			}
			if err := s.store.UpsertMessages(ctx, mutations); err != nil {
				return messageCount, err
			}
			before = page[len(page)-1].ID
			if len(page) < 100 {
				break
			}
		}
	} else {
		after := latest
		for {
			page, err := s.client.ChannelMessages(ctx, channel.ID, 100, "", after)
			if err != nil {
				return messageCount, err
			}
			if len(page) == 0 {
				break
			}
			mutations := make([]store.MessageMutation, 0, len(page))
			for _, message := range page {
				record := toMessageRecord(message, channel.Name)
				mutations = append(mutations, store.MessageMutation{
					Record:      record,
					EventType:   "upsert",
					PayloadJSON: record.RawJSON,
				})
				after = maxSnowflake(after, message.ID)
				newest = maxSnowflake(newest, message.ID)
				messageCount++
			}
			if err := s.store.UpsertMessages(ctx, mutations); err != nil {
				return messageCount, err
			}
			if len(page) < 100 {
				break
			}
		}
	}
	if newest != "" || full || latest != "" {
		if err := s.store.SetSyncState(ctx, scope, newest); err != nil {
			return messageCount, err
		}
	}
	return messageCount, nil
}

func (s *Syncer) RunTail(ctx context.Context, guildIDs []string, repairEvery time.Duration) error {
	handler := &tailHandler{
		guilds: makeGuildSet(guildIDs),
		store:  s.store,
		client: s.client,
	}
	if repairEvery <= 0 {
		return s.client.Tail(ctx, handler)
	}
	errCh := make(chan error, 2)
	go func() {
		errCh <- s.client.Tail(ctx, handler)
	}()
	ticker := time.NewTicker(repairEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case <-ticker.C:
			if _, err := s.Sync(ctx, SyncOptions{GuildIDs: guildIDs, Full: false, RepairReason: "tail_repair"}); err != nil {
				s.logger.Warn("repair sync failed", "err", err)
			}
		}
	}
}

type tailHandler struct {
	guilds map[string]struct{}
	store  *store.Store
	client Client
}

func (t *tailHandler) OnMessageCreate(ctx context.Context, msg *discordgo.Message) error {
	if !t.allowGuild(msg.GuildID) {
		return nil
	}
	if err := t.store.UpsertMessage(ctx, toMessageRecord(msg, "")); err != nil {
		return err
	}
	if err := t.store.AppendMessageEvent(ctx, msg.GuildID, msg.ChannelID, msg.ID, "create", msg); err != nil {
		return err
	}
	if err := t.store.SetSyncState(ctx, "tail:last_event", msg.ID); err != nil {
		return err
	}
	return t.store.SetSyncState(ctx, "channel:"+msg.ChannelID+":latest_message_id", msg.ID)
}

func (t *tailHandler) OnMessageUpdate(ctx context.Context, msg *discordgo.Message) error {
	if !t.allowGuild(msg.GuildID) {
		return nil
	}
	if err := t.store.UpsertMessage(ctx, toMessageRecord(msg, "")); err != nil {
		return err
	}
	if err := t.store.AppendMessageEvent(ctx, msg.GuildID, msg.ChannelID, msg.ID, "update", msg); err != nil {
		return err
	}
	return t.store.SetSyncState(ctx, "tail:last_event", msg.ID)
}

func (t *tailHandler) OnMessageDelete(ctx context.Context, evt *discordgo.MessageDelete) error {
	if !t.allowGuild(evt.GuildID) {
		return nil
	}
	if err := t.store.MarkMessageDeleted(ctx, evt.GuildID, evt.ChannelID, evt.ID, evt); err != nil {
		return err
	}
	return t.store.SetSyncState(ctx, "tail:last_event", evt.ID)
}

func (t *tailHandler) OnChannelUpsert(ctx context.Context, channel *discordgo.Channel) error {
	if !t.allowGuild(channel.GuildID) {
		return nil
	}
	raw, _ := json.Marshal(channel)
	return t.store.UpsertChannel(ctx, toChannelRecord(channel, string(raw)))
}

func (t *tailHandler) OnMemberUpsert(ctx context.Context, guildID string, member *discordgo.Member) error {
	if !t.allowGuild(guildID) || member == nil || member.User == nil {
		return nil
	}
	raw, _ := json.Marshal(member)
	roles, _ := json.Marshal(member.Roles)
	return t.store.UpsertMember(ctx, store.MemberRecord{
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
	})
}

func (t *tailHandler) OnMemberDelete(ctx context.Context, guildID, userID string) error {
	if !t.allowGuild(guildID) {
		return nil
	}
	return t.store.DeleteMember(ctx, guildID, userID)
}

func (t *tailHandler) allowGuild(guildID string) bool {
	if len(t.guilds) == 0 {
		return true
	}
	_, ok := t.guilds[guildID]
	return ok
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

func mapsToSlice(in map[string]*discordgo.Channel) []*discordgo.Channel {
	out := make([]*discordgo.Channel, 0, len(in))
	for _, channel := range in {
		out = append(out, channel)
	}
	slices.SortFunc(out, func(a, b *discordgo.Channel) int {
		switch {
		case a.Position < b.Position:
			return -1
		case a.Position > b.Position:
			return 1
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
	return out
}

func selectStoredChannels(rows []store.ChannelRow, requested []string) []*discordgo.Channel {
	if len(rows) == 0 || len(requested) == 0 {
		return nil
	}
	set := makeGuildSet(requested)
	out := make([]*discordgo.Channel, 0, len(requested))
	for _, row := range rows {
		if _, ok := set[row.ID]; !ok {
			continue
		}
		channelType := channelTypeFromKind(row.Kind)
		var threadMeta *discordgo.ThreadMetadata
		if strings.HasPrefix(row.Kind, "thread_") {
			threadMeta = &discordgo.ThreadMetadata{
				Archived:         row.IsArchived,
				Locked:           row.IsLocked,
				ArchiveTimestamp: row.ArchiveTimestamp,
			}
		}
		out = append(out, &discordgo.Channel{
			ID:             row.ID,
			GuildID:        row.GuildID,
			ParentID:       row.ParentID,
			Name:           row.Name,
			Topic:          row.Topic,
			Position:       row.Position,
			NSFW:           row.IsNSFW,
			Type:           channelType,
			ThreadMetadata: threadMeta,
		})
	}
	slices.SortFunc(out, func(a, b *discordgo.Channel) int {
		switch {
		case a.Position < b.Position:
			return -1
		case a.Position > b.Position:
			return 1
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
	return out
}

func isThreadParent(channel *discordgo.Channel) bool {
	if channel == nil {
		return false
	}
	switch channel.Type {
	case discordgo.ChannelTypeGuildText, discordgo.ChannelTypeGuildNews, discordgo.ChannelTypeGuildForum:
		return true
	default:
		return false
	}
}

func isMessageChannel(channel *discordgo.Channel) bool {
	if channel == nil {
		return false
	}
	switch channel.Type {
	case discordgo.ChannelTypeGuildText,
		discordgo.ChannelTypeGuildNews,
		discordgo.ChannelTypeGuildPublicThread,
		discordgo.ChannelTypeGuildPrivateThread,
		discordgo.ChannelTypeGuildNewsThread:
		return true
	default:
		return false
	}
}

func toChannelRecord(channel *discordgo.Channel, raw string) store.ChannelRecord {
	record := store.ChannelRecord{
		ID:       channel.ID,
		GuildID:  channel.GuildID,
		ParentID: channel.ParentID,
		Kind:     channelKind(channel),
		Name:     channel.Name,
		Topic:    channel.Topic,
		Position: channel.Position,
		IsNSFW:   channel.NSFW,
		RawJSON:  raw,
	}
	if channel.ThreadMetadata != nil {
		record.IsArchived = channel.ThreadMetadata.Archived
		record.IsLocked = channel.ThreadMetadata.Locked
		record.ArchiveTimestamp = channel.ThreadMetadata.ArchiveTimestamp.Format(time.RFC3339Nano)
		record.ThreadParentID = channel.ParentID
	}
	if channel.Type == discordgo.ChannelTypeGuildPrivateThread {
		record.IsPrivateThread = true
	}
	return record
}

func toMessageRecord(message *discordgo.Message, channelName string) store.MessageRecord {
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
		NormalizedContent: normalizeMessage(message),
		ReplyToMessageID:  replyTo,
		Pinned:            message.Pinned,
		HasAttachments:    len(message.Attachments) > 0,
		RawJSON:           string(raw),
	}
}

func normalizeMessage(message *discordgo.Message) string {
	parts := []string{strings.TrimSpace(message.Content)}
	for _, attachment := range message.Attachments {
		if attachment != nil && attachment.Filename != "" {
			parts = append(parts, attachment.Filename)
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

func channelKind(channel *discordgo.Channel) string {
	switch channel.Type {
	case discordgo.ChannelTypeGuildCategory:
		return "category"
	case discordgo.ChannelTypeGuildText:
		return "text"
	case discordgo.ChannelTypeGuildNews:
		return "announcement"
	case discordgo.ChannelTypeGuildForum:
		return "forum"
	case discordgo.ChannelTypeGuildPublicThread:
		return "thread_public"
	case discordgo.ChannelTypeGuildPrivateThread:
		return "thread_private"
	case discordgo.ChannelTypeGuildNewsThread:
		return "thread_announcement"
	case discordgo.ChannelTypeGuildVoice:
		return "voice"
	default:
		return fmt.Sprintf("type_%d", channel.Type)
	}
}

func channelTypeFromKind(kind string) discordgo.ChannelType {
	switch kind {
	case "category":
		return discordgo.ChannelTypeGuildCategory
	case "text":
		return discordgo.ChannelTypeGuildText
	case "announcement":
		return discordgo.ChannelTypeGuildNews
	case "forum":
		return discordgo.ChannelTypeGuildForum
	case "thread_public":
		return discordgo.ChannelTypeGuildPublicThread
	case "thread_private":
		return discordgo.ChannelTypeGuildPrivateThread
	case "thread_announcement":
		return discordgo.ChannelTypeGuildNewsThread
	case "voice":
		return discordgo.ChannelTypeGuildVoice
	default:
		return discordgo.ChannelTypeGuildText
	}
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

func isMissingAccess(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "403 Forbidden") || strings.Contains(msg, "Missing Access")
}
