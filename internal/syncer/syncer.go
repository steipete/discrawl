package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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
	client                Client
	store                 *store.Store
	logger                *slog.Logger
	attachmentTextEnabled bool
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
	return &Syncer{
		client:                client,
		store:                 store,
		logger:                logger,
		attachmentTextEnabled: true,
	}
}

func (s *Syncer) SetAttachmentTextEnabled(enabled bool) {
	s.attachmentTextEnabled = enabled
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
				converted = append(converted, toMemberRecord(guildID, member))
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
