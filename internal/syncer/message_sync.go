package syncer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/steipete/discrawl/internal/store"
)

func (s *Syncer) syncMessageChannels(
	ctx context.Context,
	guildID string,
	channels []*discordgo.Channel,
	opts SyncOptions,
) (int, error) {
	messageChannels := filterMessageChannels(channels, opts.ChannelIDs)
	if len(messageChannels) == 0 {
		return 0, nil
	}
	progress := newMessageSyncProgress(s, guildID, len(messageChannels), opts)
	workers := opts.Concurrency
	if workers <= 1 {
		return s.syncMessageChannelsSerial(ctx, guildID, messageChannels, opts, progress)
	}
	return s.syncMessageChannelsConcurrent(ctx, guildID, messageChannels, opts, workers, progress)
}

func filterMessageChannels(channels []*discordgo.Channel, requested []string) []*discordgo.Channel {
	requestedSet := makeGuildSet(requested)
	channelByID := make(map[string]*discordgo.Channel, len(channels))
	for _, channel := range channels {
		if channel != nil {
			channelByID[channel.ID] = channel
		}
	}
	out := make([]*discordgo.Channel, 0, len(channels))
	for _, channel := range channels {
		if !isMessageChannel(channel) {
			continue
		}
		if len(requestedSet) > 0 && !requestedMessageTarget(channel, channelByID, requestedSet) {
			continue
		}
		out = append(out, channel)
	}
	return out
}

func requestedMessageTarget(channel *discordgo.Channel, channelByID map[string]*discordgo.Channel, requested map[string]struct{}) bool {
	if channel == nil {
		return false
	}
	if _, ok := requested[channel.ID]; ok {
		return true
	}
	if !isThreadChannel(channel) {
		return false
	}
	if _, ok := requested[channel.ParentID]; !ok {
		return false
	}
	parent := channelByID[channel.ParentID]
	return parent != nil && parent.Type == discordgo.ChannelTypeGuildForum
}

func (s *Syncer) syncMessageChannelsSerial(ctx context.Context, guildID string, channels []*discordgo.Channel, opts SyncOptions, progress *messageSyncProgress) (int, error) {
	total := 0
	for _, channel := range channels {
		count, err := s.syncChannelMessages(ctx, guildID, channel, opts.Full, opts.Embeddings, opts.Since)
		total += count
		if err != nil {
			if s.skipSyncError(ctx, channel, err) {
				continue
			}
			return total, fmt.Errorf("sync channel %s: %w", channel.ID, err)
		}
		if err := s.clearUnavailableChannel(ctx, channel.ID); err != nil {
			return total, err
		}
		progress.record(channel, count)
	}
	return total, nil
}

func (s *Syncer) syncMessageChannelsConcurrent(
	ctx context.Context,
	guildID string,
	channels []*discordgo.Channel,
	opts SyncOptions,
	workers int,
	progress *messageSyncProgress,
) (int, error) {
	type result struct {
		channelID string
		channel   *discordgo.Channel
		count     int
		err       error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan *discordgo.Channel)
	results := make(chan result, len(channels))
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for channel := range jobs {
				if ctx.Err() != nil {
					return
				}
				count, err := s.syncChannelMessages(ctx, guildID, channel, opts.Full, opts.Embeddings, opts.Since)
				succeeded := err == nil
				if err != nil && s.skipSyncError(ctx, channel, err) {
					err = nil
				}
				if succeeded {
					err = s.clearUnavailableChannel(ctx, channel.ID)
				}
				select {
				case results <- result{channelID: channel.ID, channel: channel, count: count, err: err}:
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
		for _, channel := range channels {
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
		progress.record(result.channel, result.count)
		if result.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("sync channel %s: %w", result.channelID, result.err)
		}
	}
	return total, firstErr
}

func (s *Syncer) clearUnavailableChannel(ctx context.Context, channelID string) error {
	if s.store == nil || channelID == "" {
		return nil
	}
	return s.store.DeleteSyncState(ctx, "channel:"+channelID+":unavailable")
}

func (s *Syncer) syncChannelMessages(ctx context.Context, guildID string, channel *discordgo.Channel, full bool, embeddings bool, since time.Time) (int, error) {
	state, err := s.loadChannelSyncState(ctx, channel.ID)
	if err != nil {
		return 0, err
	}
	if full {
		if err := s.seedChannelSyncState(ctx, channel.ID, &state); err != nil {
			return 0, err
		}
		if shouldSkipChannelSync(channel, state) {
			return 0, nil
		}
		return s.syncFullChannelHistory(ctx, channel, state, embeddings, since)
	}
	if shouldSkipChannelSync(channel, state) {
		return 0, nil
	}
	return s.syncIncrementalChannelHistory(ctx, channel, state, embeddings, since)
}

type channelSyncState struct {
	Latest           string
	StoredLatest     string
	BackfillCursor   string
	BackfillComplete bool
}

func shouldSkipChannelSync(channel *discordgo.Channel, state channelSyncState) bool {
	if !state.BackfillComplete || channel == nil {
		return false
	}
	if channel.LastMessageID == "" {
		return state.Latest == ""
	}
	if state.Latest == "" {
		return false
	}
	return maxSnowflake(state.Latest, channel.LastMessageID) == state.Latest
}

func (s *Syncer) loadChannelSyncState(ctx context.Context, channelID string) (channelSyncState, error) {
	latest, err := s.store.GetSyncState(ctx, channelLatestScope(channelID))
	if err != nil {
		return channelSyncState{}, err
	}
	backfillCursor, err := s.store.GetSyncState(ctx, channelBackfillScope(channelID))
	if err != nil {
		return channelSyncState{}, err
	}
	backfillComplete, err := s.store.GetSyncState(ctx, channelHistoryCompleteScope(channelID))
	if err != nil {
		return channelSyncState{}, err
	}
	return channelSyncState{
		Latest:           latest,
		StoredLatest:     latest,
		BackfillCursor:   backfillCursor,
		BackfillComplete: backfillComplete != "",
	}, nil
}

func (s *Syncer) seedChannelSyncState(ctx context.Context, channelID string, state *channelSyncState) error {
	if state.Latest == "" || state.BackfillCursor == "" {
		oldestStored, newestStored, err := s.store.ChannelMessageBounds(ctx, channelID)
		if err != nil {
			return err
		}
		if state.Latest == "" && newestStored != "" {
			state.Latest = newestStored
		}
		if state.BackfillCursor == "" && oldestStored != "" {
			state.BackfillCursor = oldestStored
		}
	}
	if state.StoredLatest != "" && state.BackfillCursor == "" && !state.BackfillComplete {
		if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channelID), "1"); err != nil {
			return err
		}
		state.BackfillComplete = true
	}
	return nil
}

func (s *Syncer) syncFullChannelHistory(ctx context.Context, channel *discordgo.Channel, state channelSyncState, embeddings bool, since time.Time) (int, error) {
	messageCount := 0
	newest := state.Latest
	if state.Latest != "" {
		count, latest, err := s.syncForwardPages(ctx, channel, state.Latest, channel.Name, embeddings)
		messageCount += count
		if err != nil {
			return messageCount, err
		}
		newest = maxSnowflake(newest, latest)
		if err := s.store.SetSyncState(ctx, channelLatestScope(channel.ID), newest); err != nil {
			return messageCount, err
		}
	}
	if !state.BackfillComplete {
		before := state.BackfillCursor
		if before == "" && state.Latest != "" {
			before = state.Latest
		}
		count, latest, err := s.syncBackfillPages(ctx, channel, before, channel.Name, embeddings, since)
		messageCount += count
		newest = maxSnowflake(newest, latest)
		if err != nil {
			return messageCount, err
		}
	}
	if newest != "" || state.Latest != "" || !state.BackfillComplete {
		if err := s.store.SetSyncState(ctx, channelLatestScope(channel.ID), newest); err != nil {
			return messageCount, err
		}
	}
	return messageCount, nil
}

func (s *Syncer) syncIncrementalChannelHistory(ctx context.Context, channel *discordgo.Channel, state channelSyncState, embeddings bool, since time.Time) (int, error) {
	if state.Latest == "" {
		return s.bootstrapChannelHistory(ctx, channel, embeddings, since)
	}
	count, newest, err := s.syncForwardPages(ctx, channel, state.Latest, channel.Name, embeddings)
	if err != nil {
		return count, err
	}
	if newest == "" && state.Latest == "" {
		return count, nil
	}
	if err := s.store.SetSyncState(ctx, channelLatestScope(channel.ID), maxSnowflake(state.Latest, newest)); err != nil {
		return count, err
	}
	return count, nil
}

func (s *Syncer) bootstrapChannelHistory(ctx context.Context, channel *discordgo.Channel, embeddings bool, since time.Time) (int, error) {
	messageCount := 0
	before := ""
	newest := ""
	for {
		page, err := s.client.ChannelMessages(ctx, channel.ID, 100, before, "")
		if err != nil {
			return messageCount, err
		}
		if len(page) == 0 {
			break
		}
		eligible, reachedSince := filterMessagesSince(page, since)
		pageNewest, err := s.persistMessagePage(ctx, eligible, channel.Name, embeddings)
		if err != nil {
			return messageCount, err
		}
		newest = maxSnowflake(newest, pageNewest)
		messageCount += len(eligible)
		if reachedSince {
			if len(eligible) > 0 {
				before = eligible[len(eligible)-1].ID
				if err := s.store.SetSyncState(ctx, channelBackfillScope(channel.ID), before); err != nil {
					return messageCount, err
				}
			}
			break
		}
		before = page[len(page)-1].ID
		if len(page) < 100 {
			if newest != "" {
				if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channel.ID), "1"); err != nil {
					return messageCount, err
				}
			}
			break
		}
	}
	if newest != "" {
		if err := s.store.SetSyncState(ctx, channelLatestScope(channel.ID), newest); err != nil {
			return messageCount, err
		}
	}
	return messageCount, nil
}

func (s *Syncer) syncForwardPages(ctx context.Context, channel *discordgo.Channel, after, channelName string, embeddings bool) (int, string, error) {
	messageCount := 0
	newest := after
	for {
		page, err := s.client.ChannelMessages(ctx, channel.ID, 100, "", after)
		if err != nil {
			return messageCount, newest, err
		}
		if len(page) == 0 {
			break
		}
		pageNewest, err := s.persistMessagePage(ctx, page, channelName, embeddings)
		if err != nil {
			return messageCount, newest, err
		}
		after = maxSnowflake(after, pageNewest)
		newest = maxSnowflake(newest, pageNewest)
		messageCount += len(page)
		if err := s.store.SetSyncState(ctx, channelLatestScope(channel.ID), newest); err != nil {
			return messageCount, newest, err
		}
		if len(page) < 100 {
			break
		}
	}
	return messageCount, newest, nil
}

func (s *Syncer) syncBackfillPages(ctx context.Context, channel *discordgo.Channel, before, channelName string, embeddings bool, since time.Time) (int, string, error) {
	messageCount := 0
	newest := ""
	for {
		page, err := s.client.ChannelMessages(ctx, channel.ID, 100, before, "")
		if err != nil {
			return messageCount, newest, err
		}
		if len(page) == 0 {
			if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channel.ID), "1"); err != nil {
				return messageCount, newest, err
			}
			break
		}
		eligible, reachedSince := filterMessagesSince(page, since)
		pageNewest, err := s.persistMessagePage(ctx, eligible, channelName, embeddings)
		if err != nil {
			return messageCount, newest, err
		}
		newest = maxSnowflake(newest, pageNewest)
		messageCount += len(eligible)
		if newest != "" {
			if err := s.store.SetSyncState(ctx, channelLatestScope(channel.ID), newest); err != nil {
				return messageCount, newest, err
			}
		}
		if reachedSince {
			if len(eligible) > 0 {
				before = eligible[len(eligible)-1].ID
				if err := s.store.SetSyncState(ctx, channelBackfillScope(channel.ID), before); err != nil {
					return messageCount, newest, err
				}
			}
			break
		}
		before = page[len(page)-1].ID
		if err := s.store.SetSyncState(ctx, channelBackfillScope(channel.ID), before); err != nil {
			return messageCount, newest, err
		}
		if len(page) < 100 {
			if err := s.store.SetSyncState(ctx, channelHistoryCompleteScope(channel.ID), "1"); err != nil {
				return messageCount, newest, err
			}
			break
		}
	}
	return messageCount, newest, nil
}

func (s *Syncer) persistMessagePage(ctx context.Context, messages []*discordgo.Message, channelName string, embeddings bool) (string, error) {
	if len(messages) == 0 {
		return "", nil
	}
	mutations, newest, err := buildMessageMutations(ctx, messages, channelName, embeddings, s.attachmentTextEnabled)
	if err != nil {
		return "", err
	}
	if err := s.store.UpsertMessages(ctx, mutations); err != nil {
		return "", err
	}
	return newest, nil
}

func buildMessageMutations(ctx context.Context, messages []*discordgo.Message, channelName string, embeddings bool, attachmentText bool) ([]store.MessageMutation, string, error) {
	mutations := make([]store.MessageMutation, 0, len(messages))
	newest := ""
	for _, message := range messages {
		mutation, err := buildMessageMutation(ctx, message, channelName, embeddings, attachmentText)
		if err != nil {
			return nil, "", err
		}
		mutations = append(mutations, mutation)
		newest = maxSnowflake(newest, message.ID)
	}
	return mutations, newest, nil
}

func filterMessagesSince(messages []*discordgo.Message, since time.Time) ([]*discordgo.Message, bool) {
	if since.IsZero() || len(messages) == 0 {
		return messages, false
	}
	out := make([]*discordgo.Message, 0, len(messages))
	reachedSince := false
	for _, message := range messages {
		if message.Timestamp.Before(since) {
			reachedSince = true
			break
		}
		out = append(out, message)
	}
	return out, reachedSince
}

type messageSyncProgress struct {
	syncer        *Syncer
	guildID       string
	totalChannels int
	startedAt     time.Time
	lastLogAt     time.Time
	processed     int
	messages      int
}

func newMessageSyncProgress(s *Syncer, guildID string, totalChannels int, opts SyncOptions) *messageSyncProgress {
	if s == nil || s.logger == nil || totalChannels == 0 {
		return nil
	}
	progress := &messageSyncProgress{
		syncer:        s,
		guildID:       guildID,
		totalChannels: totalChannels,
		startedAt:     time.Now(),
		lastLogAt:     time.Now(),
	}
	s.logger.Info(
		"message sync started",
		"guild_id", guildID,
		"channels", totalChannels,
		"full", opts.Full,
		"concurrency", max(1, opts.Concurrency),
	)
	return progress
}

func (p *messageSyncProgress) record(channel *discordgo.Channel, count int) {
	if p == nil || p.syncer == nil || p.syncer.logger == nil {
		return
	}
	p.processed++
	p.messages += count
	now := time.Now()
	shouldLog := p.processed == p.totalChannels ||
		p.processed == 1 ||
		p.processed%100 == 0 ||
		now.Sub(p.lastLogAt) >= 15*time.Second
	if !shouldLog {
		return
	}
	p.lastLogAt = now
	channelID := ""
	channelName := ""
	if channel != nil {
		channelID = channel.ID
		channelName = channel.Name
	}
	p.syncer.logger.Info(
		"message sync progress",
		"guild_id", p.guildID,
		"processed_channels", p.processed,
		"total_channels", p.totalChannels,
		"remaining_channels", p.totalChannels-p.processed,
		"messages_written", p.messages,
		"last_channel_id", channelID,
		"last_channel_name", channelName,
		"elapsed", now.Sub(p.startedAt).Round(time.Second).String(),
	)
}
