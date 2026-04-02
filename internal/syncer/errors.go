package syncer

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/bwmarrin/discordgo"
)

func (s *Syncer) skipSyncError(ctx context.Context, channel *discordgo.Channel, err error) bool {
	if s.skipUnavailableChannel(ctx, channel, err) {
		return true
	}
	if !isRetryableSyncError(ctx, err) {
		return false
	}
	s.logger.Warn("channel message crawl deferred", "channel_id", channel.ID, "err", err)
	return true
}

func (s *Syncer) skipUnavailableChannel(ctx context.Context, channel *discordgo.Channel, err error) bool {
	if channel == nil {
		return false
	}
	return s.skipUnavailableChannelByID(ctx, channel.ID, err, "channel message crawl skipped")
}

func (s *Syncer) skipUnavailableChannelByID(ctx context.Context, channelID string, err error, logMsg string) bool {
	reason := unavailableReason(err)
	if reason == "" {
		return false
	}
	s.logger.Warn(logMsg, "channel_id", channelID, "err", err)
	if s.store != nil && channelID != "" {
		_ = s.store.SetSyncState(ctx, "channel:"+channelID+":unavailable", reason)
	}
	return true
}

func isRetryableSyncError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "deadline exceeded"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "eof"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "stream error"),
		strings.Contains(msg, "goaway"),
		strings.Contains(msg, "http 429"),
		strings.Contains(msg, "http 500"),
		strings.Contains(msg, "http 502"),
		strings.Contains(msg, "http 503"),
		strings.Contains(msg, "http 504"):
		return true
	default:
		return false
	}
}

func unavailableReason(err error) string {
	switch {
	case isMissingAccess(err):
		return "missing_access"
	case isUnknownChannel(err):
		return "unknown_channel"
	default:
		return ""
	}
}

func isUnknownChannel(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown channel") ||
		(strings.Contains(msg, "http 404") && strings.Contains(msg, `"code": 10003`))
}

func isMissingAccess(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "403 Forbidden") || strings.Contains(msg, "Missing Access")
}
