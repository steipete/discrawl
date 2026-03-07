package discord

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bwmarrin/discordgo"
)

type EventHandler interface {
	OnMessageCreate(context.Context, *discordgo.Message) error
	OnMessageUpdate(context.Context, *discordgo.Message) error
	OnMessageDelete(context.Context, *discordgo.MessageDelete) error
	OnChannelUpsert(context.Context, *discordgo.Channel) error
	OnMemberUpsert(context.Context, string, *discordgo.Member) error
	OnMemberDelete(context.Context, string, string) error
}

type Client struct {
	session        *discordgo.Session
	requestTimeout time.Duration
}

func New(token string) (*Client, error) {
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, fmt.Errorf("create discord session: %w", err)
	}
	session.Identify.Intents = discordgo.IntentsGuilds |
		discordgo.IntentsGuildMessages |
		discordgo.IntentsMessageContent |
		discordgo.IntentsGuildMembers
	return &Client{
		session:        session,
		requestTimeout: 45 * time.Second,
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.session == nil {
		return nil
	}
	return c.session.Close()
}

func (c *Client) Self(ctx context.Context) (*discordgo.User, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.User("@me", discordgo.WithContext(reqCtx))
}

func (c *Client) Guilds(ctx context.Context) ([]*discordgo.UserGuild, error) {
	var out []*discordgo.UserGuild
	before := ""
	for {
		reqCtx, cancel := c.requestContext(ctx)
		page, err := c.session.UserGuilds(200, before, "", false, discordgo.WithContext(reqCtx))
		cancel()
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		before = page[len(page)-1].ID
		if len(page) < 200 {
			return out, nil
		}
	}
}

func (c *Client) Guild(ctx context.Context, guildID string) (*discordgo.Guild, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.Guild(guildID, discordgo.WithContext(reqCtx))
}

func (c *Client) GuildChannels(ctx context.Context, guildID string) ([]*discordgo.Channel, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.GuildChannels(guildID, discordgo.WithContext(reqCtx))
}

func (c *Client) ThreadsActive(ctx context.Context, channelID string) ([]*discordgo.Channel, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	list, err := c.session.ThreadsActive(channelID, discordgo.WithContext(reqCtx))
	if err != nil {
		return nil, err
	}
	return list.Threads, nil
}

func (c *Client) ThreadsArchived(ctx context.Context, channelID string, private bool) ([]*discordgo.Channel, error) {
	var out []*discordgo.Channel
	var before *time.Time
	for {
		reqCtx, cancel := c.requestContext(ctx)
		var list *discordgo.ThreadsList
		var err error
		if private {
			list, err = c.session.ThreadsPrivateArchived(channelID, before, 100, discordgo.WithContext(reqCtx))
		} else {
			list, err = c.session.ThreadsArchived(channelID, before, 100, discordgo.WithContext(reqCtx))
		}
		cancel()
		if err != nil {
			return nil, err
		}
		if len(list.Threads) == 0 {
			return out, nil
		}
		out = append(out, list.Threads...)
		if !list.HasMore {
			return uniqueChannels(out), nil
		}
		oldest := list.Threads[len(list.Threads)-1]
		if oldest.ThreadMetadata == nil {
			return uniqueChannels(out), nil
		}
		archiveAt := oldest.ThreadMetadata.ArchiveTimestamp
		before = &archiveAt
	}
}

func (c *Client) GuildMembers(ctx context.Context, guildID string) ([]*discordgo.Member, error) {
	var out []*discordgo.Member
	after := ""
	for {
		reqCtx, cancel := c.requestContext(ctx)
		page, err := c.session.GuildMembers(guildID, after, 1000, discordgo.WithContext(reqCtx))
		cancel()
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		after = page[len(page)-1].User.ID
		if len(page) < 1000 {
			return out, nil
		}
	}
}

func (c *Client) ChannelMessages(ctx context.Context, channelID string, limit int, beforeID, afterID string) ([]*discordgo.Message, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.ChannelMessages(channelID, limit, beforeID, afterID, "", discordgo.WithContext(reqCtx))
}

func (c *Client) ChannelMessage(ctx context.Context, channelID, messageID string) (*discordgo.Message, error) {
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	return c.session.ChannelMessage(channelID, messageID, discordgo.WithContext(reqCtx))
}

func (c *Client) Tail(ctx context.Context, handler EventHandler) error {
	if handler == nil {
		return fmt.Errorf("missing event handler")
	}
	errCh := make(chan error, 1)
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.MessageCreate) {
		if err := handler.OnMessageCreate(ctx, evt.Message); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	c.session.AddHandler(func(session *discordgo.Session, evt *discordgo.MessageUpdate) {
		msg := evt.Message
		if msg != nil && msg.Content == "" {
			full, err := session.ChannelMessage(evt.ChannelID, evt.ID)
			if err == nil {
				msg = full
			}
		}
		if msg == nil {
			return
		}
		if err := handler.OnMessageUpdate(ctx, msg); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.MessageDelete) {
		if err := handler.OnMessageDelete(ctx, evt); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.ChannelCreate) {
		if err := handler.OnChannelUpsert(ctx, evt.Channel); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.ChannelUpdate) {
		if err := handler.OnChannelUpsert(ctx, evt.Channel); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberAdd) {
		if err := handler.OnMemberUpsert(ctx, evt.GuildID, evt.Member); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberUpdate) {
		member := &discordgo.Member{
			GuildID:  evt.GuildID,
			Nick:     evt.Nick,
			Avatar:   evt.Avatar,
			Roles:    evt.Roles,
			JoinedAt: evt.JoinedAt,
			User:     evt.User,
		}
		if err := handler.OnMemberUpsert(ctx, evt.GuildID, member); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberRemove) {
		if evt.User == nil {
			return
		}
		if err := handler.OnMemberDelete(ctx, evt.GuildID, evt.User.ID); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	if err := c.session.Open(); err != nil {
		return err
	}
	defer func() { _ = c.session.Close() }()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func uniqueChannels(in []*discordgo.Channel) []*discordgo.Channel {
	if len(in) == 0 {
		return nil
	}
	out := make([]*discordgo.Channel, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, ch := range in {
		if ch == nil {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		out = append(out, ch)
	}
	slices.SortFunc(out, func(a, b *discordgo.Channel) int {
		switch {
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

func (c *Client) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c == nil || c.requestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.requestTimeout)
}
