package discord

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
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
	session            *discordgo.Session
	fallbackToken      string
	authMu             sync.Mutex
	requestTimeout     time.Duration
	tailWorkerCount    int
	tailQueueSize      int
	tailHandlerTimeout time.Duration
}

func New(token string) (*Client, error) {
	authToken, fallbackToken := authTokens(token)
	session, err := discordgo.New(authToken)
	if err != nil {
		return nil, fmt.Errorf("create discord session: %w", err)
	}
	session.Identify.Intents = discordgo.IntentsGuilds |
		discordgo.IntentsGuildMessages |
		discordgo.IntentsMessageContent |
		discordgo.IntentsGuildMembers
	return &Client{
		session:            session,
		fallbackToken:      fallbackToken,
		requestTimeout:     45 * time.Second,
		tailWorkerCount:    defaultTailWorkerCount(),
		tailQueueSize:      defaultTailQueueSize(),
		tailHandlerTimeout: 30 * time.Second,
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.session == nil {
		return nil
	}
	return c.session.Close()
}

func (c *Client) Self(ctx context.Context) (*discordgo.User, error) {
	var out *discordgo.User
	err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
		reqCtx, cancel := c.requestContext(callCtx)
		defer cancel()
		user, err := c.session.User("@me", discordgo.WithContext(reqCtx))
		if err != nil {
			return err
		}
		out = user
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) Guilds(ctx context.Context) ([]*discordgo.UserGuild, error) {
	var out []*discordgo.UserGuild
	before := ""
	for {
		var page []*discordgo.UserGuild
		err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
			reqCtx, cancel := c.requestContext(callCtx)
			defer cancel()
			result, err := c.session.UserGuilds(200, before, "", false, discordgo.WithContext(reqCtx))
			if err != nil {
				return err
			}
			page = result
			return nil
		})
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
	var out *discordgo.Guild
	err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
		reqCtx, cancel := c.requestContext(callCtx)
		defer cancel()
		guild, err := c.session.Guild(guildID, discordgo.WithContext(reqCtx))
		if err != nil {
			return err
		}
		out = guild
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) GuildChannels(ctx context.Context, guildID string) ([]*discordgo.Channel, error) {
	var out []*discordgo.Channel
	err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
		reqCtx, cancel := c.requestContext(callCtx)
		defer cancel()
		channels, err := c.session.GuildChannels(guildID, discordgo.WithContext(reqCtx))
		if err != nil {
			return err
		}
		out = channels
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) ThreadsActive(ctx context.Context, channelID string) ([]*discordgo.Channel, error) {
	var list *discordgo.ThreadsList
	err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
		reqCtx, cancel := c.requestContext(callCtx)
		defer cancel()
		result, err := c.session.ThreadsActive(channelID, discordgo.WithContext(reqCtx))
		if err != nil {
			return err
		}
		list = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list.Threads, nil
}

func (c *Client) ThreadsArchived(ctx context.Context, channelID string, private bool) ([]*discordgo.Channel, error) {
	var out []*discordgo.Channel
	var before *time.Time
	for {
		var list *discordgo.ThreadsList
		err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
			reqCtx, cancel := c.requestContext(callCtx)
			defer cancel()
			if private {
				result, err := c.session.ThreadsPrivateArchived(channelID, before, 100, discordgo.WithContext(reqCtx))
				if err != nil {
					return err
				}
				list = result
				return nil
			}
			result, err := c.session.ThreadsArchived(channelID, before, 100, discordgo.WithContext(reqCtx))
			if err != nil {
				return err
			}
			list = result
			return nil
		})
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
		var page []*discordgo.Member
		err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
			reqCtx, cancel := c.requestContext(callCtx)
			defer cancel()
			result, err := c.session.GuildMembers(guildID, after, 1000, discordgo.WithContext(reqCtx))
			if err != nil {
				return err
			}
			page = result
			return nil
		})
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
	var out []*discordgo.Message
	err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
		reqCtx, cancel := c.requestContext(callCtx)
		defer cancel()
		messages, err := c.session.ChannelMessages(channelID, limit, beforeID, afterID, "", discordgo.WithContext(reqCtx))
		if err != nil {
			return err
		}
		out = messages
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) ChannelMessage(ctx context.Context, channelID, messageID string) (*discordgo.Message, error) {
	var out *discordgo.Message
	err := c.withAuthFallback(ctx, func(callCtx context.Context) error {
		reqCtx, cancel := c.requestContext(callCtx)
		defer cancel()
		message, err := c.session.ChannelMessage(channelID, messageID, discordgo.WithContext(reqCtx))
		if err != nil {
			return err
		}
		out = message
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) Tail(ctx context.Context, handler EventHandler) error {
	if handler == nil {
		return fmt.Errorf("missing event handler")
	}
	tailCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	workCh := make(chan func(context.Context) error, c.tailQueueSize)
	var wg sync.WaitGroup
	for i := 0; i < c.tailWorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-tailCtx.Done():
					return
				case task := <-workCh:
					if task == nil {
						continue
					}
					if err := c.runTailTask(tailCtx, task); err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}
			}
		}()
	}

	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.MessageCreate) {
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			return handler.OnMessageCreate(taskCtx, evt.Message)
		})
	})
	c.session.AddHandler(func(session *discordgo.Session, evt *discordgo.MessageUpdate) {
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			msg := evt.Message
			if msg != nil && msg.Content == "" {
				full, err := session.ChannelMessage(evt.ChannelID, evt.ID, discordgo.WithContext(taskCtx))
				if err == nil {
					msg = full
				}
			}
			if msg == nil {
				return nil
			}
			return handler.OnMessageUpdate(taskCtx, msg)
		})
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.MessageDelete) {
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			return handler.OnMessageDelete(taskCtx, evt)
		})
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.ChannelCreate) {
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			return handler.OnChannelUpsert(taskCtx, evt.Channel)
		})
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.ChannelUpdate) {
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			return handler.OnChannelUpsert(taskCtx, evt.Channel)
		})
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberAdd) {
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			return handler.OnMemberUpsert(taskCtx, evt.GuildID, evt.Member)
		})
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
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			return handler.OnMemberUpsert(taskCtx, evt.GuildID, member)
		})
	})
	c.session.AddHandler(func(_ *discordgo.Session, evt *discordgo.GuildMemberRemove) {
		if evt.User == nil {
			return
		}
		c.enqueueTailTask(tailCtx, workCh, errCh, func(taskCtx context.Context) error {
			return handler.OnMemberDelete(taskCtx, evt.GuildID, evt.User.ID)
		})
	})
	if err := c.withAuthFallback(tailCtx, func(context.Context) error {
		return c.session.Open()
	}); err != nil {
		return err
	}
	defer func() {
		cancel()
		_ = c.session.Close()
		wg.Wait()
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (c *Client) enqueueTailTask(
	ctx context.Context,
	workCh chan<- func(context.Context) error,
	errCh chan<- error,
	task func(context.Context) error,
) {
	select {
	case <-ctx.Done():
		return
	case workCh <- task:
	default:
		select {
		case errCh <- fmt.Errorf("tail worker queue full"):
		default:
		}
	}
}

func (c *Client) runTailTask(ctx context.Context, task func(context.Context) error) (err error) {
	if c.tailHandlerTimeout > 0 {
		taskCtx, cancel := context.WithTimeout(ctx, c.tailHandlerTimeout)
		defer cancel()
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("tail handler panic: %v", recovered)
			}
		}()
		return task(taskCtx)
	}
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("tail handler panic: %v", recovered)
		}
	}()
	return task(taskCtx)
}

func defaultTailWorkerCount() int {
	workers := runtime.GOMAXPROCS(0)
	switch {
	case workers < 4:
		return 4
	case workers > 16:
		return 16
	default:
		return workers
	}
}

func defaultTailQueueSize() int {
	return defaultTailWorkerCount() * 32
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

func authTokens(raw string) (token string, fallback string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", ""
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "bot ") || strings.HasPrefix(lower, "bearer ") {
		return trimmed, ""
	}
	return "Bot " + trimmed, trimmed
}

func (c *Client) withAuthFallback(ctx context.Context, call func(context.Context) error) error {
	err := call(ctx)
	if err == nil || !isUnauthorized(err) {
		return err
	}
	if !c.swapToFallbackToken() {
		return err
	}
	return call(ctx)
}

func (c *Client) swapToFallbackToken() bool {
	if c == nil {
		return false
	}
	c.authMu.Lock()
	defer c.authMu.Unlock()
	if c.fallbackToken == "" || c.session == nil || c.session.Token == c.fallbackToken {
		return false
	}
	c.session.Token = c.fallbackToken
	c.fallbackToken = ""
	return true
}

func isUnauthorized(err error) bool {
	var restErr *discordgo.RESTError
	if errors.As(err, &restErr) && restErr.Response != nil {
		if restErr.Response.StatusCode == http.StatusUnauthorized {
			return true
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "401") ||
		strings.Contains(message, "4004") ||
		strings.Contains(message, "unauthorized") ||
		strings.Contains(message, "authentication failed")
}
