package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"

	"github.com/steipete/discrawl/internal/config"
	"github.com/steipete/discrawl/internal/store"
)

func (r *runtime) runSeed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	cfg, err := config.Load(r.configPath)
	if err != nil {
		return configErr(err)
	}
	if err := config.EnsureRuntimeDirs(cfg); err != nil {
		return configErr(err)
	}

	dataDir, err := config.ExpandPath(cfg.EffectiveDataDir())
	if err != nil {
		return configErr(fmt.Errorf("data dir: %w", err))
	}

	registry, err := store.NewRegistry(r.ctx, store.RegistryConfig{
		DataDir: dataDir,
	})
	if err != nil {
		return dbErr(fmt.Errorf("open registry: %w", err))
	}
	defer func() { _ = registry.Close() }()

	return r.seedData(r.ctx, registry)
}

func (r *runtime) seedData(ctx context.Context, registry *store.Registry) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	guilds := []guildDef{
		{name: "Gaming Community", channels: 12, members: 45, messages: 1500},
		{name: "Tech Hub", channels: 15, members: 50, messages: 2000},
		{name: "Art Studio", channels: 10, members: 35, messages: 1200},
		{name: "Music Lounge", channels: 8, members: 30, messages: 800},
		{name: "Study Group", channels: 10, members: 40, messages: 1000},
	}

	var totalGuilds, totalChannels, totalMembers, totalMessages int

	for _, guildDef := range guilds {
		guildID := genSnowflake(rng)
		r.logger.Info("seeding guild", "guild", guildDef.name, "id", guildID)

		gs, err := registry.Get(ctx, guildID)
		if err != nil {
			return fmt.Errorf("get guild store: %w", err)
		}

		// Insert guild
		guildRaw, _ := json.Marshal(map[string]any{
			"id":   guildID,
			"name": guildDef.name,
		})
		if err := gs.UpsertGuild(ctx, store.GuildRecord{
			ID:      guildID,
			Name:    guildDef.name,
			RawJSON: string(guildRaw),
		}); err != nil {
			registry.Release(guildID)
			return fmt.Errorf("upsert guild: %w", err)
		}

		// Generate channels
		channels := generateChannels(rng, guildID, guildDef.channels)
		for _, ch := range channels {
			if err := gs.UpsertChannel(ctx, ch); err != nil {
				registry.Release(guildID)
				return fmt.Errorf("upsert channel: %w", err)
			}
		}

		// Generate members
		members := generateMembers(rng, guildID, guildDef.members)
		if err := gs.ReplaceMembers(ctx, guildID, members); err != nil {
			registry.Release(guildID)
			return fmt.Errorf("replace members: %w", err)
		}

		// Generate messages over 30 days
		messages := generateMessages(rng, guildID, channels, members, guildDef.messages)
		mutations := make([]store.MessageMutation, len(messages))
		for i, msg := range messages {
			mutations[i] = store.MessageMutation{
				Record:  msg,
				Options: store.WriteOptions{},
			}
		}
		if err := gs.UpsertMessages(ctx, mutations); err != nil {
			registry.Release(guildID)
			return fmt.Errorf("upsert messages: %w", err)
		}

		registry.Release(guildID)

		totalGuilds++
		totalChannels += len(channels)
		totalMembers += len(members)
		totalMessages += len(messages)
	}

	_, _ = fmt.Fprintf(r.stdout, "✓ Seeded %d guilds, %d channels, %d members, %d messages\n",
		totalGuilds, totalChannels, totalMembers, totalMessages)
	return nil
}

type guildDef struct {
	name     string
	channels int
	members  int
	messages int
}

func generateChannels(rng *rand.Rand, guildID string, count int) []store.ChannelRecord {
	channelNames := []string{
		"general", "announcements", "off-topic", "dev-talk", "memes",
		"help", "showcase", "random", "introductions", "events",
		"feedback", "voice-chat", "gaming", "music", "art",
	}

	channels := make([]store.ChannelRecord, 0, count)
	for i := 0; i < count && i < len(channelNames); i++ {
		chID := genSnowflake(rng)
		name := channelNames[i]
		chRaw, _ := json.Marshal(map[string]any{
			"id":       chID,
			"guild_id": guildID,
			"name":     name,
			"type":     0,
		})
		channels = append(channels, store.ChannelRecord{
			ID:       chID,
			GuildID:  guildID,
			Kind:     "GUILD_TEXT",
			Name:     name,
			Position: i,
			RawJSON:  string(chRaw),
		})
	}
	return channels
}

func generateMembers(rng *rand.Rand, guildID string, count int) []store.MemberRecord {
	firstNames := []string{
		"Alex", "Blake", "Casey", "Drew", "Eli", "Finn", "Gray", "Harper",
		"Indigo", "Jordan", "Kelly", "Logan", "Morgan", "Noel", "Onyx", "Parker",
		"Quinn", "Reese", "Sage", "Taylor", "Uma", "Val", "Wren", "Xen",
		"Yuki", "Zane", "Aria", "Beau", "Cleo", "Devon", "Echo", "Frost",
	}
	suffixes := []string{
		"Dev", "Pro", "Gamer", "Artist", "Coder", "Ninja", "Master", "Wizard",
		"Guru", "Monk", "Sage", "Rebel", "Ghost", "Phoenix", "Dragon", "Wolf",
	}

	members := make([]store.MemberRecord, 0, count)
	for i := 0; i < count; i++ {
		userID := genSnowflake(rng)
		username := fmt.Sprintf("%s%s%d", firstNames[rng.Intn(len(firstNames))],
			suffixes[rng.Intn(len(suffixes))], rng.Intn(1000))
		displayName := firstNames[rng.Intn(len(firstNames))]

		memberRaw, _ := json.Marshal(map[string]any{
			"user": map[string]any{
				"id":       userID,
				"username": username,
			},
			"nick": displayName,
		})
		members = append(members, store.MemberRecord{
			GuildID:     guildID,
			UserID:      userID,
			Username:    username,
			DisplayName: displayName,
			JoinedAt:    time.Now().Add(-time.Duration(rng.Intn(365*24)) * time.Hour).UTC().Format(time.RFC3339Nano),
			RawJSON:     string(memberRaw),
		})
	}
	return members
}

func generateMessages(rng *rand.Rand, guildID string, channels []store.ChannelRecord, members []store.MemberRecord, count int) []store.MessageRecord {
	templates := []string{
		"Hey everyone! 👋",
		"Just finished working on %s, thoughts?",
		"Anyone here familiar with %s?",
		"Check out this cool project: %s",
		"Quick question about %s",
		"This is amazing: %s",
		"Has anyone tried %s yet?",
		"I'm stuck on %s, any ideas?",
		"```go\nfunc main() {\n\tfmt.Println(\"Hello, World!\")\n}\n```",
		"```python\ndef greet():\n    print('Hello!')\n```",
		"Great discussion! Thanks for the help",
		"I agree with that approach",
		"Let me look into that and get back to you",
		"That makes sense, I'll give it a try",
		"Interesting idea! 🤔",
		"lol that's hilarious 😂",
		"Thanks for sharing!",
		"Welcome to the server!",
		"Good morning everyone ☀️",
		"Have a great weekend! 🎉",
	}

	topics := []string{"React", "Go", "Rust", "Python", "Docker", "Kubernetes", "TypeScript", "WebGL", "AI", "game dev"}

	now := time.Now().UTC()
	thirtyDaysAgo := now.Add(-30 * 24 * time.Hour)

	messages := make([]store.MessageRecord, 0, count)
	for i := 0; i < count; i++ {
		channel := channels[rng.Intn(len(channels))]
		member := members[rng.Intn(len(members))]
		msgID := genSnowflake(rng)

		template := templates[rng.Intn(len(templates))]
		content := template
		if fmt.Sprintf(template, "X") != template {
			content = fmt.Sprintf(template, topics[rng.Intn(len(topics))])
		}

		createdAt := thirtyDaysAgo.Add(time.Duration(rng.Int63n(int64(30*24*time.Hour))))

		msgRaw, _ := json.Marshal(map[string]any{
			"id":         msgID,
			"channel_id": channel.ID,
			"author": map[string]any{
				"id":       member.UserID,
				"username": member.Username,
			},
			"content":   content,
			"timestamp": createdAt.Format(time.RFC3339),
		})

		messages = append(messages, store.MessageRecord{
			ID:                msgID,
			GuildID:           guildID,
			ChannelID:         channel.ID,
			ChannelName:       channel.Name,
			AuthorID:          member.UserID,
			AuthorName:        member.Username,
			MessageType:       0,
			CreatedAt:         createdAt.Format(time.RFC3339Nano),
			Content:           content,
			NormalizedContent: content,
			RawJSON:           string(msgRaw),
		})
	}
	return messages
}

func genSnowflake(rng *rand.Rand) string {
	// Discord snowflake: 64-bit, typically 17-19 digits
	// Simple approximation: timestamp + random bits
	ts := time.Now().UnixMilli() << 22
	random := int64(rng.Intn(1 << 22))
	return strconv.FormatInt(ts|random, 10)
}
