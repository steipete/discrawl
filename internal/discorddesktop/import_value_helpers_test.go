package discorddesktop

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrimitiveValueHelpers(t *testing.T) {
	raw := map[string]any{
		"string":      "value",
		"blank":       "  ",
		"int":         3,
		"int64":       int64(4),
		"float":       float64(5),
		"json_number": json.Number("6"),
		"numeric":     "7",
		"bad_numeric": "nope",
		"truthy":      true,
		"array":       []any{"one", "two"},
	}

	require.Equal(t, "value", stringField(raw, "string"))
	require.Empty(t, stringField(raw, "blank"))
	require.Equal(t, "6", stringField(raw, "json_number"))
	require.Empty(t, stringField(raw, "int"))
	require.Empty(t, stringField(raw, "missing"))

	for key, want := range map[string]int{
		"int":         3,
		"float":       5,
		"json_number": 6,
	} {
		got, ok := intField(raw, key)
		require.True(t, ok, key)
		require.Equal(t, want, got, key)
	}
	_, ok := intField(raw, "bad_numeric")
	require.False(t, ok)
	_, ok = intField(raw, "int64")
	require.False(t, ok)
	_, ok = intField(raw, "numeric")
	require.False(t, ok)
	_, ok = intField(raw, "missing")
	require.False(t, ok)

	require.Equal(t, int64(3), int64Field(raw, "int"))
	require.Equal(t, int64(4), int64Field(raw, "int64"))
	require.Equal(t, int64(5), int64Field(raw, "float"))
	require.Equal(t, int64(6), int64Field(raw, "json_number"))
	require.Zero(t, int64Field(raw, "numeric"))
	require.Zero(t, int64Field(raw, "bad_numeric"))

	require.True(t, boolField(raw, "truthy"))
	require.False(t, boolField(raw, "missing"))
	require.Equal(t, 2, lenArray(raw["array"]))
	require.Zero(t, lenArray(raw["string"]))
	require.Equal(t, "fallback", firstNonEmpty("", "  ", "fallback", "later"))
	require.Empty(t, firstNonEmpty("", " "))
}

func TestDiscordValueFormatHelpers(t *testing.T) {
	require.Equal(t, "456789", shortID("123456789"))
	require.Equal(t, "short", shortID("short"))
	require.Equal(t, "Discord Direct Messages", guildName(DirectMessageGuildID))
	require.Equal(t, "Discord Desktop Guild 123456", guildName("123456"))

	require.Equal(t, "dm", kindForChannelType(1, true))
	require.Equal(t, "group_dm", kindForChannelType(3, true))
	require.Equal(t, "thread_public", kindForChannelType(11, false))
	require.Equal(t, "thread_private", kindForChannelType(12, false))
	require.Equal(t, "thread_announcement", kindForChannelType(10, false))
	require.Equal(t, "desktop", kindForChannelType(2, false))
	require.Equal(t, "desktop", kindForChannelType(4, false))
	require.Equal(t, "announcement", kindForChannelType(5, false))
	require.Equal(t, "forum", kindForChannelType(15, false))
	require.Equal(t, "desktop", kindForChannelType(16, false))
	require.Equal(t, "text", kindForChannelType(0, false))
}
