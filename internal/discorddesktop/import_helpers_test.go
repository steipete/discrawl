package discorddesktop

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
)

func TestFileFingerprintStatusHelpers(t *testing.T) {
	base := fileFingerprint{Size: 123, ModUnixNS: 456}
	require.True(t, sameFileFingerprint(base, fileFingerprint{Size: 123, ModUnixNS: 456, Status: fileStatusSkipped}))
	require.False(t, sameFileFingerprint(base, fileFingerprint{Size: 124, ModUnixNS: 456}))
	require.False(t, sameFileFingerprint(base, fileFingerprint{Size: 123, ModUnixNS: 457}))

	require.True(t, isImportedFingerprint(base))
	require.True(t, isImportedFingerprint(importedFingerprint(base)))
	require.False(t, isImportedFingerprint(skippedFingerprint(base)))
	require.Equal(t, fileStatusImported, importedFingerprint(base).Status)
	require.Equal(t, fileStatusSkipped, skippedFingerprint(base).Status)
	require.Equal(t, wiretapFileIndexScope, fileIndexScope(Options{}))
	require.Equal(t, wiretapFileIndexScope, fileIndexScope(Options{FullCache: true}))
}

func TestSnapshotCopyHelpers(t *testing.T) {
	base := newSnapshot()
	base.routes["111111111111111121"] = "999999999999999996"
	base.userLabels["222222222222222232"] = userLabel{Name: "Alice"}
	base.channels["111111111111111121"] = store.ChannelRecord{ID: "111111111111111121", GuildID: "999999999999999996", Name: "general"}

	snap := newSnapshotWithContext(base)
	require.Equal(t, base.routes, snap.routes)
	require.Equal(t, base.userLabels, snap.userLabels)
	require.Empty(t, snap.channels)

	next := newSnapshot()
	next.routes["111111111111111122"] = "999999999999999996"
	next.userLabels["222222222222222233"] = userLabel{Name: "Bob"}
	next.channels["111111111111111122"] = store.ChannelRecord{ID: "111111111111111122", GuildID: "999999999999999996", Name: "random"}
	mergeSnapshotContext(base, next)

	require.Equal(t, "999999999999999996", base.routes["111111111111111122"])
	require.Equal(t, "Bob", base.userLabels["222222222222222233"].Name)
	require.Equal(t, "random", base.channels["111111111111111122"].Name)

	lookup := copyChannelLookup(base.channels)
	lookup["111111111111111122"] = store.ChannelRecord{ID: "changed"}
	require.Equal(t, "random", base.channels["111111111111111122"].Name)
}

func TestSnapshotWithoutMessageEvents(t *testing.T) {
	snap := newSnapshot()
	snap.messages["333333333333333346"] = store.MessageMutation{
		Record: store.MessageRecord{ID: "333333333333333346"},
		Options: store.WriteOptions{
			AppendEvent:      true,
			EnqueueEmbedding: true,
		},
	}
	stripped := snapshotWithoutMessageEvents(snap)
	require.False(t, stripped.messages["333333333333333346"].Options.AppendEvent)
	require.True(t, stripped.messages["333333333333333346"].Options.EnqueueEmbedding)
	require.True(t, snap.messages["333333333333333346"].Options.AppendEvent)
}

func TestRouteFilteredCacheHelpers(t *testing.T) {
	require.Equal(t, fileSourceCacheData, sourceForPath("/tmp/discord", "/tmp/discord/Cache/Cache_Data/entry", "Cache/Cache_Data/entry"))
	require.Equal(t, fileSourceCacheData, sourceForPath("/tmp/discord", "/tmp/discord/Service Worker/CacheStorage/cache/entry", "Service Worker/CacheStorage/cache/entry"))
	require.Equal(t, fileSourceContext, sourceForPath("/tmp/discord", "/tmp/discord/Local Storage/leveldb/000001.log", "Local Storage/leveldb/000001.log"))
}

func TestCacheFileHasRouteHint(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "route"), []byte("https://discord.com/api/v9/channels/111111111111111121/messages?limit=50"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plain"), []byte("no discord route here"), 0o600))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()

	ok, err := cacheFileHasRouteHint(root, "route")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = cacheFileHasRouteHint(root, "plain")
	require.NoError(t, err)
	require.False(t, ok)
	_, err = cacheFileHasRouteHint(root, "missing")
	require.Error(t, err)
}
