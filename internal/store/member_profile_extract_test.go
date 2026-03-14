package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractMemberProfileExplicitFields(t *testing.T) {
	t.Parallel()

	profile := extractMemberProfile(`{
		"bio":"Builds native tools",
		"pronouns":"he/him",
		"location":"Vienna",
		"website":"https://steipete.me",
		"github":"steipete",
		"twitter":"steipete",
		"email":"secret@example.com",
		"token":"super-secret"
	}`)

	require.Equal(t, "Builds native tools", profile.Bio)
	require.Equal(t, "he/him", profile.Pronouns)
	require.Equal(t, "Vienna", profile.Location)
	require.Equal(t, "https://steipete.me", profile.Website)
	require.Equal(t, "steipete", profile.GitHubLogin)
	require.Equal(t, "steipete", profile.XHandle)
	require.NotContains(t, profile.SearchText, "secret@example.com")
	require.NotContains(t, profile.SearchText, "super-secret")
}

func TestExtractMemberProfileInfersHandlesFromURLs(t *testing.T) {
	t.Parallel()

	profile := extractMemberProfile(`{
		"description":"Maintainer",
		"links":[
			"https://github.com/example-user",
			"https://x.com/example_handle",
			"https://example.com/about"
		]
	}`)

	require.Equal(t, "Maintainer", profile.Bio)
	require.Equal(t, "example-user", profile.GitHubLogin)
	require.Equal(t, "example_handle", profile.XHandle)
	require.Equal(t, "https://example.com/about", profile.Website)
	require.Equal(t, []string{
		"https://example.com/about",
		"https://github.com/example-user",
		"https://x.com/example_handle",
	}, profile.URLs)
}

func TestExtractMemberProfileInvalidJSONFallsBackToRawSearchText(t *testing.T) {
	t.Parallel()

	raw := `github steipete twitter steipete`
	profile := extractMemberProfile(raw)

	require.Equal(t, raw, profile.SearchText)
	require.Empty(t, profile.GitHubLogin)
	require.Empty(t, profile.XHandle)
}

func TestNormalizeSocialHelpers(t *testing.T) {
	t.Parallel()

	require.Equal(t, "steipete", normalizeGitHubLogin("https://github.com/steipete"))
	require.Equal(t, "steipete", normalizeGitHubLogin("@steipete"))
	require.Equal(t, "steipete", normalizeXHandle("https://x.com/steipete"))
	require.Equal(t, "steipete", normalizeXHandle("@steipete"))
	require.Equal(t, "steipete", socialLoginFromURLs([]string{"https://github.com/steipete"}, "github.com"))
	require.Equal(t, "steipete", socialLoginFromURLs([]string{"https://twitter.com/steipete"}, "twitter.com"))
	require.Equal(t, "https://example.com", firstNonSocialURL([]string{
		"https://github.com/steipete",
		"https://example.com",
	}))
}
