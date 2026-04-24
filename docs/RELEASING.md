---
summary: "Release checklist for discrawl (GitHub release binaries via GoReleaser + Homebrew tap update)"
---

# Releasing `discrawl`

Always do all steps below. No partial releases.

Assumptions:
- Repo: `steipete/discrawl`
- Binary: `discrawl`
- GoReleaser config: `.goreleaser.yaml`
- Homebrew tap repo: `~/Projects/homebrew-tap`

## 0) Prereqs

- Clean working tree on `main`
- Go toolchain from `go.mod`
- GitHub CLI authenticated
- CI green on `main`

## 1) Verify build + tests

```sh
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.1 run
go test -count=1 ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | tail -n 1
go test -count=1 -race ./...
go build -o /tmp/discrawl ./cmd/discrawl
gh run list -L 5 --branch main
```

Coverage floor: `85%+`

## 2) Update changelog

Add a new section in `CHANGELOG.md`.

Example:

- `## 0.2.0 - 2026-03-08`

## 3) Commit, tag, push

```sh
git checkout main
git pull --ff-only origin main
git commit -am "release: vX.Y.Z"
git tag -a vX.Y.Z -m "Release X.Y.Z"
git push origin main --tags
```

## 4) Verify GitHub release assets

The tag push triggers `.github/workflows/release.yml`.

```sh
gh run list -L 5 --workflow release.yml
gh release view vX.Y.Z
```

Confirm assets exist for:

- `darwin_amd64`
- `darwin_arm64`
- `linux_amd64`
- `linux_arm64`
- `windows_amd64`
- `windows_arm64`

## 5) Update Homebrew tap

`discrawl` ships a binary formula in `~/Projects/homebrew-tap/Formula/discrawl.rb` that points at the GitHub release archives.

After tagging a real release:

1. update the formula `version`
2. update the per-platform release archive `sha256` values
3. test local install + version output
4. commit + push `homebrew-tap`

Useful commands:

```sh
curl -L -o /tmp/discrawl-darwin-arm64.tgz https://github.com/steipete/discrawl/releases/download/vX.Y.Z/discrawl_X.Y.Z_darwin_arm64.tar.gz
shasum -a 256 /tmp/discrawl-darwin-arm64.tgz
brew uninstall discrawl || true
brew install steipete/tap/discrawl
discrawl --version
brew info steipete/tap/discrawl
```

## Notes

- Build-time version stamping comes from `-X github.com/steipete/discrawl/internal/cli.version={{ .Version }}`
- If release workflow needs a rerun:

```sh
gh workflow run release.yml -f tag=vX.Y.Z
```
