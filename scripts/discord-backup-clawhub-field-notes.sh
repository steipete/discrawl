#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <discrawl-config> <backup-repo>" >&2
  exit 2
fi

CONFIG=$1
BACKUP_REPO=$2
OUTPUT_FILE=${DISCORD_CLAWHUB_FIELD_NOTES_OUTPUT:-clawhub-field-notes.md}
OUTPUT_PATH="$BACKUP_REPO/$OUTPUT_FILE"
OPENCLAW_BIN=${OPENCLAW_BIN:-openclaw}
OPENCLAW_TIMEOUT=${OPENCLAW_TIMEOUT:-300}
OPENCLAW_THINKING=${OPENCLAW_THINKING:-low}
OPENCLAW_ATTEMPTS=${OPENCLAW_ATTEMPTS:-2}
GH_REPO=${DISCORD_CLAWHUB_FIELD_NOTES_GITHUB_REPO:-openclaw/clawhub}
FOCUS_TERM=${DISCORD_CLAWHUB_FIELD_NOTES_FOCUS_TERM:-clawhub}
FOCUS_LABEL=${DISCORD_CLAWHUB_FIELD_NOTES_FOCUS_LABEL:-Clawhub}
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

date_utc_days_ago() {
  local days=$1
  if date -u -d "$days days ago" '+%Y-%m-%d' >/dev/null 2>&1; then
    date -u -d "$days days ago" '+%Y-%m-%d'
  else
    date -u -v-"$days"d '+%Y-%m-%d'
  fi
}

sql_like_literal() {
  if [ "$#" -gt 0 ]; then
    printf '%s' "$1"
  else
    cat
  fi | sed "s/'/''/g"
}

run_sql() {
  local title=$1
  local query=$2
  local output
  {
    printf "\n## %s\n\n" "$title"
    if output=$(DISCRAWL_NO_AUTO_UPDATE=1 go run ./cmd/discrawl --config "$CONFIG" --json sql "$query" 2>&1); then
      printf '%s\n' "$output" | jq -c .
    else
      printf '[]\n'
      printf '\n_query skipped: %s_\n' "$(printf '%s' "$output" | tail -n 1)"
    fi
  } >>"$TMP_DIR/context.md"
}

run_openclaw_agent() {
  local -a cmd=(
    "$OPENCLAW_BIN" agent
    --local
    --agent main
    --thinking "$OPENCLAW_THINKING"
    --timeout "$OPENCLAW_TIMEOUT"
    --json
    --message "$(cat "$TMP_DIR/prompt.md")"
  )

  if command -v timeout >/dev/null 2>&1; then
    timeout "${OPENCLAW_TIMEOUT}s" "${cmd[@]}"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "${OPENCLAW_TIMEOUT}s" "${cmd[@]}"
  else
    "${cmd[@]}"
  fi
}

extract_openclaw_text() {
  if jq -r '
    [
      .payloads[]?.text?,
      .finalAssistantVisibleText?,
      .finalAssistantRawText?,
      .result.finalAssistantVisibleText?,
      .result.finalAssistantRawText?,
      (.. | objects | .finalAssistantVisibleText?),
      (.. | objects | .finalAssistantRawText?)
    ]
    | map(select(type == "string" and length > 0))[0] // empty
  ' "$TMP_DIR/openclaw-result.json" 2>/dev/null; then
    return
  fi

  awk 'found || $0 == "{" { found = 1; print }' "$TMP_DIR/openclaw-result.json" >"$TMP_DIR/openclaw-result-tail.json"
  if [ -s "$TMP_DIR/openclaw-result-tail.json" ] && jq -r '
    [
      .payloads[]?.text?,
      .finalAssistantVisibleText?,
      .finalAssistantRawText?,
      .result.finalAssistantVisibleText?,
      .result.finalAssistantRawText?,
      (.. | objects | .finalAssistantVisibleText?),
      (.. | objects | .finalAssistantRawText?)
    ]
    | map(select(type == "string" and length > 0))[0] // empty
  ' "$TMP_DIR/openclaw-result-tail.json" 2>/dev/null; then
    return
  fi

  jq -Rrs '
    def field_notes:
      [
        .payloads[]?.text?,
        .finalAssistantVisibleText?,
        .finalAssistantRawText?,
        .result.finalAssistantVisibleText?,
        .result.finalAssistantRawText?,
        (.. | objects | .finalAssistantVisibleText?),
        (.. | objects | .finalAssistantRawText?)
      ]
      | map(select(type == "string" and length > 0))[0] // empty;

    capture("(?s)(?<json>\\{.*\\})").json
    | fromjson
    | field_notes
  ' "$TMP_DIR/openclaw-result.json" 2>/dev/null || true
}

anchor_expr="(select max(created_at) from messages)"
since_7="strftime('%Y-%m-%dT%H:%M:%fZ', datetime($anchor_expr, '-7 days'))"
since_30="strftime('%Y-%m-%dT%H:%M:%fZ', datetime($anchor_expr, '-30 days'))"
human_filter="lower(coalesce(mem.username, mem.display_name, m.author_id, '')) not in ('github', 'dependabot')"
focus_sql=$(printf '%s' "$FOCUS_TERM" | tr '[:upper:]' '[:lower:]' | sql_like_literal)
focus_filter="lower(coalesce(nullif(m.content, ''), m.normalized_content, '')) like '%$focus_sql%'"
github_since=$(date_utc_days_ago 30)

recent_human_cte="
with recent as (
  select
    m.created_at,
    coalesce(nullif(c.name, ''), m.channel_id) as channel,
    coalesce(nullif(mem.display_name, ''), nullif(mem.username, ''), m.author_id, '') as author,
    coalesce(nullif(m.content, ''), m.normalized_content, '') as body
  from messages m
  left join channels c on c.id = m.channel_id
  left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
  where $human_filter
    and $focus_filter
  order by m.rowid desc
  limit 50000
)"

cat >"$TMP_DIR/context.md" <<EOF
# $FOCUS_LABEL Field Notes Context

Generated at: $(date -u '+%Y-%m-%d %H:%M UTC')
Discord discussion focus: messages mentioning "$FOCUS_LABEL"
GitHub repo for issue/PR correlation: $GH_REPO

Rules for interpreting this context:
- Focus only on $FOCUS_LABEL chatter; do not generalize from unrelated OpenClaw conversation.
- Call out when the human Discord sample is thin or mostly GitHub integration traffic.
- Prefer human discussion samples and GitHub issue/PR titles when explaining trends.
- Ignore bot/integration message volume as an insight.
- No secrets, tokens, raw internal IDs, or private URLs.
EOF

run_sql "Archive Totals" "
select
  count(*) as messages,
  count(distinct channel_id) as channels,
  count(distinct author_id) as authors,
  max(created_at) as latest_message
from messages;
"

run_sql "$FOCUS_LABEL Activity Windows" "
select '24h' as window, count(*) as messages, count(distinct author_id) as authors, count(distinct channel_id) as channels
from messages m
left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
where $human_filter and $focus_filter and created_at >= strftime('%Y-%m-%dT%H:%M:%fZ', datetime($anchor_expr, '-1 day'))
union all
select '7d', count(*), count(distinct author_id), count(distinct channel_id)
from messages m
left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
where $human_filter and $focus_filter and created_at >= $since_7
union all
select '30d', count(*), count(distinct author_id), count(distinct channel_id)
from messages m
left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
where $human_filter and $focus_filter and created_at >= $since_30;
"

run_sql "$FOCUS_LABEL Hot Channels" "
$recent_human_cte
select channel, count(*) as messages
from recent
group by 1
order by messages desc
limit 10;
"

run_sql "$FOCUS_LABEL Recent Samples" "
$recent_human_cte
select created_at, channel, author, substr(body, 1, 420) as sample
from recent
order by created_at desc
limit 24;
"

run_sql "$FOCUS_LABEL Friction Samples" "
$recent_human_cte
select created_at, channel, author, substr(body, 1, 420) as sample
from recent
where lower(body) like '%bug%'
   or lower(body) like '%broken%'
   or lower(body) like '%fail%'
   or lower(body) like '%error%'
   or lower(body) like '%suspicious%'
   or lower(body) like '%hidden%'
   or lower(body) like '%publish%'
   or lower(body) like '%install%'
   or lower(body) like '%confusing%'
order by created_at desc
limit 24;
"

{
  printf "\n## GitHub Issues Updated This Month\n\n"
  if command -v gh >/dev/null 2>&1; then
    gh search issues --repo "$GH_REPO" --updated ">=$github_since" \
      --json number,title,state,updatedAt,url,labels \
      --limit 35 | jq -c . || printf "[]\n"
  else
    printf "gh unavailable\n"
  fi

  printf "\n## GitHub Pull Requests Updated This Month\n\n"
  if command -v gh >/dev/null 2>&1; then
    gh search prs --repo "$GH_REPO" --updated ">=$github_since" \
      --json number,title,state,updatedAt,url,labels \
      --limit 35 | jq -c . || printf "[]\n"
  else
    printf "gh unavailable\n"
  fi
} >>"$TMP_DIR/context.md"

cat >"$TMP_DIR/prompt.md" <<EOF
You are an OpenClaw agent writing a private Discord backup side report.

Use the context below. Return ONLY Markdown for the file $OUTPUT_FILE.

Required shape:

# $FOCUS_LABEL Field Notes

Last generated: <UTC timestamp>

## Current Chatter
- 3-5 specific bullets.

## Friction And Questions
- 3-5 specific bullets.
- Correlate Discord topics with GitHub issue/PR clusters when the context supports it.

## GitHub Issues To Watch
- Pick 1-3 likely highest-leverage GitHub issues or PRs, include title and URL if available, and explain why.

Hard rules:
- Focus only on $FOCUS_LABEL chatter.
- Do not include deterministic fallback language.
- Do not overstate thin evidence.
- Prefer concrete product/topic names over channel-volume trivia.
- No secrets, tokens, raw internal IDs, or private URLs.
- No bullying or dunking on individual people.

Context:

$(cat "$TMP_DIR/context.md")
EOF

attempt=1
while [ "$attempt" -le "$OPENCLAW_ATTEMPTS" ]; do
  if run_openclaw_agent >"$TMP_DIR/openclaw-result.json" 2>&1; then
    extract_openclaw_text >"$TMP_DIR/field-notes.md"
    if grep -Eq "^# ${FOCUS_LABEL} Field Notes[[:space:]]*$" "$TMP_DIR/field-notes.md"; then
      break
    fi
    echo "openclaw returned invalid clawhub field notes on attempt $attempt; retrying" >&2
  else
    echo "openclaw clawhub field notes failed on attempt $attempt; retrying" >&2
  fi
  if [ "$attempt" -eq "$OPENCLAW_ATTEMPTS" ]; then
    echo "openclaw did not return valid clawhub field notes; refusing to write deterministic notes" >&2
    cat "$TMP_DIR/openclaw-result.json" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 5
done

mkdir -p "$(dirname "$OUTPUT_PATH")"
cp "$TMP_DIR/field-notes.md" "$OUTPUT_PATH"
