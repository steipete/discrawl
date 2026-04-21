#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <discrawl-config> <backup-repo>" >&2
  exit 2
fi

CONFIG=$1
BACKUP_REPO=$2
OPENCLAW_BIN=${OPENCLAW_BIN:-openclaw}
OPENCLAW_TIMEOUT=${OPENCLAW_TIMEOUT:-300}
OPENCLAW_THINKING=${OPENCLAW_THINKING:-low}
GH_REPO=${DISCORD_FIELD_NOTES_GITHUB_REPO:-openclaw/openclaw}
START_MARKER="<!-- discrawl-field-notes:start -->"
END_MARKER="<!-- discrawl-field-notes:end -->"
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

fallback_query() {
  local query=$1
  if ! DISCRAWL_NO_AUTO_UPDATE=1 go run ./cmd/discrawl --config "$CONFIG" --json sql "$query"; then
    printf '[]\n'
  fi
}

write_fallback_notes() {
  local generated_at
  local latest_message
  generated_at=$(date -u '+%Y-%m-%d %H:%M UTC')
  latest_message=$(
    fallback_query "select max(created_at) as latest_message from messages;" |
      jq -r 'if type == "array" then .[0].latest_message // "unknown" else .rows[0][0] // "unknown" end'
  )

  fallback_query "$recent_human_cte
select topic, count(*) as matches
from (
  select case
    when lower(body) like '%thank%' or lower(body) like '%helpful%' or lower(body) like '%useful%' then 'Helpful answers and practical fixes'
    when lower(body) like '%fast%' or lower(body) like '%speed%' or lower(body) like '%quick%' then 'Speed and responsiveness'
    when lower(body) like '%agent%' or lower(body) like '%workflow%' or lower(body) like '%automatic%' then 'Agent workflows and automation'
    when lower(body) like '%skill%' or lower(body) like '%tool%' or lower(body) like '%mcp%' then 'Skills, tools, and MCP integrations'
    else 'General positive feedback'
  end as topic
  from recent
  where $body_love_terms
)
group by 1
order by matches desc
limit 4;
" | jq -r '
  if type == "array" then .[] else (.rows // [])[] | {topic: .[0], matches: .[1]} end |
  "- " + .topic + ": " + (.matches | tostring) + " positive mentions in the recent sample."
' >"$TMP_DIR/fallback-love.md"

  fallback_query "$recent_human_cte
select topic, count(*) as matches
from (
  select case
    when lower(body) like '%overload%' or lower(body) like '%fallback%' or lower(body) like '%provider%' or lower(body) like '%model%' then 'Provider reliability and model fallback'
    when lower(body) like '%token%' or lower(body) like '%secret%' or lower(body) like '%auth%' or lower(body) like '%config%' or lower(body) like '%install%' then 'Setup, auth, and configuration'
    when lower(body) like '%github%' or lower(body) like '%repo%' or lower(body) like '%pr%' or lower(body) like '%issue%' then 'GitHub and repository workflow'
    when lower(body) like '%skill%' or lower(body) like '%tool%' or lower(body) like '%mcp%' then 'Skills, tools, and runtime bridges'
    when lower(body) like '%encoding%' or lower(body) like '%character%' or lower(body) like '%message%' then 'Message quality, editing, and encoding'
    else 'General bugs, failures, and confusing behavior'
  end as topic
  from recent
  where $body_complaint_terms
)
group by 1
order by matches desc
limit 4;
" | jq -r '
  if type == "array" then .[] else (.rows // [])[] | {topic: .[0], matches: .[1]} end |
  "- " + .topic + ": " + (.matches | tostring) + " complaint-flavored mentions in the recent sample; compare with the issue/PR cluster below."
' >"$TMP_DIR/fallback-complaints.md"

  if command -v gh >/dev/null 2>&1; then
    gh search issues --repo "$GH_REPO" --updated ">=$github_since" \
      --json number,title,state,updatedAt,url,labels \
      --limit 8 | jq -r '.[0:3][]? | "- Issue #" + (.number | tostring) + " (" + .state + "): [" + .title + "](" + .url + ")"' >"$TMP_DIR/fallback-issues.md" || :
    gh search prs --repo "$GH_REPO" --updated ">=$github_since" \
      --json number,title,state,updatedAt,url,labels \
      --limit 25 | jq -r '
        def score($title):
          [ "fix", "bug", "fail", "error", "provider", "fallback", "auth", "config", "skill", "tool", "github", "encoding" ]
          | map(if $title | contains(.) then 1 else 0 end)
          | add;
        map(. + {score: score(.title | ascii_downcase)})
        | map(select(.state == "open")) as $open
        | ($open // .) | sort_by(.score, .updatedAt) | reverse | .[0] // empty
        | "- PR #" + (.number | tostring) + ": [" + .title + "](" + .url + ") is the best recent watch candidate from title/recency signals."
      ' >"$TMP_DIR/fallback-pr.md" || :
  fi

  if [ ! -s "$TMP_DIR/fallback-love.md" ]; then
    printf '%s\n' "- Not enough clear positive signal in the latest 30-day window." >"$TMP_DIR/fallback-love.md"
  fi
  if [ ! -s "$TMP_DIR/fallback-complaints.md" ]; then
    printf '%s\n' "- Not enough clear complaint signal in the latest 30-day window." >"$TMP_DIR/fallback-complaints.md"
  fi
  if [ -s "$TMP_DIR/fallback-issues.md" ]; then
    {
      printf '\n'
      printf '%s\n' "Related GitHub issue cluster:"
      cat "$TMP_DIR/fallback-issues.md"
    } >>"$TMP_DIR/fallback-complaints.md"
  fi
  if [ ! -s "$TMP_DIR/fallback-pr.md" ]; then
    printf '%s\n' "- No recent PR signal available from GitHub search." >"$TMP_DIR/fallback-pr.md"
  fi

  {
    printf '### Field Notes\n\n'
    printf 'Last generated: %s\n\n' "$generated_at"
    printf '_OpenClaw agent timed out; generated deterministic field notes from the same archive and GitHub context._\n\n'
    printf 'Latest archived message: %s\n\n' "$latest_message"
    printf '#### What People Love\n'
    cat "$TMP_DIR/fallback-love.md"
    printf '\n#### What People Complain About\n'
    cat "$TMP_DIR/fallback-complaints.md"
    printf '\n#### Best PR To Watch\n'
    cat "$TMP_DIR/fallback-pr.md"
  }
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
  jq -r '
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
  ' "$TMP_DIR/openclaw-result.json"
}

anchor_expr="(select max(created_at) from messages)"
since_7="strftime('%Y-%m-%dT%H:%M:%fZ', datetime($anchor_expr, '-7 days'))"
since_30="strftime('%Y-%m-%dT%H:%M:%fZ', datetime($anchor_expr, '-30 days'))"
human_filter="lower(coalesce(mem.username, mem.display_name, m.author_id, '')) not in ('github', 'dependabot')"
love_terms="(lower(coalesce(normalized_content, content, '')) like '%love%' or lower(coalesce(normalized_content, content, '')) like '%great%' or lower(coalesce(normalized_content, content, '')) like '%awesome%' or lower(coalesce(normalized_content, content, '')) like '%amazing%' or lower(coalesce(normalized_content, content, '')) like '%thanks%' or lower(coalesce(normalized_content, content, '')) like '%thank you%' or lower(coalesce(normalized_content, content, '')) like '%works%' or lower(coalesce(normalized_content, content, '')) like '%useful%' or lower(coalesce(normalized_content, content, '')) like '%helpful%' or lower(coalesce(normalized_content, content, '')) like '%fast%')"
complaint_terms="(lower(coalesce(normalized_content, content, '')) like '%bug%' or lower(coalesce(normalized_content, content, '')) like '%broken%' or lower(coalesce(normalized_content, content, '')) like '%fail%' or lower(coalesce(normalized_content, content, '')) like '%error%' or lower(coalesce(normalized_content, content, '')) like '%crash%' or lower(coalesce(normalized_content, content, '')) like '%regression%' or lower(coalesce(normalized_content, content, '')) like '%slow%' or lower(coalesce(normalized_content, content, '')) like '%confusing%' or lower(coalesce(normalized_content, content, '')) like '%annoying%' or lower(coalesce(normalized_content, content, '')) like '%not working%' or lower(coalesce(normalized_content, content, '')) like '%cannot%' or lower(coalesce(normalized_content, content, '')) like '%can''t%')"
body_love_terms="(lower(body) like '%love%' or lower(body) like '%great%' or lower(body) like '%awesome%' or lower(body) like '%amazing%' or lower(body) like '%thanks%' or lower(body) like '%thank you%' or lower(body) like '%works%' or lower(body) like '%useful%' or lower(body) like '%helpful%' or lower(body) like '%fast%')"
body_complaint_terms="(lower(body) like '%bug%' or lower(body) like '%broken%' or lower(body) like '%fail%' or lower(body) like '%error%' or lower(body) like '%crash%' or lower(body) like '%regression%' or lower(body) like '%slow%' or lower(body) like '%confusing%' or lower(body) like '%annoying%' or lower(body) like '%not working%' or lower(body) like '%cannot%' or lower(body) like '%can''t%')"
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
  order by m.rowid desc
  limit 50000
)"
github_since=$(date_utc_days_ago 30)

cat >"$TMP_DIR/context.md" <<EOF
# Discord Backup Field Notes Context

Generated at: $(date -u '+%Y-%m-%d %H:%M UTC')
GitHub repo for issue/PR correlation: $GH_REPO

Rules for interpreting this context:
- Ignore bot/integration message volume as an insight.
- Do not treat "GitHub posted the most" as funny or useful.
- Prefer human discussion samples and GitHub issue/PR titles when explaining trends.
- Avoid repeating bracketed channel prefixes such as [openclaw]; use concise channel names.
EOF

run_sql "Archive Totals" "
select
  count(*) as messages,
  count(distinct channel_id) as channels,
  count(distinct author_id) as authors,
  max(created_at) as latest_message
from messages;
"

run_sql "Activity Windows" "
select '24h' as window, count(*) as messages, count(distinct author_id) as authors, count(distinct channel_id) as channels
from messages where created_at >= strftime('%Y-%m-%dT%H:%M:%fZ', datetime($anchor_expr, '-1 day'))
union all
select '7d', count(*), count(distinct author_id), count(distinct channel_id)
from messages where created_at >= $since_7
union all
select '30d', count(*), count(distinct author_id), count(distinct channel_id)
from messages where created_at >= $since_30;
"

run_sql "Human Hot Channels This Week" "
$recent_human_cte
select channel, count(*) as messages
from recent
group by 1
order by messages desc
limit 8;
"

run_sql "What People Seem To Love In Recent Messages" "
$recent_human_cte
select channel, count(*) as matches
from recent
where $body_love_terms
group by 1
order by matches desc
limit 8;
"

run_sql "Love Samples" "
${recent_human_cte}
select created_at, channel, author, substr(body, 1, 260) as sample
from recent
where $body_love_terms
order by created_at desc
limit 10;
"

run_sql "What People Complain About In Recent Messages" "
$recent_human_cte
select channel, count(*) as matches
from recent
where $body_complaint_terms
group by 1
order by matches desc
limit 8;
"

run_sql "Complaint Samples" "
${recent_human_cte}
select created_at, channel, author, substr(body, 1, 320) as sample
from recent
where $body_complaint_terms
order by created_at desc
limit 12;
"

{
  printf "\n## GitHub Pull Requests Updated This Month\n\n"
  if command -v gh >/dev/null 2>&1; then
    gh search prs --repo "$GH_REPO" --updated ">=$github_since" \
      --json number,title,state,updatedAt,url,labels \
      --limit 25 | jq -c . || printf "[]\n"
  else
    printf "gh unavailable\n"
  fi
  printf "\n## GitHub Issues Updated This Month\n\n"
  if command -v gh >/dev/null 2>&1; then
    gh search issues --repo "$GH_REPO" --updated ">=$github_since" \
      --json number,title,state,updatedAt,url,labels \
      --limit 25 | jq -c . || printf "[]\n"
  else
    printf "gh unavailable\n"
  fi
} >>"$TMP_DIR/context.md"

cat >"$TMP_DIR/prompt.md" <<EOF
You are an OpenClaw agent writing the private Discord backup field notes.

Use the context below. Return ONLY Markdown for insertion between README markers.

Required shape:

### Field Notes

Last generated: <UTC timestamp>

#### What People Love
- 3-5 specific bullets.

#### What People Complain About
- 3-5 specific bullets.
- Correlate complaint topics with GitHub issue/PR clusters when the context supports it.

#### Best PR To Watch
- Pick the likely highest-leverage PR from GitHub context, include its title and URL if available, and explain why.

Hard rules:
- Do not say GitHub being the top poster is funny or noteworthy.
- Do not overuse the literal "[openclaw]" prefix.
- Prefer concrete product/topic names over channel-volume trivia.
- No secrets, tokens, raw internal IDs, or private URLs.
- No bullying or dunking on individual people.
- Be useful first, funny only if the evidence earns it.

Context:

$(cat "$TMP_DIR/context.md")
EOF

if run_openclaw_agent >"$TMP_DIR/openclaw-result.json"; then
  extract_openclaw_text >"$TMP_DIR/field-notes.md"
else
  echo "openclaw field notes failed; using deterministic fallback" >&2
  write_fallback_notes >"$TMP_DIR/field-notes.md"
fi
if [ ! -s "$TMP_DIR/field-notes.md" ]; then
  echo "openclaw did not return field notes text; using deterministic fallback" >&2
  write_fallback_notes >"$TMP_DIR/field-notes.md"
fi

awk -v start="$START_MARKER" -v end="$END_MARKER" -v notes="$TMP_DIR/field-notes.md" '
  BEGIN {
    while ((getline line < notes) > 0) {
      body = body line "\n"
    }
    close(notes)
  }
  index($0, start) {
    print start
    printf "%s", body
    in_notes = 1
    seen = 1
    next
  }
  index($0, end) {
    print end
    in_notes = 0
    next
  }
  in_notes {
    next
  }
  {
    print
    if (!seen && !inserted && index($0, "<!-- discrawl-report:end -->")) {
      print ""
      print start
      printf "%s", body
      print end
      inserted = 1
      seen = 1
    }
  }
  END {
    if (!seen) {
      print ""
      print start
      printf "%s", body
      print end
    }
  }
' "$BACKUP_REPO/README.md" >"$TMP_DIR/README.md"
mv "$TMP_DIR/README.md" "$BACKUP_REPO/README.md"
