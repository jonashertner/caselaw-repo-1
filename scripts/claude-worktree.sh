#!/usr/bin/env bash
# claude-worktree.sh — create or attach to a Claude worktree + tmux session.
#
# Usage:
#   scripts/claude-worktree.sh <name>             # new worktree off main, new tmux session
#   scripts/claude-worktree.sh <name> <base>      # new worktree off <base> (default: main)
#   scripts/claude-worktree.sh <name>             # rerunning: just attach
#   scripts/claude-worktree.sh                    # print usage + current worktrees
#
# Layout per session:
#   .claude/worktrees/<name>/   — git worktree on branch <name>
#   tmux session "claude-<name>" — 2 panes (left: ready for `claude`; right: spare shell)
#
# Idempotent. Rerunning with the same <name> attaches the existing session.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ]; then
  echo "error: not inside a git repo" >&2
  exit 1
fi

if [ "$#" -lt 1 ] || [ -z "${1:-}" ]; then
  cat >&2 <<EOF
usage: $(basename "$0") <name> [base-branch]
       new worktree at .claude/worktrees/<name>/ on branch <name>, off [base-branch] (default: main).
       launches tmux session "claude-<name>" with 2 panes; rerunning just attaches.

current worktrees:
EOF
  git -C "$REPO_ROOT" worktree list >&2
  exit 1
fi

NAME="$1"
BASE="${2:-main}"

case "$NAME" in
  *[!a-zA-Z0-9_-]*)
    echo "error: name must match [a-zA-Z0-9_-]+ (got: $NAME)" >&2
    exit 1
    ;;
esac

WT_DIR="$REPO_ROOT/.claude/worktrees/$NAME"
BRANCH="$NAME"
SESSION="claude-$NAME"

if ! command -v tmux >/dev/null 2>&1; then
  echo "error: tmux not installed (brew install tmux)" >&2
  exit 1
fi

if [ ! -d "$WT_DIR" ]; then
  echo "→ creating worktree at $WT_DIR (branch $BRANCH from $BASE)"
  mkdir -p "$REPO_ROOT/.claude/worktrees"
  if git -C "$REPO_ROOT" rev-parse --verify --quiet "refs/heads/$BRANCH" >/dev/null; then
    git -C "$REPO_ROOT" worktree add "$WT_DIR" "$BRANCH"
  else
    git -C "$REPO_ROOT" worktree add -b "$BRANCH" "$WT_DIR" "$BASE"
  fi
  if [ -f "$REPO_ROOT/.env" ]; then
    cp "$REPO_ROOT/.env" "$WT_DIR/.env"
    echo "  copied .env into worktree"
  else
    echo "  note: no .env at repo root — worktree will run without it"
  fi
else
  echo "→ worktree $WT_DIR already exists"
fi

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "→ creating tmux session $SESSION"
  tmux new-session -d -s "$SESSION" -c "$WT_DIR"
  tmux split-window -h -t "$SESSION":0 -c "$WT_DIR"
  tmux select-pane -t "$SESSION":0.0
  tmux send-keys -t "$SESSION":0.1 "git status -sb && git log --oneline -5" C-m
else
  echo "→ tmux session $SESSION already exists"
fi

if [ -n "${TMUX:-}" ]; then
  echo "→ already inside tmux; switch with: tmux switch-client -t $SESSION"
elif [ ! -t 0 ] || [ ! -t 1 ]; then
  echo "→ session ready (non-interactive shell, not attaching). attach with: tmux attach -t $SESSION"
else
  tmux attach -t "$SESSION"
fi
