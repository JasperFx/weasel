# Pre-staged chip prompts for Weasel

The coordinator session (rooted in `jasperfx`) drops `mcp__ccd_session__spawn_task`
chips off whatever repo the *coordinator's* conversation is rooted in. Chips
intended for Weasel can't be spawned directly from the jasperfx coordinator
without the resulting sub-agent landing in a jasperfx worktree it can't escape.

Workaround: the coordinator stashes the chip prompt here as a markdown file,
and a Claude Code session opened from this repo (`/Users/jeremymiller/code/weasel`)
drops the chip locally. The spawned sub-agent then gets a Weasel-rooted
worktree, which is what we want.

## How to use

From a Weasel-rooted Claude Code session:

> Please drop a chip from `.claude/chips/<name>.md` — title, tldr, and prompt
> are all in the file.

Each chip file has three sections (`# Title`, `## TL;DR`, then `## Prompt`).
The session reads the file and calls `spawn_task` with each section mapped to
the corresponding parameter.

## Conventions

- One file per chip, named `<short-slug>.md`.
- Delete the file after the chip's PR merges — these are ephemeral.
- `.claude/chips/` is gitignored — these don't get committed.

This pattern is mirrored in CritterWatch, Wolverine, and Polecat's `.claude/chips/`.
