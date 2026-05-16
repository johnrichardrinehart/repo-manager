# repo-manager

`repo-manager` provides the `repo` CLI for managing local Git repository
placement with stable metadata.

The storage model is based on a generic Git locator:

```text
<authority>/<remote-path>
```

Examples:

```text
github.com/torvalds/linux
git.sr.ht/~sircmpwn/scdoc
git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux
```

Canonical repositories and forks live under `~/code/clones`. Development
worktrees live under `~/code/worktrees`. Historical locator paths become
symlinks to the latest real path when a move is applied.

## Remotes

`repo move` updates `origin` to the new locator. `repo reconcile` does the
same for detected moves, preserving the existing remote URL style when
possible.

Forks are Git worktrees under the clone root, not development worktrees under
the worktree root. Each fork gets a stable remote name derived from its locator,
so the canonical checkout and every fork worktree share the same `git remote -v`
view: `origin` plus all fork remotes.

## Daemon API

Clone lifecycle RPC is defined in `api/repo_manager/v1/rpc.proto` and encoded
with Protocol Buffers. The `repo` client sends clone events with its own
`scan_root`; the `repod` process uses that event root when comparing
repositories for shared Git history. `repod` is intended to run as the same user
as the `repo` client, using the same config and state database. Shared-history
review is enabled by default and can be disabled with `--detect-related=false`
or `REPO_MANAGER_DETECT_RELATED=false`.

RPC clients and daemons include an envelope protocol version. The current
protocol is v1; breaking protobuf changes require a v2 protocol and are
rejected by mismatched peers.

## Configuration

Persist common values with:

```sh
repo setup --clone-root ~/code/clones --worktree-root ~/code/worktrees
```

Use `repo setup --file <path>` to write a specific config file. The config file
loaded by default is `$XDG_CONFIG_HOME/repo-manager/config.json`.
Runtime environment variables and top-level CLI options override persisted
values.

The metadata database defaults to `$XDG_STATE_HOME/repo-manager/repos.sqlite`.
Disposable forge metadata, such as GitHub API responses used by `repo
reconcile`, is cached under `$XDG_CACHE_HOME/repo-manager`.

## Development

```sh
direnv allow
nix develop
cargo test --all-targets
nix flake check
```
