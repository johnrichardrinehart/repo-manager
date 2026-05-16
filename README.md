# repo-manager

`repo-manager` provides the `repo` CLI for managing local Git repository
placement with stable metadata. `repod` is packaged separately as the companion
RPC daemon. Both binaries reuse the shared `repo-manager-core` crate.

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
worktrees live under `~/code/dev-worktrees`. Historical locator paths become
symlinks to the latest real path when a move is applied.

Existing checkouts can be registered without recloning:

```sh
repo manage ~/src/linux
```

`repo manage` accepts any subdirectory inside the checkout, moves the Git
worktree root into its managed locator path when needed, records it in
repo-manager metadata, and asks `repod` to review repositories under the clone
root for shared Git history.

## Remotes

`repo move` updates `origin` to the new locator. `repo reconcile` does the
same for detected moves, preserving the existing remote URL style when
possible.

Forks are Git worktrees under the clone root, not development worktrees under
the dev-worktree root. Each fork gets a stable remote name derived from its
locator, so the canonical checkout and every fork worktree share the same
`git remote -v` view: `origin` plus all fork remotes.

## Daemon API

Repository lifecycle RPC is defined in `api/repo_manager/v1/rpc.proto` and
encoded with Protocol Buffers over Unix domain sockets. The `repo` client sends
clone and manage events with its own `scan_root`; the `repod` process uses that
event root when comparing repositories for shared Git history. `repod` is
intended to run as the same user as the `repo` client, using the same config and
state database. Shared-history review is enabled by default and can be disabled
with `--detect-related=false` or `REPO_MANAGER_DETECT_RELATED=false`.

RPC clients and daemons include an envelope protocol version. The current
protocol is v1; breaking protobuf changes require a v2 protocol and are
rejected by mismatched peers.

## Configuration

Persist common values with:

```sh
repo setup --root ~/code
```

Use `repo setup --file <path>` to write a specific config file. The config file
loaded by default is `$XDG_CONFIG_HOME/repo-manager/config.json`.
Runtime environment variables and top-level CLI options override persisted
values.

The metadata database defaults to `$XDG_STATE_HOME/repo-manager/repos.sqlite`.
Disposable forge metadata, such as GitHub API responses used by `repo
reconcile`, is cached under `$XDG_CACHE_HOME/repo-manager`.

## Development

The Rust workspace is split into separate binary crates:

- `crates/repo-manager` builds the `repo` client.
- `crates/repod` builds the `repod` daemon.
- `crates/repo-manager-core` holds shared implementation code.

The flake exposes separate `repo-manager` and `repod` derivations.

```sh
direnv allow
nix develop
cargo test --all-targets
nix flake check
```
