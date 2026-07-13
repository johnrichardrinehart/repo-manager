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

New remote repositories can be created and immediately cloned into the managed
clone root:

```sh
repo create https://github.com/me/new-project --private
```

`repo create` infers GitHub and SourceHut from `github.com` and `git.sr.ht`.
Other forges must be configured explicitly, for example:

```json
{
  "config_version": 1,
  "create_default_visibility": "private",
  "forges": {
    "git.example.test": {
      "backend": "forgejo",
      "token_env": "EXAMPLE_FORGE_TOKEN",
      "api_base_url": "https://git.example.test"
    }
  }
}
```

The command reads forge tokens from environment variables, fails if the remote
repository already exists, and refuses to overwrite an existing local target
path.

## Remotes

`repo move` updates `origin` to the new locator. `repo reconcile` does the
same for detected moves, preserving the existing remote URL style when
possible.

By default, forks and mirrors are Git worktrees under the clone root, not
development worktrees under the dev-worktree root. Each fork or mirror gets a
stable remote name derived from its locator, so the canonical checkout and
every dependent checkout share the same `git remote -v` view: `origin` plus all
dependent remotes.

Set `clone-as-bare` to `true` in the config file to keep clone-root repositories
bare. In that mode, `repo clone` creates bare repositories, `repo fork` records
bare fork repositories instead of fork worktrees, and fork/mirror repair avoids
creating checked-out dependent trees. Checked-out working trees should be
created under the dev-worktree root. `repo worktree add` delegates their
lifecycle to Git while supplying the managed path and, for fork views, mapping
namespaced refs and configuring fork-safe pushes. Worktrees are not persisted
in repo-manager's database, so direct `git worktree add`, `git worktree remove`,
and `git worktree prune` remain authoritative. `repo check` reports existing
non-bare clone-root repositories as repairable; `repo check --repair` converts
clean managed checkouts to bare repositories.

## Daemon API

Repository lifecycle RPC is defined in `api/repo_manager/v1/rpc.proto` and
encoded with Protocol Buffers over Unix domain sockets. The `repo` client sends
clone and manage events with its own `scan_root`; the `repod` process uses that
event root when comparing repositories for shared Git history. `repod` is
intended to run as the same user as the `repo` client, using the same config and
state database. Shared-history review is enabled by default and can be disabled
with `--detect-related=false` or `REPO_MANAGER_DETECT_RELATED=false`.
Set `background-fetch-minimum-interval-seconds` to make `repod` periodically
fetch tracked repositories. The daemon records fetch activity in SQLite, backs
off quiet repositories, and resets active repositories to the configured
minimum interval when a fetch observes ref changes.

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

Config files are versioned JSON and validated against the matching JSON Schema
before being deserialized. `repo setup` writes `config_version: 1`; existing
unversioned config files are treated as v1. The v1 schema lives at
`crates/repo-manager-core/schemas/config/v1/schema.json`; incompatible config
format changes should add a new versioned schema instead of changing v1.

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
