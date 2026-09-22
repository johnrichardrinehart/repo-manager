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

Canonical repositories live under `~/code/clones` as bare repositories. Forks
and mirrors live there too, as namespace views: a small JSON file at the
dependent's locator path whose refs are stored inside the canonical bare
repository under `refs/repo-manager/<forks|mirrors>/<locator>/`. Nothing under
the clone root has a working tree. Development worktrees live under
`~/code/dev-worktrees`. Historical locator paths become symlinks to the latest
real path when a move is applied.

Existing checkouts can be registered without recloning:

```sh
repo manage ~/src/linux
```

`repo manage` accepts any subdirectory inside the checkout. The checkout must
be clean: its Git directory moves to the managed locator path and becomes the
bare repository (canonical) or is folded into its canonical repository's
namespace as a view (fork or mirror, cloning the canonical first if needed),
and the branch that was checked out becomes a development worktree under the
dev-worktree root. Canonical checkouts are then handed to `repod` to review
repositories under the clone root for shared Git history.

New remote repositories can be created and immediately cloned into the managed
clone root:

```sh
repo create github.com/me/new-project --private
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

When constructing a remote URL from a locator such as `github.com/me/project`,
repo-manager uses `git@github.com:me/project`: SSH, with no added `.git` suffix.
This policy is the same for every forge. Explicit URLs retain their transport,
username, port, and `.git` suffix if supplied; use an explicit HTTPS URL when
HTTPS is desired.

`repo create --no-auto-create-remote` initializes a local repository with `origin`
and tracking configuration for its initial branch. Once the remote exists and
the branch has a commit, a plain `git push` works with Git's default `simple`
push policy, without a separate `--set-upstream` step. This also applies to the
initial branch checked out in a worktree of a locally created bare repository.
Existing repositories are not automatically migrated to this configuration.

`repo move` updates `origin` to the new locator. `repo reconcile` does the
same for detected moves, preserving the existing remote URL style when
possible.

Forks and mirrors are namespace views of their canonical bare repository, not
separate clones: `repo fork` fetches the fork's refs into the canonical Git dir
and writes the view file at the fork's locator path. Checked-out working trees
are created under the dev-worktree root. `repo worktree add` delegates their
lifecycle to Git while supplying the managed path and, for fork views, mapping
namespaced refs and configuring fork-safe pushes. Worktrees are not persisted
in repo-manager's database, so direct `git worktree add`, `git worktree remove`,
and `git worktree prune` remain authoritative. After removing worktrees,
`repo worktree clean` removes empty repository and owner directories while
preserving each authority directory. It leaves active worktrees intact and
exits nonzero with a list of stale subtrees that contain any file, hidden
entry, or symlink.

`repo check` fails on any managed clone-root repository that is neither bare
nor a view, and on fork or mirror checkouts from before views existed;
`repo check --repair` converts clean checkouts to bare repositories and
replaces legacy dependent checkouts with views. The `clone-as-bare` config key
is accepted for compatibility with existing files and ignored; setting it to
`false` is an error.

Temporary refs use the `refs/tmp/` namespace. Run `repo refs gc --dry-run` to
review old temporary refs. Run `repo refs gc` to remove refs older than the
30-day grace period when a stable ref already reaches their target commit.
The collector keeps newer refs and refs that hold otherwise unreachable
commits. Pass `--prune-unreachable` after review to remove old refs that hold
unique commits.

### Identities

SSH keys and signing keys can be pinned per locator prefix under `identities`.
Keys are `<authority>`, `<authority>/<owner>`, `<authority>/<owner>/<repo>`, or
any deeper prefix of a locator. When a repository is cloned, created, managed,
moved, or forked, repo-manager resolves its locator against every matching
prefix, least specific first; each field set on a more specific entry overrides
the same field from a less specific one, so an owner entry that only sets
`signing-key` still inherits the authority's `ssh-identity-file`.

```json
{
  "config_version": 1,
  "identities": {
    "github.com": {
      "ssh-identity-file": "~/.ssh/id_ed25519_personal.pub",
      "signing-key": "~/.ssh/id_ed25519_personal.pub"
    },
    "github.com/work-org": {
      "ssh-identity-file": "~/.ssh/id_ed25519_work.pub",
      "signing-key": "0123456789ABCDEF",
      "signing-format": "openpgp"
    },
    "github.com/work-org/legacy-service": {
      "signing-key": "~/.ssh/id_ed25519_work.pub"
    }
  }
}
```

The resolved identity is written to the repository's Git config:

- `ssh-identity-file` sets `core.sshCommand` to
  `ssh -o IdentitiesOnly=yes -i <file>`. A `.pub` path is enough when the
  private key lives in an SSH agent; `IdentitiesOnly` stops the agent from
  offering another account's key first. Clones, `ls-remote`, and fetches that
  run before or outside the repository's own config use the same command via
  `GIT_SSH_COMMAND`.
- `signing-key` sets `user.signingKey`, `commit.gpgsign=true`, and
  `tag.gpgsign=true`. `gpg.format`
  is set to `signing-format` when given; otherwise it is set to `ssh` when the
  key looks like an SSH key (a path, `.pub`, or `key::ssh-…`) and left to
  Git's default for OpenPGP key ids. Paths are written as configured: a
  leading `~` is left for ssh and Git to expand, so a managed tree shared
  between hosts with different home directories resolves on each.

Forks and other dependents that share a canonical repository's Git dir get
their own identity in worktree-scoped config (`git config --worktree`), so a
fork under another account pushes with that account's key while the canonical
checkout keeps its own. Namespace views are not Git directories; their
worktrees receive the config when created with `repo worktree add`.

repo-manager owns a key group only once some identity entry configures it: if
no entry sets `signing-key`, signing config in managed repositories is never
touched, and an empty `identities` map changes nothing. Within an owned group,
a locator that resolves to no value has the keys removed from the repository
so Git falls back to global config; `repo move` between owners therefore swaps
identities rather than leaving a stale one. `repo check` reports managed
repositories whose config disagrees with their identity and `repo check
--repair` applies it, which is how existing repositories pick up new
`identities` entries.

`core.sshCommand` is per repository, not per remote. A non-bare canonical
checkout that carries a fork as an extra remote fetches that remote with the
canonical identity except during `repo fork`, which passes the fork's key.

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

Git worktrees are linked to their clone with relative paths by default:
repo-manager asks Git to store the links between `clones` and `dev-worktrees`
relative to each other and records the same policy in each repository for
direct `git worktree add` commands, so a root shared across host and guest
filesystems with different mount prefixes resolves from both sides. Relative
links need Git 2.48 or newer, and a repository that has one carries the
`extensions.relativeWorktrees` marker, which older Git refuses to open. Set
`worktree-use-relative-paths` to `false` (or pass
`--worktree-use-relative-paths=false` / set
`REPO_MANAGER_WORKTREE_USE_RELATIVE_PATHS=false`) where repositories must stay
readable by such a Git. Existing absolute links can be converted with
`git worktree repair --relative-paths <worktree>...`.

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
