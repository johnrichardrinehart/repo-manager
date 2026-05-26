use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use directories::BaseDirs;
use log::{debug, warn};
use prost::Message;
use repo_help_derive::{HelpGroup, HelpTemplate};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use url::Url;

const DEFAULT_DETECT_RELATED: bool = true;
const CURRENT_CONFIG_VERSION: u32 = 1;
const RPC_PROTOCOL_VERSION: u32 = 1;
const BACKGROUND_FETCH_MIN_WAKE_SECONDS: u64 = 1;
const BACKGROUND_FETCH_MAX_WAKE_SECONDS: u64 = 300;
const REPO_VIEW_METADATA_FILE: &str = ".repo-manager-view.json";
const REPO_VIEW_METADATA_VERSION: u32 = 1;
const CONFIG_SCHEMA_V1: &str = include_str!("../schemas/config/v1/schema.json");

pub mod api {
    include!(concat!(env!("OUT_DIR"), "/repo_manager.v1.rs"));
}

#[derive(Debug, Parser)]
#[command(
    name = "repo",
    version,
    disable_help_subcommand = true,
    about = "Manage local Git repository placement, metadata, forks, worktrees, and old-path aliases",
    long_about = "Manage local Git repositories using a stable locator model: <authority>/<remote-path>.\n\nCanonical repositories and forks live under <root>/clones. Development worktrees live under <root>/dev-worktrees.\n\nWhen --config is omitted, repo-manager layers config from each $XDG_CONFIG_DIRS entry before the user config from $XDG_CONFIG_HOME. Environment variables and explicit CLI options override persisted config."
)]
pub struct Cli {
    #[command(flatten)]
    config: ConfigArgs,

    #[arg(
        long,
        global = true,
        value_name = "DIR",
        help = "Run repository-context commands as if started from DIR"
    )]
    dir: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        help = "Print command results as machine-readable JSON"
    )]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Default, Args)]
struct ConfigArgs {
    #[arg(
        long,
        env = "REPO_MANAGER_CONFIG",
        value_name = "PATH",
        help = "Config file path (default: $XDG_CONFIG_HOME/repo-manager/config.json)",
        long_help = "Config file path to load. When omitted, repo-manager layers /repo-manager/config.json from each $XDG_CONFIG_DIRS entry, then $XDG_CONFIG_HOME/repo-manager/config.json or ~/.config/repo-manager/config.json when XDG_CONFIG_HOME is unset."
    )]
    config: Option<PathBuf>,

    #[arg(
        long,
        env = "REPO_MANAGER_STATE",
        value_name = "PATH",
        help = "SQLite metadata database path (default: $XDG_STATE_HOME/repo-manager/repos.sqlite)",
        long_help = "SQLite metadata database path. Defaults to $XDG_STATE_HOME/repo-manager/repos.sqlite, or ~/.local/state/repo-manager/repos.sqlite when XDG_STATE_HOME is unset."
    )]
    state: Option<PathBuf>,

    #[arg(
        long,
        env = "REPO_MANAGER_CACHE_ROOT",
        value_name = "DIR",
        help = "XDG cache directory for disposable forge metadata (default: $XDG_CACHE_HOME/repo-manager)",
        long_help = "XDG cache directory for disposable forge metadata such as GitHub repository API responses. Defaults to $XDG_CACHE_HOME/repo-manager, or ~/.cache/repo-manager when XDG_CACHE_HOME is unset."
    )]
    cache_root: Option<PathBuf>,

    #[arg(
        long,
        env = "REPO_MANAGER_ROOT",
        value_name = "DIR",
        help = "Repo-manager root directory (default: ~/code)",
        long_help = "Repo-manager root directory. Canonical repositories and forks live under <root>/clones; development worktrees live under <root>/dev-worktrees. Defaults to ~/code."
    )]
    root: Option<PathBuf>,

    #[arg(
        long,
        env = "REPO_MANAGER_RPC_URL",
        value_name = "URL",
        help = "Unix-domain RPC endpoint for repository lifecycle events (default: user runtime socket)",
        long_help = "Unix-domain RPC endpoint for repository lifecycle events. Defaults to unix://$XDG_RUNTIME_DIR/repo-manager/socket when XDG_RUNTIME_DIR is set."
    )]
    rpc_url: Option<String>,

    #[arg(
        long,
        env = "REPO_MANAGER_CLIENT_ID",
        value_name = "UUID",
        help = "Stable client identifier sent with repository lifecycle RPC events"
    )]
    client_id: Option<String>,

    #[arg(
        long,
        env = "REPO_MANAGER_ASSUME_ORIGIN_AS_CANONICAL",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        help = "Treat origin as canonical during manage without prompting"
    )]
    assume_origin_as_canonical: Option<bool>,
}

#[derive(Debug, Parser)]
#[command(
    name = "repod",
    version,
    about = "Run the repo-manager RPC daemon",
    long_about = "Run the repo-manager RPC daemon.\n\nThe daemon receives repository lifecycle events from clients over a Unix domain socket. When related-history detection is configured, clone completion and manage requests make the daemon scan the client-provided event root for other Git repositories, compare commit history, and record pending relationship decisions."
)]
struct RepodCli {
    #[command(flatten)]
    config: DaemonConfigArgs,

    #[command(flatten)]
    daemon: DaemonArgs,
}

#[derive(Debug, Clone, Default, Args)]
struct DaemonConfigArgs {
    #[arg(
        long,
        env = "REPO_MANAGER_CONFIG",
        value_name = "PATH",
        help = "Config file path (default: $XDG_CONFIG_HOME/repo-manager/config.json)",
        long_help = "Config file path to load. When omitted, repo-manager layers /repo-manager/config.json from each $XDG_CONFIG_DIRS entry, then $XDG_CONFIG_HOME/repo-manager/config.json or ~/.config/repo-manager/config.json when XDG_CONFIG_HOME is unset."
    )]
    config: Option<PathBuf>,

    #[arg(
        long,
        env = "REPO_MANAGER_STATE",
        value_name = "PATH",
        help = "SQLite metadata database path (default: $XDG_STATE_HOME/repo-manager/repos.sqlite)",
        long_help = "SQLite metadata database path. Defaults to $XDG_STATE_HOME/repo-manager/repos.sqlite, or ~/.local/state/repo-manager/repos.sqlite when XDG_STATE_HOME is unset."
    )]
    state: Option<PathBuf>,

    #[arg(
        long,
        env = "REPO_MANAGER_RPC_URL",
        value_name = "URL",
        help = "Unix-domain RPC endpoint for repository lifecycle events (default: user runtime socket)",
        long_help = "Unix-domain RPC endpoint for repository lifecycle events. Defaults to unix://$XDG_RUNTIME_DIR/repo-manager/socket when XDG_RUNTIME_DIR is set."
    )]
    rpc_url: Option<String>,

    #[arg(
        long,
        env = "REPO_MANAGER_DETECT_RELATED",
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        help = "Enable shared-history review after clone completion and manage requests (default: true)"
    )]
    detect_related: Option<bool>,

    #[arg(
        long,
        env = "REPO_MANAGER_CLONE_START_TTL_MINUTES",
        value_name = "MINUTES",
        help = "TTL for in-progress clone events (default: 60)"
    )]
    clone_start_ttl_minutes: Option<u64>,

    #[arg(
        long,
        env = "REPO_MANAGER_RPC_RATE_LIMIT_PER_SECOND",
        value_name = "N",
        help = "RPC receive rate limit per client (default: 1; 0 disables)"
    )]
    rpc_rate_limit_per_second: Option<u32>,

    #[arg(
        long,
        env = "REPO_MANAGER_BACKGROUND_FETCH_MINIMUM_INTERVAL_SECONDS",
        value_name = "SECONDS",
        help = "Minimum interval between background fetches per tracked clone; disabled when unset"
    )]
    background_fetch_minimum_interval_seconds: Option<u64>,
}

#[derive(Debug, Subcommand, HelpTemplate)]
enum Commands {
    #[command(flatten, next_help_heading = "Setup")]
    Setup(SetupCommands),
    #[command(flatten, next_help_heading = "Repository operations")]
    RepositoryOperations(RepositoryOperationCommands),
    #[command(flatten, next_help_heading = "Organizational Changes")]
    OrganizationalChanges(OrganizationalChangeCommands),
    #[command(flatten, next_help_heading = "Organizational Analysis")]
    OrganizationalAnalysis(OrganizationalAnalysisCommands),
    #[command(flatten, next_help_heading = "Daemon")]
    Daemon(DaemonCommands),
}

#[derive(Debug, Subcommand, HelpGroup)]
#[help_group(title = "Setup")]
enum SetupCommands {
    #[command(
        about = "Persist common repo-manager settings to a config file",
        long_about = "Persist common repo-manager settings to a config file.\n\nValues written by setup are loaded on future runs from the selected file. Environment variables and top-level CLI options still override persisted config at runtime."
    )]
    Setup(SetupArgs),
}

#[derive(Debug, Subcommand, HelpGroup)]
#[help_group(title = "Repository operations")]
enum RepositoryOperationCommands {
    #[command(about = "Clone a repository into the managed clone root")]
    Clone(CloneArgs),
    #[command(
        about = "Create a remote repository and clone it into the managed clone root",
        long_about = "Create a remote repository on GitHub, SourceHut, or a configured Forgejo host, then clone it into the managed clone root.\n\nThe forge backend is inferred for github.com and git.sr.ht. Other authorities must be listed under `forges` in repo-manager config. The command fails if the remote repository already exists or if the target path already exists locally."
    )]
    Create(CreateArgs),
    #[command(about = "Fetch a managed repository or namespace-backed fork view")]
    Fetch(FetchArgs),
    #[command(about = "List or create branches for a managed repository or fork view")]
    Branch(BranchArgs),
    #[command(
        about = "Register an existing checkout under the managed clone root",
        long_about = "Register an existing Git checkout under the managed clone root without cloning it.\n\nUse this for a repository that already exists on disk. The command resolves the Git worktree root, chooses a canonical URL from its remotes or an interactive prompt, moves the checkout into its managed locator path when needed, records it in repo-manager metadata, and asks repod to review repositories under the clone root for shared Git history."
    )]
    Manage(ManageArgs),
    #[command(about = "Create or register a fork view for a canonical repository or fork parent")]
    Fork(ForkArgs),
    #[command(
        about = "Find missing tracked checkouts, unmanaged clone-root repos, and unmaterialized fork/mirror worktrees",
        long_about = "Find missing checkout paths, Git repositories under the clone root that are absent from repo-manager metadata, and incomplete fork/mirror worktree relationships.\n\nrepo-manager checks repositories and relationships recorded in its SQLite database, then scans the managed clone root for out-of-band checkouts whose origin remote can be parsed as a locator.\n\nBy default, this command is read-only. With --repair, it removes safe stale metadata rows, records repairable unmanaged clone-root checkouts, updates metadata for moved managed checkouts, and converts repairable fork/mirror checkouts into Git worktrees of their canonical checkouts."
    )]
    Check(CheckArgs),
    #[command(
        hide = true,
        about = "Deprecated alias for `repo check --repair`",
        long_about = "Deprecated alias for `repo check --repair`."
    )]
    Repair(DeprecatedRepairArgs),
    #[command(about = "Manage development worktrees under the managed dev-worktree root")]
    Worktree(WorktreeCommand),
    #[command(
        name = "repos",
        visible_alias = "repositories",
        about = "Inspect or change managed repository relationship metadata"
    )]
    Repos(ReposCommand),
}

#[derive(Debug, Subcommand, HelpGroup)]
#[help_group(title = "Organizational Changes")]
enum OrganizationalChangeCommands {
    // Move and successor are intentionally separate concepts. A move is the
    // same hosted repository at a new locator; a successor records that the
    // canonical source continued elsewhere after the old source stopped being
    // the canonical place to use. Successors do not alias paths or merge
    // repository records.
    #[command(
        about = "Same repo, new locator (e.g. renamed/transferred GitHub repo)",
        long_about = "Record and apply a move for the same hosted repository at a new locator.\n\nUse this when the same repository was renamed, transferred, or otherwise kept its hosted repository record but changed locator. `repo move` moves the real directory, records historical locators, updates remotes, and makes old paths aliases of the current path.\n\nDo not use this for canonicalization changes where the old source was archived, source-closed, deleted, or resumed elsewhere as a distinct repository. Use `repo successor set` for that."
    )]
    Move(MoveArgs),
    // Reconcile operates only on repositories already known to the metadata DB.
    // Arbitrary local-directory inventory is intentionally out of scope.
    #[command(
        about = "Apply URL/path changes for managed repos (e.g. forge redirect or origin drift)",
        long_about = "Detect managed repositories whose locator changed by probing supported forge metadata first, then by comparing the configured origin URL with the stored current locator.\n\nGitHub repository redirects are probed through the GitHub repository API. When drift is found, reconcile applies the same move behavior as `repo move`: it moves the real directory, records the new current locator, updates origin, and creates historical alias symlinks."
    )]
    Reconcile,
    #[command(
        about = "Canonicalization change (e.g. old source archived/source-closed/deleted)",
        long_about = "Record a canonicalization change without treating it as a repository move.\n\nUse this when the old source was archived, source-closed, deleted, or otherwise stopped being the canonical source, and development resumed under a different organization or repository. Successors are metadata only: they do not move the old checkout, do not create alias symlinks, and do not merge the old and new repository records.\n\nUse `repo move` instead for a rename, transfer, or locator change of the same hosted repository."
    )]
    Successor(SuccessorCommand),
}

#[derive(Debug, Subcommand, HelpGroup)]
#[help_group(title = "Organizational Analysis")]
enum OrganizationalAnalysisCommands {
    // Aliases are old locator paths created by moves. They are not shell
    // aliases and not alternate remotes.
    #[command(
        about = "Show old paths that symlink to the current moved repo path",
        long_about = "Show historical locator paths and old-path symlinks for a repository after moves.\n\nThese aliases are filesystem paths created by `repo move` or `repo reconcile`; they are not shell aliases and not Git remotes."
    )]
    Aliases(AliasesCommand),
    #[command(
        about = "Review repositories with shared Git history",
        long_about = "List and resolve shared-history candidates.\n\nThese are suggestions only: shared Git objects can mean mirrors, forks, moved repositories, vendor trees, or unrelated repositories with common ancestry."
    )]
    Related(RelatedCommand),
}

#[derive(Debug, Subcommand, HelpGroup)]
#[help_group(title = "Daemon")]
enum DaemonCommands {
    #[command(about = "Interact with the repo-manager daemon")]
    Daemon(DaemonCommand),
}

#[derive(Debug, Args)]
struct SetupArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Config file to write (default: same path as top-level --config)"
    )]
    file: Option<PathBuf>,

    #[arg(long, value_name = "PATH", help = "Persist the metadata database path")]
    state: Option<PathBuf>,

    #[arg(
        long,
        value_name = "DIR",
        help = "Persist the XDG cache directory for disposable forge metadata"
    )]
    cache_root: Option<PathBuf>,

    #[arg(
        long,
        value_name = "DIR",
        help = "Persist the repo-manager root directory"
    )]
    root: Option<PathBuf>,

    #[arg(
        long,
        value_name = "URL",
        help = "Persist the repository lifecycle RPC endpoint"
    )]
    rpc_url: Option<String>,

    #[arg(
        long,
        value_name = "UUID",
        help = "Persist a stable client identifier (default: generate one)"
    )]
    client_id: Option<String>,

    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        help = "Persist origin-as-canonical behavior for `repo manage`"
    )]
    assume_origin_as_canonical: Option<bool>,
}

#[derive(Debug, Args)]
struct DaemonArgs {
    #[arg(
        long,
        value_name = "URL",
        help = "Unix-domain RPC endpoint to listen on (default: configured RPC endpoint)"
    )]
    listen: Option<String>,
}

#[derive(Debug, Args)]
struct CloneArgs {
    #[arg(
        value_name = "URL",
        help = "Git URL or locator to clone",
        long_help = "Git URL or locator to clone. The URL is normalized into <authority>/<remote-path> and placed under the clone root."
    )]
    url: String,
}

#[derive(Debug, Args)]
struct CreateArgs {
    #[arg(
        value_name = "URL",
        help = "Git URL or locator for the repository to create",
        long_help = "Git URL or locator for the repository to create. The URL is normalized into <authority>/<remote-path>, created on the corresponding forge, and cloned under the clone root."
    )]
    url: String,

    #[arg(
        long,
        conflicts_with = "public",
        help = "Create the remote repository as private"
    )]
    private: bool,

    #[arg(
        long,
        conflicts_with = "private",
        help = "Create the remote repository as public"
    )]
    public: bool,
}

#[derive(Debug, Args)]
struct FetchArgs {
    #[arg(
        value_name = "REMOTE",
        default_value = "origin",
        help = "Remote to fetch (default: origin)"
    )]
    remote: String,
}

#[derive(Debug, Args)]
struct BranchArgs {
    #[arg(
        value_name = "BRANCH",
        help = "Branch to create; omit to list branches"
    )]
    branch: Option<String>,

    #[arg(
        value_name = "START_POINT",
        help = "Optional start point for a newly created branch"
    )]
    start_point: Option<String>,
}

#[derive(Debug, Args)]
struct ManageArgs {
    #[arg(
        value_name = "PATH",
        default_value = ".",
        help = "Existing Git checkout path or subdirectory to register"
    )]
    path: PathBuf,

    #[arg(long, help = "Treat origin as canonical without prompting")]
    assume_origin_as_canonical: bool,
}

#[derive(Debug, Args)]
struct ForkArgs {
    #[arg(
        value_name = "FORK_URL",
        help = "Git URL or locator for the fork repository"
    )]
    fork_url: String,

    #[arg(
        long,
        value_name = "CANONICAL_URL",
        help = "Immediate parent or canonical upstream Git URL or locator for this fork"
    )]
    canonical: String,
}

#[derive(Debug, Args)]
struct CheckArgs {
    #[arg(
        long,
        help = "Open an editor-backed repair plan for stale metadata, unmanaged checkouts, repository formats, and fork/mirror structure"
    )]
    repair: bool,

    #[arg(
        long,
        help = "Print a read-only JSON inventory of tracked repositories, discovered Git directories, and background fetch state"
    )]
    dump: bool,
}

#[derive(Debug, Args)]
struct DeprecatedRepairArgs {
    #[arg(long, help = "Deprecated; `repo check` is read-only by default")]
    check: bool,
}

#[derive(Debug, Args)]
struct MoveArgs {
    #[arg(
        value_name = "REPO_REF",
        help = "Existing same-hosted-repository URL, current locator, or historical locator"
    )]
    repo_ref: String,
    #[arg(
        value_name = "NEW_URL",
        help = "New Git URL or locator for the same hosted repository"
    )]
    new_url: String,
}

#[derive(Debug, Subcommand)]
enum WorktreeSubcommand {
    #[command(about = "Create a development worktree under the managed dev-worktree root")]
    Add(WorktreeAddArgs),
}

#[derive(Debug, Args)]
struct WorktreeCommand {
    #[command(subcommand)]
    command: WorktreeSubcommand,
}

#[derive(Debug, Clone, Args)]
struct WorktreeAddArgs {
    #[arg(
        value_name = "REPO_OR_NAME",
        help = "Canonical repository URL/locator, or worktree name when --dir selects a repository"
    )]
    repo_or_name: String,
    #[arg(
        value_name = "NAME_OR_START_POINT",
        help = "Worktree name, or optional start point when --dir selects a repository"
    )]
    name_or_start_point: Option<String>,
    #[arg(
        value_name = "START_POINT",
        help = "Optional Git start point: branch, tag, SHA, remote branch, or commit-ish"
    )]
    start_point: Option<String>,

    #[arg(
        short = 'b',
        long,
        value_name = "BRANCH",
        help = "Create and check out a new branch in the worktree"
    )]
    branch: Option<String>,

    #[arg(long, help = "Create the worktree detached at the start point")]
    detach: bool,

    #[arg(long, help = "Pass --force to git worktree add")]
    force: bool,

    #[arg(
        long,
        help = "After creating the worktree, hard-reset it to START_POINT"
    )]
    reset: bool,
}

#[derive(Debug, Subcommand)]
enum ReposSubcommand {
    #[command(
        name = "set-type",
        about = "Change a managed repository relationship type",
        long_about = "Change a managed repository relationship type.\n\nREPO_PATH is the root-relative path shown as `id` in `repo check --dump`, such as clones/github.com/example/repo. Absolute managed paths are also accepted.\n\nChanging to fork or mirror requires --canonical. The value may be the immediate fork/mirror parent or the ultimate canonical repository: another dumped repo path, a managed locator, or a Git URL/locator. If the parent is itself a fork/mirror, repo-manager records that immediate parent while materializing the new repository against the ultimate canonical bare repository."
    )]
    SetType(RepoSetTypeArgs),
}

#[derive(Debug, Args)]
struct ReposCommand {
    #[command(subcommand)]
    command: ReposSubcommand,
}

#[derive(Debug, Args)]
struct RepoSetTypeArgs {
    #[arg(
        value_name = "REPO_PATH",
        help = "Root-relative or absolute managed repository path"
    )]
    repo_path: PathBuf,

    #[arg(
        value_name = "TYPE",
        help = "Repository type: canonical, fork, or mirror"
    )]
    repo_type: String,

    #[arg(
        long,
        value_name = "CANONICAL",
        help = "Immediate parent or canonical repository path, URL, or locator required for fork/mirror"
    )]
    canonical: Option<String>,
}

#[derive(Debug, Subcommand)]
enum SuccessorSubcommand {
    #[command(
        about = "Record canonical source continuation without treating it as a move",
        long_about = "Record that the canonical source for a project continued at a different repository without treating that change as a rename or transfer.\n\nThis is for cases where the old source was archived, source-closed, deleted, or otherwise ceased to be the source to use. It records metadata only and deliberately does not move paths or create aliases."
    )]
    Set(SuccessorSetArgs),
}

#[derive(Debug, Args)]
struct SuccessorCommand {
    #[command(subcommand)]
    command: SuccessorSubcommand,
}

#[derive(Debug, Args)]
struct SuccessorSetArgs {
    #[arg(
        value_name = "OLD_REF",
        help = "Old source URL or locator that stopped being canonical"
    )]
    old_ref: String,
    #[arg(
        value_name = "NEW_URL",
        help = "New canonical source URL or locator, not a rename target"
    )]
    new_url: String,
}

#[derive(Debug, Subcommand)]
enum AliasesSubcommand {
    #[command(
        about = "List old locator paths/symlinks for a moved repository",
        long_about = "List old locator paths and symlinks for a repository after same-identity moves.\n\nExample: after `github.com/old/repo` moves to `github.com/new/repo`, aliases list shows the old path that points directly to the current real path."
    )]
    List(RepoRef),
}

#[derive(Debug, Args)]
struct AliasesCommand {
    #[command(subcommand)]
    command: AliasesSubcommand,
}

#[derive(Debug, Subcommand)]
enum RelatedSubcommand {
    #[command(about = "List unresolved shared-history suggestions")]
    List,
    #[command(
        about = "Resolve a shared-history suggestion",
        long_about = "Resolve a shared-history suggestion with an explicit relationship.\n\nKinds: mirror, fork, canonical, moved, successor, unrelated.\n\nFor fork and mirror, the first repository shown by `repo related list` is treated as the fork or mirror checkout and the second repository is treated as the canonical checkout. The fork or mirror checkout is converted into a Git worktree of the canonical checkout when possible."
    )]
    Resolve(RelatedResolveArgs),
}

#[derive(Debug, Args)]
struct RelatedCommand {
    #[command(subcommand)]
    command: RelatedSubcommand,
}

#[derive(Debug, Subcommand)]
enum DaemonSubcommand {
    #[command(about = "Check whether repod is reachable")]
    Ping,
}

#[derive(Debug, Args)]
struct DaemonCommand {
    #[command(subcommand)]
    command: DaemonSubcommand,
}

#[derive(Debug, Args)]
struct RelatedResolveArgs {
    #[arg(value_name = "ID", help = "Suggestion ID from `repo related list`")]
    id: i64,

    #[arg(
        value_name = "KIND",
        help = "Relationship kind: mirror, fork, canonical, moved, successor, or unrelated"
    )]
    kind: String,
}

#[derive(Debug, Args)]
struct RepoRef {
    #[arg(
        value_name = "REPO_REF",
        help = "Repository URL, current locator, or historical locator"
    )]
    repo_ref: String,
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    state: PathBuf,
    cache_root: PathBuf,
    root: PathBuf,
    clone_root: PathBuf,
    dev_worktree_root: PathBuf,
    rpc_url: String,
    client_id: String,
    assume_origin_as_canonical: bool,
    clone_as_bare: bool,
    create_default_visibility: RepoVisibility,
    forges: HashMap<String, ForgeConfig>,
}

#[derive(Debug, Clone)]
struct DaemonConfig {
    state: PathBuf,
    detect_related: bool,
    clone_start_ttl_minutes: u64,
    rpc_rate_limit_per_second: u32,
    background_fetch_minimum_interval_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct Output {
    json: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct FileConfig {
    #[serde(alias = "config-version")]
    config_version: Option<u32>,
    state: Option<PathBuf>,
    cache_root: Option<PathBuf>,
    root: Option<PathBuf>,
    rpc_url: Option<String>,
    client_id: Option<String>,
    assume_origin_as_canonical: Option<bool>,
    #[serde(alias = "clone-as-bare")]
    clone_as_bare: Option<bool>,
    detect_related: Option<bool>,
    clone_start_ttl_minutes: Option<u64>,
    rpc_rate_limit_per_second: Option<u32>,
    #[serde(alias = "background-fetch-minimum-interval-seconds")]
    background_fetch_minimum_interval_seconds: Option<u64>,
    #[serde(alias = "create-default-visibility")]
    create_default_visibility: Option<RepoVisibility>,
    forges: Option<HashMap<String, ForgeConfig>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum RepoVisibility {
    #[default]
    Private,
    Public,
}

impl RepoVisibility {
    fn is_private(self) -> bool {
        matches!(self, Self::Private)
    }

    fn sourcehut(self) -> &'static str {
        match self {
            Self::Private => "PRIVATE",
            Self::Public => "PUBLIC",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ForgeConfig {
    backend: ForgeBackend,
    #[serde(alias = "token-env")]
    token_env: Option<String>,
    #[serde(alias = "api-base-url")]
    api_base_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ForgeBackend {
    Github,
    Sourcehut,
    Forgejo,
}

#[derive(Debug, Serialize)]
struct ReconcileReport {
    action: &'static str,
    planned_moves: Vec<ReconcileMove>,
    skipped: Vec<ReconcileSkip>,
}

#[derive(Debug, Serialize)]
struct ReconcileMove {
    repo_id: i64,
    repo_path: PathBuf,
    evidence: String,
    plan: MovePlan,
}

#[derive(Debug, Serialize)]
struct ReconcileSkip {
    repo_id: i64,
    repo_path: PathBuf,
    reason: String,
}

#[derive(Debug, Serialize)]
struct RepairReport {
    action: &'static str,
    check: bool,
    repository_formats: Vec<RepairRepositoryFormat>,
    stale_paths: Vec<RepairStalePath>,
    untracked_checkouts: Vec<RepairUntrackedCheckout>,
    relationships: Vec<RepairRelationship>,
    skipped: Vec<RepairSkip>,
}

#[derive(Debug, Serialize)]
struct CheckDump {
    action: &'static str,
    generated_at_epoch_seconds: i64,
    roots: CheckDumpRoots,
    tracked_repositories: Vec<TrackedRepositoryDump>,
    git_directories: Vec<GitDirectoryDump>,
}

#[derive(Debug, Serialize)]
struct CheckDumpRoots {
    clone_root: PathBuf,
    dev_worktree_root: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct ManagedRepositoryDump {
    id: String,
    #[serde(skip)]
    db_id: i64,
    #[serde(rename = "type")]
    repo_type: String,
    fork_depth: usize,
    locator: Locator,
    path: PathBuf,
    checkout_kind: String,
    parent: Option<RepositoryRelationEndpoint>,
    canonical: Option<RepositoryRelationEndpoint>,
    dependents: Vec<RepositoryRelationEndpoint>,
}

#[derive(Debug, Clone, Serialize)]
struct RepositoryRelationEndpoint {
    id: String,
    locator: Locator,
    path: PathBuf,
    relationship: String,
}

#[derive(Debug, Serialize)]
struct RepoTypeChangeResult {
    action: &'static str,
    id: String,
    previous_type: String,
    new_type: String,
    repository: ManagedRepositoryDump,
    shared_git_dir: Option<SharedGitDirResolution>,
}

#[derive(Debug, Serialize)]
struct TrackedRepositoryDump {
    id: String,
    #[serde(rename = "type")]
    repo_type: String,
    fork_depth: usize,
    locator: Locator,
    path: PathBuf,
    exists: bool,
    checkout_kind: String,
    parent: Option<RepositoryRelationEndpoint>,
    canonical: Option<RepositoryRelationEndpoint>,
    dependents: Vec<RepositoryRelationEndpoint>,
    background_fetch: Option<BackgroundFetchState>,
}

#[derive(Debug, Clone, Serialize)]
struct BackgroundFetchState {
    last_fetch_at: Option<i64>,
    last_changed_at: Option<i64>,
    learned_interval_seconds: i64,
    last_status: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct GitDirectoryDump {
    id: Option<String>,
    path: PathBuf,
    kind: Option<String>,
    tracked: bool,
    managed: bool,
    worktree_name: Option<String>,
    repository: Option<String>,
    repository_type: Option<String>,
    namespace: Option<String>,
    locator: Option<Locator>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct GitDirectoryRepositoryLink {
    id: String,
    repo_type: String,
    locator: Locator,
    namespace: Option<String>,
}

#[derive(Debug, Clone)]
struct GitDirectoryLink {
    id: Option<String>,
    tracked: bool,
    managed: bool,
    worktree_name: Option<String>,
    repository: Option<GitDirectoryRepositoryLink>,
}

#[derive(Debug, Clone)]
struct ManagedWorktreeRecord {
    repo_id: i64,
    repo_locator: Locator,
    repo_type: String,
    path: PathBuf,
    name: Option<String>,
    refs_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RepairStalePath {
    repo_id: i64,
    locator: Locator,
    path: PathBuf,
    status: RepairStalePathStatus,
    reasons: Vec<String>,
    #[serde(rename = "blocking_checkouts")]
    blocking_dependents: Vec<RepairStaleDependent>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
enum RepairStalePathStatus {
    NeedsPrune,
    Pruned,
    Blocked,
}

#[derive(Debug, Clone, Serialize)]
struct RepairStaleDependent {
    relationship: String,
    locator: Locator,
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct RepairUntrackedCheckout {
    locator: Option<Locator>,
    path: PathBuf,
    status: RepairUntrackedCheckoutStatus,
    reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RepairRepositoryFormat {
    repo_id: i64,
    locator: Locator,
    path: PathBuf,
    status: RepairRepositoryFormatStatus,
    reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
enum RepairRepositoryFormatStatus {
    NeedsBareConversion,
    ConvertedToBare,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum RepairUntrackedCheckoutStatus {
    NeedsTracking,
    Tracked,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
struct RepairRelationship {
    relationship: String,
    #[serde(rename = "checkout_locator")]
    dependent_locator: Locator,
    #[serde(rename = "canonical_locator")]
    controlling_locator: Locator,
    #[serde(rename = "checkout_path")]
    dependent_path: PathBuf,
    #[serde(rename = "canonical_path")]
    controlling_path: PathBuf,
    status: RepairStatus,
    reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_git_dir: Option<SharedGitDirResolution>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum RepairStatus {
    Ok,
    NeedsRepair,
    Repaired,
}

#[derive(Debug, Clone, Serialize)]
struct RepairSkip {
    relationship: String,
    #[serde(rename = "checkout_locator")]
    dependent_locator: Locator,
    #[serde(rename = "canonical_locator")]
    controlling_locator: Locator,
    reason: String,
}

#[derive(Debug, Clone)]
enum RpcEvent {
    Started(CloneStartedEvent),
    Finished(CloneFinishedEvent),
    Cancelled(CloneCancelledEvent),
    ManageRequested(ManageRequestedEvent),
}

#[derive(Debug, Clone)]
struct CloneStartedEvent {
    client_id: String,
    url: String,
    locator: Locator,
    path: PathBuf,
    scan_root: PathBuf,
}

#[derive(Debug, Clone)]
struct CloneFinishedEvent {
    client_id: String,
    url: String,
    locator: Locator,
    path: PathBuf,
    success: bool,
    scan_root: PathBuf,
}

#[derive(Debug, Clone)]
struct CloneCancelledEvent {
    client_id: String,
    url: String,
    locator: Locator,
    path: PathBuf,
    reason: String,
    scan_root: PathBuf,
}

#[derive(Debug, Clone)]
struct ManageRequestedEvent {
    client_id: String,
    url: String,
    locator: Locator,
    path: PathBuf,
    scan_root: PathBuf,
}

impl RpcEvent {
    fn to_proto(&self) -> api::CloneEvent {
        use api::clone_event::Event;

        let event = match self {
            Self::Started(event) => Event::Started(api::CloneStarted {
                client_id: event.client_id.clone(),
                url: event.url.clone(),
                locator: Some(locator_to_proto(&event.locator)),
                path: event.path.display().to_string(),
                scan_root: event.scan_root.display().to_string(),
            }),
            Self::Finished(event) => Event::Finished(api::CloneFinished {
                client_id: event.client_id.clone(),
                url: event.url.clone(),
                locator: Some(locator_to_proto(&event.locator)),
                path: event.path.display().to_string(),
                success: event.success,
                scan_root: event.scan_root.display().to_string(),
            }),
            Self::Cancelled(event) => Event::Cancelled(api::CloneCancelled {
                client_id: event.client_id.clone(),
                url: event.url.clone(),
                locator: Some(locator_to_proto(&event.locator)),
                path: event.path.display().to_string(),
                reason: event.reason.clone(),
                scan_root: event.scan_root.display().to_string(),
            }),
            Self::ManageRequested(event) => Event::ManageRequested(api::ManageRequested {
                client_id: event.client_id.clone(),
                url: event.url.clone(),
                locator: Some(locator_to_proto(&event.locator)),
                path: event.path.display().to_string(),
                scan_root: event.scan_root.display().to_string(),
            }),
        };

        api::CloneEvent {
            protocol_version: RPC_PROTOCOL_VERSION,
            event: Some(event),
        }
    }

    fn from_proto(message: api::CloneEvent) -> Result<Self> {
        use api::clone_event::Event;

        validate_rpc_protocol_version(message.protocol_version)?;

        match message
            .event
            .context("RPC clone event is missing event payload")?
        {
            Event::Started(event) => Ok(Self::Started(CloneStartedEvent {
                client_id: required_proto_string("client_id", event.client_id)?,
                url: required_proto_string("url", event.url)?,
                locator: locator_from_proto(event.locator)?,
                path: required_proto_path("path", event.path)?,
                scan_root: required_proto_path("scan_root", event.scan_root)?,
            })),
            Event::Finished(event) => Ok(Self::Finished(CloneFinishedEvent {
                client_id: required_proto_string("client_id", event.client_id)?,
                url: required_proto_string("url", event.url)?,
                locator: locator_from_proto(event.locator)?,
                path: required_proto_path("path", event.path)?,
                success: event.success,
                scan_root: required_proto_path("scan_root", event.scan_root)?,
            })),
            Event::Cancelled(event) => Ok(Self::Cancelled(CloneCancelledEvent {
                client_id: required_proto_string("client_id", event.client_id)?,
                url: required_proto_string("url", event.url)?,
                locator: locator_from_proto(event.locator)?,
                path: required_proto_path("path", event.path)?,
                reason: required_proto_string("reason", event.reason)?,
                scan_root: required_proto_path("scan_root", event.scan_root)?,
            })),
            Event::ManageRequested(event) => Ok(Self::ManageRequested(ManageRequestedEvent {
                client_id: required_proto_string("client_id", event.client_id)?,
                url: required_proto_string("url", event.url)?,
                locator: locator_from_proto(event.locator)?,
                path: required_proto_path("path", event.path)?,
                scan_root: required_proto_path("scan_root", event.scan_root)?,
            })),
        }
    }

    fn client_id(&self) -> &str {
        match self {
            Self::Started(event) => &event.client_id,
            Self::Finished(event) => &event.client_id,
            Self::Cancelled(event) => &event.client_id,
            Self::ManageRequested(event) => &event.client_id,
        }
    }

    fn event_name(&self) -> &'static str {
        match self {
            Self::Started(_) => "clone_started",
            Self::Finished(_) => "clone_finished",
            Self::Cancelled(_) => "clone_cancelled",
            Self::ManageRequested(_) => "manage_requested",
        }
    }
}

fn locator_to_proto(locator: &Locator) -> api::Locator {
    api::Locator {
        authority: locator.authority.clone(),
        remote_path: locator.remote_path.clone(),
    }
}

fn locator_from_proto(locator: Option<api::Locator>) -> Result<Locator> {
    let locator = locator.context("RPC clone event is missing locator")?;
    Locator::new(locator.authority, locator.remote_path)
}

fn validate_rpc_protocol_version(client_version: u32) -> Result<()> {
    if client_version != RPC_PROTOCOL_VERSION {
        bail!(
            "RPC protocol version mismatch: daemon supports v{}, client sent v{}",
            RPC_PROTOCOL_VERSION,
            client_version
        );
    }
    Ok(())
}

fn required_proto_path(field: &str, value: String) -> Result<PathBuf> {
    Ok(PathBuf::from(required_proto_string(field, value)?))
}

fn required_proto_string(field: &str, value: String) -> Result<String> {
    if value.is_empty() {
        bail!("RPC clone event is missing required field `{field}`");
    }
    Ok(value)
}

#[derive(Debug, Clone, Serialize)]
struct CloneResult {
    action: &'static str,
    locator: Locator,
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct CreateResult {
    action: &'static str,
    locator: Locator,
    path: PathBuf,
    backend: ForgeBackend,
    visibility: RepoVisibility,
    clone: CloneResult,
}

#[derive(Debug, Clone, Serialize)]
struct ManageResult {
    action: &'static str,
    locator: Locator,
    canonical_url: String,
    relationship: &'static str,
    path: PathBuf,
    moved_from: Option<PathBuf>,
    history_review_requested: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ForkResult {
    action: &'static str,
    fork_locator: Locator,
    parent_locator: Locator,
    parent_path: PathBuf,
    canonical_locator: Locator,
    fork_path: PathBuf,
    canonical_path: PathBuf,
    fork_remote: String,
    refs_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RepoViewMetadata {
    version: u32,
    relationship: String,
    locator: Locator,
    path: PathBuf,
    canonical_locator: Locator,
    canonical_path: PathBuf,
    refs_prefix: String,
    origin_url: String,
    default_branch: String,
}

#[derive(Debug, Clone)]
enum RepoContext {
    Managed(ManagedRepoRecord),
    View(RepoViewMetadata),
}

#[derive(Debug, Clone, Serialize)]
struct FetchResult {
    action: &'static str,
    locator: Locator,
    path: PathBuf,
    remote: String,
    refs_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BranchResult {
    action: &'static str,
    locator: Locator,
    path: PathBuf,
    refs_prefix: Option<String>,
    branches: Vec<String>,
    created: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SetupResult {
    action: &'static str,
    config_path: PathBuf,
    config: FileConfig,
    note: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct DaemonPingResult {
    action: &'static str,
    rpc_url: String,
    reachable: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SuccessorResult {
    action: &'static str,
    old_ref: String,
    new_locator: Locator,
    new_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct RelatedSuggestion {
    id: i64,
    repo_id: i64,
    repo_locator: Locator,
    repo_path: PathBuf,
    related_repo_id: i64,
    related_locator: Locator,
    related_path: PathBuf,
    shared_refs: Vec<String>,
    resolution: Option<String>,
}

#[derive(Debug, Clone)]
struct SharedGitDirRelationship {
    relationship: String,
    dependent_repo_id: i64,
    dependent_locator: Locator,
    dependent_path: PathBuf,
    controlling_repo_id: i64,
    controlling_locator: Locator,
    controlling_path: PathBuf,
}

#[derive(Debug, Clone)]
struct RepositoryRelationshipSummary {
    relationship: String,
    parent_locator: Locator,
    parent_path: PathBuf,
    canonical_repo_id: i64,
    canonical_locator: Locator,
    canonical_path: PathBuf,
    depth: usize,
}

#[derive(Debug, Clone, Serialize)]
struct RelatedListReport {
    action: &'static str,
    unresolved_count: usize,
    suggestions: Vec<RelatedSuggestionReport>,
}

#[derive(Debug, Clone, Serialize)]
struct RelatedSuggestionReport {
    id: i64,
    repositories: [RelatedRepositoryReport; 2],
    evidence: RelatedEvidenceReport,
    resolution: Option<String>,
    resolve_command: String,
}

#[derive(Debug, Clone, Serialize)]
struct RelatedRepositoryReport {
    repo_id: i64,
    locator: Locator,
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct RelatedEvidenceReport {
    summary: String,
    details: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RelatedResolution {
    action: &'static str,
    id: i64,
    resolution: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_git_dir: Option<SharedGitDirResolution>,
}

#[derive(Debug, Clone, Serialize)]
struct SharedGitDirResolution {
    #[serde(rename = "checkout_locator")]
    dependent_locator: Locator,
    #[serde(rename = "canonical_locator")]
    controlling_locator: Locator,
    #[serde(rename = "checkout_path")]
    dependent_path: PathBuf,
    #[serde(rename = "canonical_path")]
    controlling_path: PathBuf,
    #[serde(rename = "relationship_remote")]
    dependent_remote: String,
    #[serde(rename = "relationship_url")]
    dependent_url: String,
    local_branch: String,
    remote_branch: String,
    converted_to_worktree: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Locator {
    pub authority: String,
    pub remote_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChangeFlags {
    pub authority_changed: bool,
    pub remote_path_changed: bool,
    pub path_prefix_changed: bool,
    pub leaf_name_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MovePlan {
    pub old_locator: Locator,
    pub new_locator: Locator,
    pub old_path: PathBuf,
    pub new_path: PathBuf,
    pub flags: ChangeFlags,
    pub aliases: Vec<AliasPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AliasPlan {
    pub alias_path: PathBuf,
    pub target_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorktreePlan {
    pub canonical_locator: Locator,
    pub canonical_path: PathBuf,
    pub worktree_path: PathBuf,
    pub git_args: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WorktreeAddOptions<'a> {
    pub start_point: Option<&'a str>,
    pub branch: Option<&'a str>,
    pub detach: bool,
    pub force: bool,
}

pub struct HelpCommand {
    pub name: &'static str,
    pub about: &'static str,
}

pub struct HelpCommandGroup {
    pub heading: &'static str,
    pub commands: Vec<HelpCommand>,
}

pub trait HelpGroup {
    fn help_group() -> HelpCommandGroup;
}

pub trait HelpTemplate {
    fn help_template() -> String;
}

pub fn render_help_template(groups: Vec<HelpCommandGroup>) -> String {
    let mut template = "{about-with-newline}\n{usage-heading} {usage}\n\n".to_string();
    template.push_str(&styled_heading("Command groups:"));
    template.push('\n');

    for group in groups {
        let command_name_width = group
            .commands
            .iter()
            .map(|command| command.name.len())
            .max()
            .unwrap_or(0);
        template.push_str("  ");
        template.push_str(&styled_heading(&format!("{}:", group.heading)));
        template.push('\n');
        for command in group.commands {
            let padded_name = format!("{:<width$}", command.name, width = command_name_width);
            template.push_str("    ");
            template.push_str(&styled_command(&padded_name));
            template.push_str("  ");
            template.push_str(command.about);
            template.push('\n');
        }
        template.push('\n');
    }

    template.push_str(&styled_heading("Options:"));
    template.push('\n');
    template.push_str("{options}");
    template
}

fn styled_heading(text: &str) -> String {
    styled(anstyle::Style::new().bold().underline(), text)
}

fn styled_command(text: &str) -> String {
    styled(anstyle::Style::new().bold(), text)
}

fn styled(style: anstyle::Style, text: &str) -> String {
    format!("{style}{text}{}", style.render_reset())
}

pub fn run() -> Result<()> {
    let _ = env_logger::try_init();
    let cli = parse_cli();
    let config = Config::from_cli(&cli)?;
    let output = Output { json: cli.json };

    match cli.command {
        Commands::Setup(command) => match command {
            SetupCommands::Setup(args) => setup_config(&config, &output, args),
        },
        Commands::RepositoryOperations(command) => match command {
            RepositoryOperationCommands::Clone(args) => {
                let db = Store::open(&config.state)?;
                clone_repo(&config, &db, &output, &args.url)
            }
            RepositoryOperationCommands::Create(args) => {
                let db = Store::open(&config.state)?;
                create_repo(&config, &db, &output, args)
            }
            RepositoryOperationCommands::Fetch(args) => {
                let db = Store::open(&config.state)?;
                fetch_repo(&config, &db, &output, cli.dir.as_deref(), args)
            }
            RepositoryOperationCommands::Branch(args) => {
                let db = Store::open(&config.state)?;
                branch_repo(&config, &db, &output, cli.dir.as_deref(), args)
            }
            RepositoryOperationCommands::Manage(args) => {
                let db = Store::open(&config.state)?;
                manage_repo(&config, &db, &output, args)
            }
            RepositoryOperationCommands::Fork(args) => {
                let db = Store::open(&config.state)?;
                fork_repo(&config, &db, &output, &args.fork_url, &args.canonical)
            }
            RepositoryOperationCommands::Check(args) => {
                let db = Store::open(&config.state)?;
                if args.dump {
                    if args.repair {
                        bail!(
                            "repo check --dump is read-only and cannot be combined with --repair"
                        );
                    }
                    dump_check(&config, &db)
                } else if args.repair && !output.json {
                    repair_repos_interactive(&config, &db, &output)
                } else {
                    repair_repos(&config, &db, &output, !args.repair)
                }
            }
            RepositoryOperationCommands::Repair(args) => {
                let db = Store::open(&config.state)?;
                repair_repos(&config, &db, &output, args.check)
            }
            RepositoryOperationCommands::Worktree(command) => match command.command {
                WorktreeSubcommand::Add(args) => {
                    let db = Store::open(&config.state)?;
                    add_worktree(&config, &db, &output, cli.dir.as_deref(), args)
                }
            },
            RepositoryOperationCommands::Repos(command) => {
                let db = Store::open(&config.state)?;
                match command.command {
                    ReposSubcommand::SetType(args) => repos_set_type(&config, &db, &output, args),
                }
            }
        },
        Commands::OrganizationalChanges(command) => match command {
            OrganizationalChangeCommands::Move(args) => {
                let db = Store::open(&config.state)?;
                move_repo(&config, &db, &output, &args.repo_ref, &args.new_url)
            }
            OrganizationalChangeCommands::Reconcile => {
                let db = Store::open(&config.state)?;
                reconcile(&config, &db, &output)
            }
            OrganizationalChangeCommands::Successor(command) => match command.command {
                SuccessorSubcommand::Set(args) => {
                    let db = Store::open(&config.state)?;
                    successor_set(&config, &db, &output, &args.old_ref, &args.new_url)
                }
            },
        },
        Commands::OrganizationalAnalysis(command) => match command {
            OrganizationalAnalysisCommands::Aliases(command) => match command.command {
                AliasesSubcommand::List(args) => {
                    let db = Store::open(&config.state)?;
                    warn_pending_related(&db)?;
                    aliases_list(&db, &output, &args.repo_ref)
                }
            },
            OrganizationalAnalysisCommands::Related(command) => {
                let db = Store::open(&config.state)?;
                match command.command {
                    RelatedSubcommand::List => related_list(&db, &output),
                    RelatedSubcommand::Resolve(args) => {
                        related_resolve(&config, &db, &output, args.id, &args.kind)
                    }
                }
            }
        },
        Commands::Daemon(command) => match command {
            DaemonCommands::Daemon(command) => match command.command {
                DaemonSubcommand::Ping => daemon_ping(&config, &output),
            },
        },
    }
}

pub fn run_repod() -> Result<()> {
    let _ = env_logger::try_init();
    reject_sudo_repod()?;
    let cli = RepodCli::parse();
    let (config, rpc_url) = DaemonConfig::from_args(&cli.config)?;
    run_daemon(&config, &rpc_url, cli.daemon)
}

fn reject_sudo_repod() -> Result<()> {
    if env::var_os("SUDO_USER").is_some() {
        bail!(
            "repod is a user daemon; run it without sudo so it uses the same config, state DB, and notification bus as repo"
        );
    }
    Ok(())
}

fn parse_cli() -> Cli {
    let matches = Cli::command()
        .help_template(<Commands as HelpTemplate>::help_template())
        .get_matches();
    Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit())
}

impl Config {
    fn from_cli(cli: &Cli) -> Result<Self> {
        Self::from_args(&cli.config)
    }

    fn from_args(args: &ConfigArgs) -> Result<Self> {
        let (config_path, file_config) = match &args.config {
            Some(config_path) => (config_path.clone(), FileConfig::load(config_path)?),
            None => {
                let config_path = default_config_path()?;
                let file_config = FileConfig::load_xdg_layered(&config_path)?;
                (config_path, file_config)
            }
        };
        let state = args
            .state
            .clone()
            .or(file_config.state)
            .unwrap_or(default_state_path()?);
        let cache_root = args
            .cache_root
            .clone()
            .or(file_config.cache_root)
            .unwrap_or(default_cache_root()?);
        let root = args
            .root
            .clone()
            .or(file_config.root)
            .unwrap_or(default_root()?);
        let clone_root = clone_root_for(&root);
        let dev_worktree_root = dev_worktree_root_for(&root);
        let rpc_url = args
            .rpc_url
            .clone()
            .or(file_config.rpc_url)
            .unwrap_or_else(default_rpc_url);
        let client_id = args
            .client_id
            .clone()
            .or(file_config.client_id)
            .map_or_else(generate_client_id, validate_client_id)?;
        let assume_origin_as_canonical = args
            .assume_origin_as_canonical
            .or(file_config.assume_origin_as_canonical)
            .unwrap_or(false);
        let clone_as_bare = file_config.clone_as_bare.unwrap_or(false);
        let create_default_visibility = file_config.create_default_visibility.unwrap_or_default();
        let forges = file_config.forges.unwrap_or_default();
        Ok(Self {
            config_path,
            state,
            cache_root,
            root,
            clone_root,
            dev_worktree_root,
            rpc_url,
            client_id,
            assume_origin_as_canonical,
            clone_as_bare,
            create_default_visibility,
            forges,
        })
    }
}

impl FileConfig {
    fn load_xdg_layered(user_config_path: &Path) -> Result<Self> {
        let mut config = Self::default();
        for path in xdg_config_dir_paths() {
            config.merge(Self::load(&path)?);
        }
        config.merge(Self::load(user_config_path)?);
        Ok(config)
    }

    fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content =
            fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let json: serde_json::Value = serde_json::from_str(&content)
            .with_context(|| format!("parsing {}", path.display()))?;
        validate_config_json(&json)
            .map_err(|error| anyhow!("validating {}: {error}", path.display()))?;
        serde_json::from_value(json).with_context(|| format!("parsing {}", path.display()))
    }

    fn merge(&mut self, other: Self) {
        self.config_version = other.config_version.or(self.config_version);
        self.state = other.state.or_else(|| self.state.take());
        self.cache_root = other.cache_root.or_else(|| self.cache_root.take());
        self.root = other.root.or_else(|| self.root.take());
        self.rpc_url = other.rpc_url.or_else(|| self.rpc_url.take());
        self.client_id = other.client_id.or_else(|| self.client_id.take());
        self.assume_origin_as_canonical = other
            .assume_origin_as_canonical
            .or(self.assume_origin_as_canonical);
        self.clone_as_bare = other.clone_as_bare.or(self.clone_as_bare);
        self.detect_related = other.detect_related.or(self.detect_related);
        self.clone_start_ttl_minutes = other
            .clone_start_ttl_minutes
            .or(self.clone_start_ttl_minutes);
        self.rpc_rate_limit_per_second = other
            .rpc_rate_limit_per_second
            .or(self.rpc_rate_limit_per_second);
        self.background_fetch_minimum_interval_seconds = other
            .background_fetch_minimum_interval_seconds
            .or(self.background_fetch_minimum_interval_seconds);
        self.create_default_visibility = other
            .create_default_visibility
            .or(self.create_default_visibility);
        if let Some(other_forges) = other.forges {
            let forges = self.forges.get_or_insert_with(HashMap::new);
            forges.extend(other_forges);
        }
    }

    fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating config directory {}", parent.display()))?;
        }
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, format!("{content}\n"))
            .with_context(|| format!("writing {}", path.display()))
    }
}

fn validate_config_json(config: &serde_json::Value) -> Result<()> {
    let version = config
        .get("config_version")
        .or_else(|| config.get("config-version"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(u64::from(CURRENT_CONFIG_VERSION));
    let schema = match version {
        1 => CONFIG_SCHEMA_V1,
        unsupported => {
            bail!("unsupported repo-manager config version {unsupported}; supported versions: 1")
        }
    };
    let schema: serde_json::Value =
        serde_json::from_str(schema).context("parsing repo-manager config JSON Schema")?;
    let validator =
        jsonschema::validator_for(&schema).context("compiling repo-manager config JSON Schema")?;
    let errors = validator
        .iter_errors(config)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        bail!(
            "repo-manager config does not match schema v{version}: {}",
            errors.join("; ")
        )
    }
}

impl DaemonConfig {
    fn from_args(args: &DaemonConfigArgs) -> Result<(Self, String)> {
        let (_config_path, file_config) = match &args.config {
            Some(config_path) => (config_path.clone(), FileConfig::load(config_path)?),
            None => {
                let config_path = default_config_path()?;
                let file_config = FileConfig::load_xdg_layered(&config_path)?;
                (config_path, file_config)
            }
        };
        let state = args
            .state
            .clone()
            .or(file_config.state)
            .unwrap_or(default_state_path()?);
        let rpc_url = args
            .rpc_url
            .clone()
            .or(file_config.rpc_url)
            .unwrap_or_else(default_rpc_url);
        let detect_related = args
            .detect_related
            .or(file_config.detect_related)
            .unwrap_or(DEFAULT_DETECT_RELATED);
        let clone_start_ttl_minutes = args
            .clone_start_ttl_minutes
            .or(file_config.clone_start_ttl_minutes)
            .unwrap_or(60);
        let rpc_rate_limit_per_second = args
            .rpc_rate_limit_per_second
            .or(file_config.rpc_rate_limit_per_second)
            .unwrap_or(1);
        let background_fetch_minimum_interval_seconds = args
            .background_fetch_minimum_interval_seconds
            .or(file_config.background_fetch_minimum_interval_seconds);

        Ok((
            Self {
                state,
                detect_related,
                clone_start_ttl_minutes,
                rpc_rate_limit_per_second,
                background_fetch_minimum_interval_seconds,
            },
            rpc_url,
        ))
    }
}

fn setup_config(config: &Config, output: &Output, args: SetupArgs) -> Result<()> {
    let config_path = args.file.unwrap_or_else(|| config.config_path.clone());
    let file_config = FileConfig {
        config_version: Some(CURRENT_CONFIG_VERSION),
        state: Some(args.state.unwrap_or_else(|| config.state.clone())),
        cache_root: Some(args.cache_root.unwrap_or_else(|| config.cache_root.clone())),
        root: Some(args.root.unwrap_or_else(|| config.root.clone())),
        rpc_url: Some(args.rpc_url.unwrap_or_else(|| config.rpc_url.clone())),
        client_id: Some(args.client_id.unwrap_or_else(|| config.client_id.clone())),
        assume_origin_as_canonical: args
            .assume_origin_as_canonical
            .or(Some(config.assume_origin_as_canonical)),
        clone_as_bare: Some(config.clone_as_bare),
        detect_related: None,
        clone_start_ttl_minutes: None,
        rpc_rate_limit_per_second: None,
        background_fetch_minimum_interval_seconds: None,
        create_default_visibility: Some(config.create_default_visibility),
        forges: (!config.forges.is_empty()).then(|| config.forges.clone()),
    };
    file_config.save(&config_path)?;
    let result = SetupResult {
        action: "setup",
        config_path,
        config: file_config,
        note: "Environment variables and top-level CLI options override these persisted values at runtime.",
    };
    output_setup(output, &result)
}

fn home_dir() -> Result<PathBuf> {
    Ok(base_dirs()?.home_dir().to_path_buf())
}

fn base_dirs() -> Result<BaseDirs> {
    BaseDirs::new().ok_or_else(|| anyhow!("could not determine XDG base directories"))
}

fn default_config_path() -> Result<PathBuf> {
    Ok(base_dirs()?.config_dir().join("repo-manager/config.json"))
}

fn xdg_config_dir_paths() -> Vec<PathBuf> {
    let dirs = env::var_os("XDG_CONFIG_DIRS")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "/etc/xdg".into());
    env::split_paths(&dirs)
        .map(|path| path.join("repo-manager/config.json"))
        .collect()
}

fn default_state_path() -> Result<PathBuf> {
    let base_dirs = base_dirs()?;
    let state_dir = base_dirs
        .state_dir()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| base_dirs.home_dir().join(".local/state"));
    Ok(state_dir.join("repo-manager/repos.sqlite"))
}

fn default_cache_root() -> Result<PathBuf> {
    Ok(base_dirs()?.cache_dir().join("repo-manager"))
}

fn default_root() -> Result<PathBuf> {
    Ok(home_dir()?.join("code"))
}

fn clone_root_for(root: &Path) -> PathBuf {
    root.join("clones")
}

fn dev_worktree_root_for(root: &Path) -> PathBuf {
    root.join("dev-worktrees")
}

fn default_rpc_url() -> String {
    let socket_path = env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let user = env::var("USER").unwrap_or_else(|_| "unknown".to_string());
            env::temp_dir().join(format!("repo-manager-{user}"))
        })
        .join("repo-manager/socket");
    format!("unix://{}", socket_path.display())
}

fn generate_client_id() -> Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).context("generating client UUID")?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format_uuid(bytes))
}

fn validate_client_id(value: String) -> Result<String> {
    if is_uuid_like(&value) {
        Ok(value)
    } else {
        bail!("client ID must be a UUID: {value}")
    }
}

fn is_uuid_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (idx, byte) in bytes.iter().enumerate() {
        match idx {
            8 | 13 | 18 | 23 => {
                if *byte != b'-' {
                    return false;
                }
            }
            _ => {
                if !byte.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

fn format_uuid(bytes: [u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

impl Locator {
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            bail!("repository locator is empty");
        }

        if input.contains("://") {
            return Self::parse_url(input);
        }

        if let Some((authority, remote_path)) = parse_scp_like(input) {
            return Self::new(authority, remote_path);
        }

        let (authority, remote_path) = input
            .split_once('/')
            .ok_or_else(|| anyhow!("expected URL, scp-style URL, or <authority>/<remote-path>"))?;
        Self::new(authority, remote_path)
    }

    fn parse_url(input: &str) -> Result<Self> {
        let url = Url::parse(input).with_context(|| format!("invalid Git URL: {input}"))?;
        let host = url
            .host_str()
            .or_else(|| (url.scheme() == "file").then_some("localhost"))
            .ok_or_else(|| anyhow!("URL does not include an authority: {input}"))?;
        let authority = match url.port() {
            Some(port) => format!("{}:{port}", host.to_ascii_lowercase()),
            None => host.to_ascii_lowercase(),
        };
        Self::new(authority, url.path())
    }

    fn new(authority: impl Into<String>, remote_path: impl AsRef<str>) -> Result<Self> {
        let authority = authority
            .into()
            .trim()
            .trim_end_matches('/')
            .to_ascii_lowercase();
        if authority.is_empty() || authority.contains('/') {
            bail!("invalid authority: {authority:?}");
        }

        let mut remote_path = remote_path.as_ref().trim().trim_matches('/').to_string();
        while remote_path.ends_with('/') {
            remote_path.pop();
        }
        if let Some(stripped) = remote_path.strip_suffix(".git") {
            remote_path = stripped.to_string();
        }
        validate_remote_path(&remote_path)?;

        Ok(Self {
            authority,
            remote_path,
        })
    }

    pub fn key(&self) -> String {
        format!("{}/{}", self.authority, self.remote_path)
    }
}

fn parse_scp_like(input: &str) -> Option<(&str, &str)> {
    if input.contains("://") {
        return None;
    }
    let (left, right) = input.split_once(':')?;
    if left.is_empty() || right.is_empty() || left.contains('/') {
        return None;
    }
    let authority = left.rsplit_once('@').map_or(left, |(_, host)| host);
    Some((authority, right))
}

fn validate_remote_path(remote_path: &str) -> Result<()> {
    if remote_path.is_empty() {
        bail!("remote path is empty");
    }
    for component in remote_path.split('/') {
        match component {
            "" | "." | ".." => bail!("remote path contains unsafe component: {remote_path}"),
            _ => {}
        }
    }
    Ok(())
}

pub fn locator_path(root: &Path, locator: &Locator) -> PathBuf {
    locator
        .remote_path
        .split('/')
        .fold(root.join(&locator.authority), |path, part| path.join(part))
}

pub fn plan_move(
    clone_root: &Path,
    old_locator: Locator,
    new_locator: Locator,
    historical_locators: &[Locator],
) -> MovePlan {
    let old_path = locator_path(clone_root, &old_locator);
    let new_path = locator_path(clone_root, &new_locator);
    let flags = ChangeFlags {
        authority_changed: old_locator.authority != new_locator.authority,
        remote_path_changed: old_locator.remote_path != new_locator.remote_path,
        path_prefix_changed: path_prefix(&old_locator.remote_path)
            != path_prefix(&new_locator.remote_path),
        leaf_name_changed: path_leaf(&old_locator.remote_path)
            != path_leaf(&new_locator.remote_path),
    };

    let mut seen = BTreeSet::new();
    let aliases = historical_locators
        .iter()
        .chain(std::iter::once(&old_locator))
        .map(|locator| locator_path(clone_root, locator))
        .filter(|path| path != &new_path)
        .filter(|path| seen.insert(path.clone()))
        .map(|alias_path| AliasPlan {
            alias_path,
            target_path: new_path.clone(),
        })
        .collect();

    MovePlan {
        old_locator,
        new_locator,
        old_path,
        new_path,
        flags,
        aliases,
    }
}

fn path_prefix(remote_path: &str) -> String {
    remote_path
        .rsplit_once('/')
        .map_or(String::new(), |(prefix, _)| prefix.to_string())
}

fn path_leaf(remote_path: &str) -> String {
    remote_path
        .rsplit_once('/')
        .map_or(remote_path.to_string(), |(_, leaf)| leaf.to_string())
}

pub fn plan_worktree_add(
    clone_root: &Path,
    worktree_root: &Path,
    canonical_locator: Locator,
    name: &str,
    options: WorktreeAddOptions<'_>,
) -> Result<WorktreePlan> {
    validate_worktree_name(name)?;
    let canonical_path = locator_path(clone_root, &canonical_locator);
    let worktree_path = locator_path(worktree_root, &canonical_locator).join(name);
    let mut git_args = vec!["worktree".to_string(), "add".to_string()];
    if options.force {
        git_args.push("--force".to_string());
    }
    if let Some(branch) = options.branch {
        git_args.push("-b".to_string());
        git_args.push(branch.to_string());
    }
    if options.detach {
        git_args.push("--detach".to_string());
    }
    git_args.push(worktree_path.display().to_string());
    if let Some(start_point) = options.start_point {
        git_args.push(start_point.to_string());
    }
    Ok(WorktreePlan {
        canonical_locator,
        canonical_path,
        worktree_path,
        git_args,
    })
}

fn validate_worktree_name(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        bail!("invalid worktree name: {name:?}");
    }
    Ok(())
}

struct Store {
    conn: Connection,
}

#[derive(Debug, Clone)]
struct RepoRecord {
    id: i64,
    current: Locator,
}

#[derive(Debug, Clone)]
struct ManagedRepoRecord {
    id: i64,
    current: Locator,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct RelationshipParent {
    parent: ManagedRepoRecord,
    storage: ManagedRepoRecord,
}

#[derive(Debug, Clone)]
struct BackgroundFetchCandidate {
    repo_id: i64,
    locator: Locator,
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitRemote {
    name: String,
    url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManageRelationship {
    Canonical,
    Fork,
    Mirror,
}

impl ManageRelationship {
    fn as_str(self) -> &'static str {
        match self {
            ManageRelationship::Canonical => "canonical",
            ManageRelationship::Fork => "fork",
            ManageRelationship::Mirror => "mirror",
        }
    }
}

#[derive(Debug)]
struct ManageChoice {
    checkout_url: String,
    canonical_url: String,
    relationship: ManageRelationship,
    materialize_canonical: bool,
}

struct ManageRemoteRelationship<'a> {
    checkout_locator: &'a Locator,
    canonical_locator: &'a Locator,
    checkout_url: &'a str,
    canonical_url: &'a str,
    repo_root: &'a Path,
    remotes: &'a [GitRemote],
    relationship: ManageRelationship,
    materialize_canonical: bool,
}

struct SeedCanonicalPlan<'a> {
    dependent_locator: &'a Locator,
    dependent_path: &'a Path,
    dependent_url: &'a str,
    controlling_locator: &'a Locator,
    controlling_path: &'a Path,
    controlling_url: &'a str,
    relationship: &'a str,
}

impl Store {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating state directory {}", parent.display()))?;
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS repos (
              id INTEGER PRIMARY KEY,
              identity TEXT NOT NULL UNIQUE,
              current_authority TEXT NOT NULL,
              current_remote_path TEXT NOT NULL,
              current_path TEXT NOT NULL,
              canonical_identity TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS locators (
              id INTEGER PRIMARY KEY,
              repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              authority TEXT NOT NULL,
              remote_path TEXT NOT NULL,
              path TEXT NOT NULL,
              is_current INTEGER NOT NULL DEFAULT 0,
              first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(repo_id, authority, remote_path)
            );

            CREATE TABLE IF NOT EXISTS aliases (
              id INTEGER PRIMARY KEY,
              repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              alias_path TEXT NOT NULL UNIQUE,
              target_path TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS forks (
              fork_repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              canonical_repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY(fork_repo_id, canonical_repo_id)
            );

            CREATE TABLE IF NOT EXISTS successors (
              old_ref TEXT PRIMARY KEY,
              new_authority TEXT NOT NULL,
              new_remote_path TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY,
              repo_id INTEGER REFERENCES repos(id) ON DELETE SET NULL,
              kind TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS related_history (
              id INTEGER PRIMARY KEY,
              repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              related_repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              shared_refs_json TEXT NOT NULL,
              resolution TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              resolved_at TEXT,
              UNIQUE(repo_id, related_repo_id)
            );

            CREATE TABLE IF NOT EXISTS background_fetch (
              repo_id INTEGER PRIMARY KEY REFERENCES repos(id) ON DELETE CASCADE,
              last_fetch_at INTEGER,
              last_changed_at INTEGER,
              learned_interval_seconds INTEGER NOT NULL,
              last_status TEXT,
              last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS worktrees (
              id INTEGER PRIMARY KEY,
              repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
              path TEXT NOT NULL UNIQUE,
              name TEXT,
              refs_prefix TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            ",
        )?;
        Ok(())
    }

    fn upsert_repo(
        &self,
        locator: &Locator,
        path: &Path,
        canonical_identity: Option<&str>,
    ) -> Result<i64> {
        self.conn.execute(
            "
            INSERT INTO repos (identity, current_authority, current_remote_path, current_path, canonical_identity)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(identity) DO UPDATE SET
              current_authority = excluded.current_authority,
              current_remote_path = excluded.current_remote_path,
              current_path = excluded.current_path,
              canonical_identity = COALESCE(excluded.canonical_identity, repos.canonical_identity),
              updated_at = CURRENT_TIMESTAMP
            ",
            params![
                locator.key(),
                locator.authority,
                locator.remote_path,
                path.display().to_string(),
                canonical_identity
            ],
        )?;
        let repo_id: i64 = self.conn.query_row(
            "SELECT id FROM repos WHERE identity = ?1",
            params![locator.key()],
            |row| row.get(0),
        )?;
        self.record_locator(repo_id, locator, path, true)?;
        Ok(repo_id)
    }

    fn record_locator(
        &self,
        repo_id: i64,
        locator: &Locator,
        path: &Path,
        current: bool,
    ) -> Result<()> {
        if current {
            self.conn.execute(
                "UPDATE locators SET is_current = 0 WHERE repo_id = ?1",
                params![repo_id],
            )?;
        }
        self.conn.execute(
            "
            INSERT INTO locators (repo_id, authority, remote_path, path, is_current)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(repo_id, authority, remote_path) DO UPDATE SET
              path = excluded.path,
              is_current = excluded.is_current,
              last_seen_at = CURRENT_TIMESTAMP
            ",
            params![
                repo_id,
                locator.authority,
                locator.remote_path,
                path.display().to_string(),
                i64::from(current)
            ],
        )?;
        Ok(())
    }

    fn find_repo(&self, repo_ref: &str) -> Result<Option<RepoRecord>> {
        let locator = Locator::parse(repo_ref)?;
        self.conn
            .query_row(
                "
                SELECT repos.id, repos.current_authority, repos.current_remote_path
                FROM repos
                JOIN locators ON locators.repo_id = repos.id
                WHERE (locators.authority = ?1 AND locators.remote_path = ?2)
                   OR repos.identity = ?3
                LIMIT 1
                ",
                params![locator.authority, locator.remote_path, locator.key()],
                |row| {
                    Ok(RepoRecord {
                        id: row.get(0)?,
                        current: Locator {
                            authority: row.get(1)?,
                            remote_path: row.get(2)?,
                        },
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn current_repos(&self) -> Result<Vec<ManagedRepoRecord>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT id, current_authority, current_remote_path, current_path
            FROM repos
            ORDER BY current_authority, current_remote_path
            ",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ManagedRepoRecord {
                id: row.get(0)?,
                current: Locator {
                    authority: row.get(1)?,
                    remote_path: row.get(2)?,
                },
                path: PathBuf::from(row.get::<_, String>(3)?),
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn repo_by_path(&self, path: &Path) -> Result<Option<ManagedRepoRecord>> {
        self.conn
            .query_row(
                "
                SELECT id, current_authority, current_remote_path, current_path
                FROM repos
                WHERE current_path = ?1
                LIMIT 1
                ",
                params![path.display().to_string()],
                |row| {
                    Ok(ManagedRepoRecord {
                        id: row.get(0)?,
                        current: Locator {
                            authority: row.get(1)?,
                            remote_path: row.get(2)?,
                        },
                        path: PathBuf::from(row.get::<_, String>(3)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn repo_by_id(&self, repo_id: i64) -> Result<Option<ManagedRepoRecord>> {
        self.conn
            .query_row(
                "
                SELECT id, current_authority, current_remote_path, current_path
                FROM repos
                WHERE id = ?1
                LIMIT 1
                ",
                params![repo_id],
                |row| {
                    Ok(ManagedRepoRecord {
                        id: row.get(0)?,
                        current: Locator {
                            authority: row.get(1)?,
                            remote_path: row.get(2)?,
                        },
                        path: PathBuf::from(row.get::<_, String>(3)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn background_fetch_candidates(
        &self,
        now_epoch_seconds: i64,
        minimum_interval_seconds: u64,
    ) -> Result<Vec<BackgroundFetchCandidate>> {
        let minimum_interval_seconds = minimum_interval_seconds.max(1) as i64;
        let mut stmt = self.conn.prepare(
            "
            SELECT
              repos.id,
              repos.current_authority,
              repos.current_remote_path,
              repos.current_path
            FROM repos
            LEFT JOIN background_fetch ON background_fetch.repo_id = repos.id
            WHERE background_fetch.last_fetch_at IS NULL
               OR ?1 - background_fetch.last_fetch_at >=
                  MAX(background_fetch.learned_interval_seconds, ?2)
            ORDER BY repos.current_authority, repos.current_remote_path
            ",
        )?;
        let rows = stmt.query_map(
            params![now_epoch_seconds, minimum_interval_seconds],
            |row| {
                Ok(BackgroundFetchCandidate {
                    repo_id: row.get(0)?,
                    locator: Locator {
                        authority: row.get(1)?,
                        remote_path: row.get(2)?,
                    },
                    path: PathBuf::from(row.get::<_, String>(3)?),
                })
            },
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn record_background_fetch(
        &self,
        repo_id: i64,
        now_epoch_seconds: i64,
        minimum_interval_seconds: u64,
        changed: bool,
        error: Option<&str>,
    ) -> Result<()> {
        let minimum_interval_seconds = minimum_interval_seconds.max(1) as i64;
        let existing: Option<(Option<i64>, i64)> = self
            .conn
            .query_row(
                "SELECT last_changed_at, learned_interval_seconds FROM background_fetch WHERE repo_id = ?1",
                params![repo_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let previous_interval = existing
            .as_ref()
            .map(|(_, interval)| (*interval).max(minimum_interval_seconds))
            .unwrap_or(minimum_interval_seconds);
        let learned_interval = if changed {
            minimum_interval_seconds
        } else {
            previous_interval
                .saturating_mul(2)
                .min(minimum_interval_seconds.saturating_mul(24))
                .max(minimum_interval_seconds)
        };
        let last_changed_at = if changed {
            Some(now_epoch_seconds)
        } else {
            existing.and_then(|(last_changed_at, _)| last_changed_at)
        };
        let status = if error.is_some() {
            "error"
        } else if changed {
            "changed"
        } else {
            "unchanged"
        };
        self.conn.execute(
            "
            INSERT INTO background_fetch (
              repo_id,
              last_fetch_at,
              last_changed_at,
              learned_interval_seconds,
              last_status,
              last_error
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(repo_id) DO UPDATE SET
              last_fetch_at = excluded.last_fetch_at,
              last_changed_at = COALESCE(excluded.last_changed_at, background_fetch.last_changed_at),
              learned_interval_seconds = excluded.learned_interval_seconds,
              last_status = excluded.last_status,
              last_error = excluded.last_error
            ",
            params![
                repo_id,
                now_epoch_seconds,
                last_changed_at,
                learned_interval,
                status,
                error,
            ],
        )?;
        Ok(())
    }

    fn background_fetch_state_by_repo_id(&self) -> Result<HashMap<i64, BackgroundFetchState>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT
              repo_id,
              last_fetch_at,
              last_changed_at,
              learned_interval_seconds,
              last_status,
              last_error
            FROM background_fetch
            ",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                BackgroundFetchState {
                    last_fetch_at: row.get(1)?,
                    last_changed_at: row.get(2)?,
                    learned_interval_seconds: row.get(3)?,
                    last_status: row.get(4)?,
                    last_error: row.get(5)?,
                },
            ))
        })?;
        rows.collect::<std::result::Result<HashMap<_, _>, _>>()
            .map_err(Into::into)
    }

    fn record_worktree(
        &self,
        repo_id: i64,
        path: &Path,
        name: Option<&str>,
        refs_prefix: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "
            INSERT INTO worktrees (repo_id, path, name, refs_prefix)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(path) DO UPDATE SET
              repo_id = excluded.repo_id,
              name = excluded.name,
              refs_prefix = excluded.refs_prefix,
              updated_at = CURRENT_TIMESTAMP
            ",
            params![repo_id, path.display().to_string(), name, refs_prefix],
        )?;
        Ok(())
    }

    fn managed_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT
              worktrees.repo_id,
              repos.current_authority,
              repos.current_remote_path,
              worktrees.path,
              worktrees.name,
              worktrees.refs_prefix
            FROM worktrees
            JOIN repos ON repos.id = worktrees.repo_id
            ORDER BY worktrees.path
            ",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ManagedWorktreeRecord {
                repo_id: row.get(0)?,
                repo_locator: Locator {
                    authority: row.get(1)?,
                    remote_path: row.get(2)?,
                },
                repo_type: "canonical".to_string(),
                path: PathBuf::from(row.get::<_, String>(3)?),
                name: row.get(4)?,
                refs_prefix: row.get(5)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn delete_repo(&self, repo_id: i64) -> Result<()> {
        let changed = self
            .conn
            .execute("DELETE FROM repos WHERE id = ?1", params![repo_id])?;
        if changed == 0 {
            bail!("unknown repository id: {repo_id}");
        }
        Ok(())
    }

    fn historical_locators(&self, repo_id: i64) -> Result<Vec<Locator>> {
        let mut stmt = self.conn.prepare(
            "SELECT authority, remote_path FROM locators WHERE repo_id = ?1 ORDER BY id",
        )?;
        let rows = stmt.query_map(params![repo_id], |row| {
            Ok(Locator {
                authority: row.get(0)?,
                remote_path: row.get(1)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn apply_move_metadata(&self, repo_id: i64, plan: &MovePlan) -> Result<()> {
        self.conn.execute(
            "
            UPDATE repos SET
              current_authority = ?2,
              current_remote_path = ?3,
              current_path = ?4,
              updated_at = CURRENT_TIMESTAMP
            WHERE id = ?1
            ",
            params![
                repo_id,
                plan.new_locator.authority,
                plan.new_locator.remote_path,
                plan.new_path.display().to_string()
            ],
        )?;
        self.record_locator(repo_id, &plan.old_locator, &plan.old_path, false)?;
        self.record_locator(repo_id, &plan.new_locator, &plan.new_path, true)?;
        for alias in &plan.aliases {
            self.conn.execute(
                "
                INSERT INTO aliases (repo_id, alias_path, target_path)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(alias_path) DO UPDATE SET target_path = excluded.target_path
                ",
                params![
                    repo_id,
                    alias.alias_path.display().to_string(),
                    alias.target_path.display().to_string()
                ],
            )?;
        }
        self.conn.execute(
            "INSERT INTO events (repo_id, kind, payload_json) VALUES (?1, 'move', ?2)",
            params![repo_id, serde_json::to_string(plan)?],
        )?;
        Ok(())
    }

    fn record_fork(&self, fork_repo_id: i64, canonical_repo_id: i64) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO forks (fork_repo_id, canonical_repo_id) VALUES (?1, ?2)",
            params![fork_repo_id, canonical_repo_id],
        )?;
        Ok(())
    }

    fn record_dependent_relationship(
        &self,
        dependent_repo_id: i64,
        parent_repo_id: i64,
        relationship: &str,
    ) -> Result<()> {
        match relationship {
            "fork" => self.record_fork(dependent_repo_id, parent_repo_id),
            "mirror" => self.record_resolved_related(dependent_repo_id, parent_repo_id, "mirror"),
            _ => bail!("invalid dependent relationship: {relationship}"),
        }
    }

    fn clear_dependent_relationships(&self, repo_id: i64) -> Result<()> {
        self.conn.execute(
            "DELETE FROM forks WHERE fork_repo_id = ?1",
            params![repo_id],
        )?;
        self.conn.execute(
            "DELETE FROM related_history WHERE repo_id = ?1 AND resolution IN ('fork', 'mirror')",
            params![repo_id],
        )?;
        self.conn.execute(
            "UPDATE repos SET canonical_identity = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
            params![repo_id],
        )?;
        Ok(())
    }

    fn ensure_no_dependents(&self, repo_id: i64) -> Result<()> {
        let fork_dependents: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM forks WHERE canonical_repo_id = ?1",
            params![repo_id],
            |row| row.get(0),
        )?;
        let mirror_dependents: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM related_history WHERE related_repo_id = ?1 AND resolution IN ('fork', 'mirror')",
            params![repo_id],
            |row| row.get(0),
        )?;
        if fork_dependents + mirror_dependents > 0 {
            bail!("repository is canonical for existing fork/mirror checkout(s)");
        }
        Ok(())
    }

    fn record_successor(&self, old_ref: &str, new_locator: &Locator) -> Result<()> {
        self.conn.execute(
            "
            INSERT INTO successors (old_ref, new_authority, new_remote_path)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(old_ref) DO UPDATE SET
              new_authority = excluded.new_authority,
              new_remote_path = excluded.new_remote_path
            ",
            params![old_ref, new_locator.authority, new_locator.remote_path],
        )?;
        Ok(())
    }

    fn aliases(&self, repo_ref: &str) -> Result<Vec<AliasPlan>> {
        let Some(record) = self.find_repo(repo_ref)? else {
            bail!("unknown repository: {repo_ref}");
        };
        let mut stmt = self.conn.prepare(
            "SELECT alias_path, target_path FROM aliases WHERE repo_id = ?1 ORDER BY alias_path",
        )?;
        let rows = stmt.query_map(params![record.id], |row| {
            Ok(AliasPlan {
                alias_path: PathBuf::from(row.get::<_, String>(0)?),
                target_path: PathBuf::from(row.get::<_, String>(1)?),
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn record_related_history(
        &self,
        repo_id: i64,
        related_repo_id: i64,
        shared_refs: &[String],
    ) -> Result<()> {
        if repo_id == related_repo_id {
            return Ok(());
        }
        let (repo_id, related_repo_id) = if repo_id < related_repo_id {
            (repo_id, related_repo_id)
        } else {
            (related_repo_id, repo_id)
        };
        self.conn.execute(
            "
            INSERT INTO related_history (repo_id, related_repo_id, shared_refs_json)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(repo_id, related_repo_id) DO UPDATE SET
              shared_refs_json = excluded.shared_refs_json
            ",
            params![
                repo_id,
                related_repo_id,
                serde_json::to_string(shared_refs)?
            ],
        )?;
        Ok(())
    }

    fn record_resolved_related(
        &self,
        dependent_repo_id: i64,
        controlling_repo_id: i64,
        resolution: &str,
    ) -> Result<()> {
        self.conn.execute(
            "
            INSERT INTO related_history (repo_id, related_repo_id, shared_refs_json, resolution, resolved_at)
            VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)
            ON CONFLICT(repo_id, related_repo_id) DO UPDATE SET
              resolution = excluded.resolution,
              resolved_at = CURRENT_TIMESTAMP
            ",
            params![dependent_repo_id, controlling_repo_id, "[]", resolution],
        )?;
        Ok(())
    }

    fn pending_related_count(&self) -> Result<i64> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM related_history WHERE resolution IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    fn related_suggestions(&self, unresolved_only: bool) -> Result<Vec<RelatedSuggestion>> {
        let filter = if unresolved_only {
            "WHERE related_history.resolution IS NULL"
        } else {
            ""
        };
        let mut stmt = self.conn.prepare(&format!(
            "
            SELECT
              related_history.id,
              repo.id,
              repo.current_authority,
              repo.current_remote_path,
              repo.current_path,
              related.id,
              related.current_authority,
              related.current_remote_path,
              related.current_path,
              related_history.shared_refs_json,
              related_history.resolution
            FROM related_history
            JOIN repos repo ON repo.id = related_history.repo_id
            JOIN repos related ON related.id = related_history.related_repo_id
            {filter}
            ORDER BY related_history.id
            "
        ))?;
        let rows = stmt.query_map([], |row| {
            let shared_refs_json: String = row.get(9)?;
            let shared_refs = serde_json::from_str(&shared_refs_json).unwrap_or_default();
            Ok(RelatedSuggestion {
                id: row.get(0)?,
                repo_id: row.get(1)?,
                repo_locator: Locator {
                    authority: row.get(2)?,
                    remote_path: row.get(3)?,
                },
                repo_path: PathBuf::from(row.get::<_, String>(4)?),
                related_repo_id: row.get(5)?,
                related_locator: Locator {
                    authority: row.get(6)?,
                    remote_path: row.get(7)?,
                },
                related_path: PathBuf::from(row.get::<_, String>(8)?),
                shared_refs,
                resolution: row.get(10)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn related_suggestion(&self, id: i64) -> Result<RelatedSuggestion> {
        self.related_suggestions(false)?
            .into_iter()
            .find(|suggestion| suggestion.id == id)
            .ok_or_else(|| anyhow!("unknown related-history suggestion: {id}"))
    }

    fn shared_git_dir_relationships(&self) -> Result<Vec<SharedGitDirRelationship>> {
        let mut relationships = Vec::new();
        let mut seen = HashSet::new();
        for suggestion in self.related_suggestions(false)? {
            let Some(resolution) = suggestion.resolution.as_deref() else {
                continue;
            };
            if !matches!(resolution, "fork" | "mirror") {
                continue;
            }
            let key = (
                resolution.to_string(),
                suggestion.repo_id,
                suggestion.related_repo_id,
            );
            if seen.insert(key) {
                relationships.push(SharedGitDirRelationship {
                    relationship: resolution.to_string(),
                    dependent_repo_id: suggestion.repo_id,
                    dependent_locator: suggestion.repo_locator,
                    dependent_path: suggestion.repo_path,
                    controlling_repo_id: suggestion.related_repo_id,
                    controlling_locator: suggestion.related_locator,
                    controlling_path: suggestion.related_path,
                });
            }
        }

        let mut stmt = self.conn.prepare(
            "
            SELECT
              fork.id,
              fork.current_authority,
              fork.current_remote_path,
              fork.current_path,
              canonical.id,
              canonical.current_authority,
              canonical.current_remote_path,
              canonical.current_path
            FROM forks
            JOIN repos fork ON fork.id = forks.fork_repo_id
            JOIN repos canonical ON canonical.id = forks.canonical_repo_id
            ORDER BY fork.current_authority, fork.current_remote_path
            ",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SharedGitDirRelationship {
                relationship: "fork".to_string(),
                dependent_repo_id: row.get(0)?,
                dependent_locator: Locator {
                    authority: row.get(1)?,
                    remote_path: row.get(2)?,
                },
                dependent_path: PathBuf::from(row.get::<_, String>(3)?),
                controlling_repo_id: row.get(4)?,
                controlling_locator: Locator {
                    authority: row.get(5)?,
                    remote_path: row.get(6)?,
                },
                controlling_path: PathBuf::from(row.get::<_, String>(7)?),
            })
        })?;
        for relationship in rows {
            let relationship = relationship?;
            let key = (
                relationship.relationship.clone(),
                relationship.dependent_repo_id,
                relationship.controlling_repo_id,
            );
            if seen.insert(key) {
                relationships.push(relationship);
            }
        }

        Ok(relationships)
    }

    fn resolve_related(&self, id: i64, resolution: &str) -> Result<()> {
        let changed = self.conn.execute(
            "
            UPDATE related_history
            SET resolution = ?2, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?1
            ",
            params![id, resolution],
        )?;
        if changed == 0 {
            bail!("unknown related-history suggestion: {id}");
        }
        Ok(())
    }
}

fn clone_repo(config: &Config, db: &Store, output: &Output, url: &str) -> Result<()> {
    let result = clone_repo_inner(config, db, url)?;
    output_clone(output, &result)
}

fn clone_repo_inner(config: &Config, db: &Store, url: &str) -> Result<CloneResult> {
    warn_pending_related(db)?;
    let locator = Locator::parse(url)?;
    let path = locator_path(&config.clone_root, &locator);
    fs::create_dir_all(path.parent().context("clone path has no parent")?)?;
    send_rpc_event_best_effort(
        &config.rpc_url,
        &RpcEvent::Started(CloneStartedEvent {
            client_id: config.client_id.clone(),
            url: url.to_string(),
            locator: locator.clone(),
            path: path.clone(),
            scan_root: config.clone_root.clone(),
        }),
    );
    let lifecycle = CloneLifecycle {
        rpc_url: config.rpc_url.clone(),
        client_id: config.client_id.clone(),
        url: url.to_string(),
        locator: locator.clone(),
        path: path.clone(),
        scan_root: config.clone_root.clone(),
    };
    let clone_result = if !config.clone_as_bare && which::which("ghq").is_ok() {
        match run_clone_command_with_cancellation(
            ghq_get_command(&config.clone_root, url),
            "ghq get",
            &lifecycle,
        )? {
            CloneCommandOutcome::Success => Ok(()),
            CloneCommandOutcome::Failed => run_clone_command_with_cancellation(
                git_clone_command(url, &path, config.clone_as_bare),
                "git clone",
                &lifecycle,
            )
            .and_then(CloneCommandOutcome::into_result),
        }
    } else {
        run_clone_command_with_cancellation(
            git_clone_command(url, &path, config.clone_as_bare),
            "git clone",
            &lifecycle,
        )
        .and_then(CloneCommandOutcome::into_result)
    };
    if let Err(error) = clone_result {
        send_rpc_event_best_effort(
            &config.rpc_url,
            &RpcEvent::Finished(CloneFinishedEvent {
                client_id: config.client_id.clone(),
                url: url.to_string(),
                locator,
                path,
                success: false,
                scan_root: config.clone_root.clone(),
            }),
        );
        return Err(error);
    }
    db.upsert_repo(&locator, &path, None)?;
    send_rpc_event_best_effort(
        &config.rpc_url,
        &RpcEvent::Finished(CloneFinishedEvent {
            client_id: config.client_id.clone(),
            url: url.to_string(),
            locator: locator.clone(),
            path: path.clone(),
            success: true,
            scan_root: config.clone_root.clone(),
        }),
    );
    Ok(CloneResult {
        action: "clone",
        locator,
        path,
    })
}

fn create_repo(config: &Config, db: &Store, output: &Output, args: CreateArgs) -> Result<()> {
    let locator = Locator::parse(&args.url)?;
    let path = locator_path(&config.clone_root, &locator);
    if path.exists() {
        bail!("target path already exists: {}", path.display());
    }
    let visibility = create_visibility(config, &args);
    let forge = resolve_create_forge(config, &locator)?;
    create_remote_repository(&forge, &locator, visibility)?;
    let clone = clone_repo_inner(config, db, &args.url)?;
    output_create(
        output,
        &CreateResult {
            action: "create",
            locator,
            path,
            backend: forge.backend,
            visibility,
            clone,
        },
    )
}

fn create_visibility(config: &Config, args: &CreateArgs) -> RepoVisibility {
    if args.private {
        RepoVisibility::Private
    } else if args.public {
        RepoVisibility::Public
    } else {
        config.create_default_visibility
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CreateForge {
    backend: ForgeBackend,
    token_envs: Vec<String>,
    api_base_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteRepoParts {
    owner: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpRequestPlan {
    method: &'static str,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

fn resolve_create_forge(config: &Config, locator: &Locator) -> Result<CreateForge> {
    if let Some(forge) = config.forges.get(&locator.authority) {
        return Ok(CreateForge {
            backend: forge.backend,
            token_envs: vec![
                forge
                    .token_env
                    .clone()
                    .unwrap_or_else(|| default_token_env(forge.backend).to_string()),
            ],
            api_base_url: forge
                .api_base_url
                .clone()
                .unwrap_or_else(|| default_api_base_url(forge.backend, &locator.authority)),
        });
    }

    match locator.authority.as_str() {
        "github.com" => Ok(CreateForge {
            backend: ForgeBackend::Github,
            token_envs: vec!["GITHUB_TOKEN".to_string()],
            api_base_url: "https://api.github.com".to_string(),
        }),
        "git.sr.ht" => Ok(CreateForge {
            backend: ForgeBackend::Sourcehut,
            token_envs: vec!["SOURCEHUT_TOKEN".to_string(), "SRHT_TOKEN".to_string()],
            api_base_url: "https://git.sr.ht/query".to_string(),
        }),
        authority => bail!(
            "no forge backend configured for {}; add it under `forges` in repo-manager config",
            authority
        ),
    }
}

fn default_token_env(backend: ForgeBackend) -> &'static str {
    match backend {
        ForgeBackend::Github => "GITHUB_TOKEN",
        ForgeBackend::Sourcehut => "SOURCEHUT_TOKEN",
        ForgeBackend::Forgejo => "FORGEJO_TOKEN",
    }
}

fn default_api_base_url(backend: ForgeBackend, authority: &str) -> String {
    match backend {
        ForgeBackend::Github => "https://api.github.com".to_string(),
        ForgeBackend::Sourcehut => format!("https://{authority}/query"),
        ForgeBackend::Forgejo => format!("https://{authority}"),
    }
}

fn create_remote_repository(
    forge: &CreateForge,
    locator: &Locator,
    visibility: RepoVisibility,
) -> Result<()> {
    let token = read_forge_token(&forge.token_envs)?;
    match forge.backend {
        ForgeBackend::Github => create_rest_repository(forge, locator, visibility, &token, true),
        ForgeBackend::Forgejo => create_rest_repository(forge, locator, visibility, &token, false),
        ForgeBackend::Sourcehut => create_sourcehut_repository(forge, locator, visibility, &token),
    }
}

fn read_forge_token(token_envs: &[String]) -> Result<String> {
    for token_env in token_envs {
        if let Ok(token) = env::var(token_env)
            && !token.trim().is_empty()
        {
            return Ok(token);
        }
    }
    bail!("missing forge token; set one of: {}", token_envs.join(", "))
}

fn create_rest_repository(
    forge: &CreateForge,
    locator: &Locator,
    visibility: RepoVisibility,
    token: &str,
    github: bool,
) -> Result<()> {
    let parts = parse_owner_repo(locator)?;
    let user = rest_authenticated_user(forge, token, github)?;
    let exists_response =
        curl_http_json(&rest_repository_get_request(forge, &parts, token, github))?;
    match exists_response.status {
        200 => bail!("remote repository already exists: {}", locator.key()),
        404 => {}
        status => bail!(
            "checking remote repository existence failed with HTTP {status}: {}",
            trim_http_body(&exists_response.body)
        ),
    }
    let request = rest_repository_create_request(forge, &parts, visibility, token, &user, github);
    let response = curl_http_json(&request)?;
    if response.status != 201 {
        bail!(
            "creating remote repository failed with HTTP {}: {}",
            response.status,
            trim_http_body(&response.body)
        );
    }
    Ok(())
}

fn rest_authenticated_user(forge: &CreateForge, token: &str, github: bool) -> Result<String> {
    let response = curl_http_json(&rest_authenticated_user_request(forge, token, github))?;
    if response.status != 200 {
        bail!(
            "reading authenticated forge user failed with HTTP {}: {}",
            response.status,
            trim_http_body(&response.body)
        );
    }
    let json: serde_json::Value =
        serde_json::from_str(&response.body).context("parsing authenticated user response")?;
    json.get("login")
        .and_then(|value| value.as_str())
        .filter(|login| !login.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("authenticated user response did not include `login`"))
}

fn create_sourcehut_repository(
    forge: &CreateForge,
    locator: &Locator,
    visibility: RepoVisibility,
    token: &str,
) -> Result<()> {
    let parts = parse_sourcehut_repo(locator)?;
    let user_response = curl_http_json(&sourcehut_me_request(forge, token))?;
    ensure_sourcehut_success(&user_response, "reading authenticated SourceHut user")?;
    let user = sourcehut_username(&user_response.body)?;
    if user != parts.owner {
        bail!(
            "SourceHut can only create repositories for the authenticated user `{user}`, got `{}`",
            parts.owner
        );
    }

    let exists_response =
        curl_http_json(&sourcehut_repository_by_path_request(forge, token, locator))?;
    ensure_sourcehut_success(&exists_response, "checking remote repository existence")?;
    if sourcehut_repository_exists(&exists_response.body)? {
        bail!("remote repository already exists: {}", locator.key());
    }

    let create_response = curl_http_json(&sourcehut_create_request(
        forge,
        token,
        &parts.name,
        visibility,
    ))?;
    ensure_sourcehut_success(&create_response, "creating SourceHut repository")
}

fn parse_owner_repo(locator: &Locator) -> Result<RemoteRepoParts> {
    let mut parts = locator.remote_path.split('/');
    let owner = parts.next().unwrap_or_default();
    let name = parts.next().unwrap_or_default();
    if owner.is_empty() || name.is_empty() || parts.next().is_some() {
        bail!(
            "repository URL for {} must have exactly <owner>/<repo>",
            locator.authority
        );
    }
    Ok(RemoteRepoParts {
        owner: owner.to_string(),
        name: name.to_string(),
    })
}

fn parse_sourcehut_repo(locator: &Locator) -> Result<RemoteRepoParts> {
    let parts = parse_owner_repo(locator)?;
    let owner = parts.owner.strip_prefix('~').ok_or_else(|| {
        anyhow!(
            "SourceHut repository path must be ~<user>/<repo>, got {}",
            locator.key()
        )
    })?;
    Ok(RemoteRepoParts {
        owner: owner.to_string(),
        name: parts.name,
    })
}

fn rest_authenticated_user_request(
    forge: &CreateForge,
    token: &str,
    github: bool,
) -> HttpRequestPlan {
    HttpRequestPlan {
        method: "GET",
        url: if github {
            format!("{}/user", forge.api_base_url.trim_end_matches('/'))
        } else {
            format!("{}/api/v1/user", forge.api_base_url.trim_end_matches('/'))
        },
        headers: rest_headers(token, github, false),
        body: None,
    }
}

fn rest_repository_get_request(
    forge: &CreateForge,
    parts: &RemoteRepoParts,
    token: &str,
    github: bool,
) -> HttpRequestPlan {
    HttpRequestPlan {
        method: "GET",
        url: if github {
            format!(
                "{}/repos/{}/{}",
                forge.api_base_url.trim_end_matches('/'),
                parts.owner,
                parts.name
            )
        } else {
            format!(
                "{}/api/v1/repos/{}/{}",
                forge.api_base_url.trim_end_matches('/'),
                parts.owner,
                parts.name
            )
        },
        headers: rest_headers(token, github, false),
        body: None,
    }
}

fn rest_repository_create_request(
    forge: &CreateForge,
    parts: &RemoteRepoParts,
    visibility: RepoVisibility,
    token: &str,
    authenticated_user: &str,
    github: bool,
) -> HttpRequestPlan {
    let user_repo = parts.owner.eq_ignore_ascii_case(authenticated_user);
    let url = if github {
        if user_repo {
            format!("{}/user/repos", forge.api_base_url.trim_end_matches('/'))
        } else {
            format!(
                "{}/orgs/{}/repos",
                forge.api_base_url.trim_end_matches('/'),
                parts.owner
            )
        }
    } else if user_repo {
        format!(
            "{}/api/v1/user/repos",
            forge.api_base_url.trim_end_matches('/')
        )
    } else {
        format!(
            "{}/api/v1/orgs/{}/repos",
            forge.api_base_url.trim_end_matches('/'),
            parts.owner
        )
    };
    HttpRequestPlan {
        method: "POST",
        url,
        headers: rest_headers(token, github, true),
        body: Some(
            serde_json::json!({
                "name": parts.name,
                "private": visibility.is_private()
            })
            .to_string(),
        ),
    }
}

fn rest_headers(token: &str, github: bool, json_body: bool) -> Vec<(String, String)> {
    let mut headers = if github {
        vec![
            (
                "Accept".to_string(),
                "application/vnd.github+json".to_string(),
            ),
            ("X-GitHub-Api-Version".to_string(), "2022-11-28".to_string()),
            ("User-Agent".to_string(), "repo-manager".to_string()),
            ("Authorization".to_string(), format!("Bearer {token}")),
        ]
    } else {
        vec![
            ("Accept".to_string(), "application/json".to_string()),
            ("Authorization".to_string(), format!("token {token}")),
        ]
    };
    if json_body {
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
    }
    headers
}

fn sourcehut_me_request(forge: &CreateForge, token: &str) -> HttpRequestPlan {
    sourcehut_graphql_request(
        forge,
        token,
        "query me { me { username canonicalName } }",
        serde_json::json!({}),
    )
}

fn sourcehut_repository_by_path_request(
    forge: &CreateForge,
    token: &str,
    locator: &Locator,
) -> HttpRequestPlan {
    sourcehut_graphql_request(
        forge,
        token,
        "query repositoryByDiskPath($path: String!) { repositoryByDiskPath(path: $path) { id name } }",
        serde_json::json!({ "path": locator.remote_path }),
    )
}

fn sourcehut_create_request(
    forge: &CreateForge,
    token: &str,
    name: &str,
    visibility: RepoVisibility,
) -> HttpRequestPlan {
    sourcehut_graphql_request(
        forge,
        token,
        "mutation createRepository($name: String!, $visibility: Visibility!) { createRepository(name: $name, visibility: $visibility) { id name visibility } }",
        serde_json::json!({ "name": name, "visibility": visibility.sourcehut() }),
    )
}

fn sourcehut_graphql_request(
    forge: &CreateForge,
    token: &str,
    query: &str,
    variables: serde_json::Value,
) -> HttpRequestPlan {
    HttpRequestPlan {
        method: "POST",
        url: forge.api_base_url.clone(),
        headers: vec![
            ("Authorization".to_string(), format!("Bearer {token}")),
            ("Content-Type".to_string(), "application/json".to_string()),
        ],
        body: Some(
            serde_json::json!({
                "query": query,
                "variables": variables,
            })
            .to_string(),
        ),
    }
}

fn ensure_sourcehut_success(response: &HttpResponse, action: &str) -> Result<()> {
    if response.status != 200 {
        bail!(
            "{action} failed with HTTP {}: {}",
            response.status,
            trim_http_body(&response.body)
        );
    }
    let json: serde_json::Value =
        serde_json::from_str(&response.body).with_context(|| format!("{action}: parsing JSON"))?;
    if let Some(errors) = json.get("errors") {
        bail!("{action} failed: {errors}");
    }
    Ok(())
}

fn sourcehut_username(body: &str) -> Result<String> {
    let json: serde_json::Value = serde_json::from_str(body).context("parsing SourceHut user")?;
    json.pointer("/data/me/username")
        .or_else(|| json.pointer("/data/me/canonicalName"))
        .and_then(|value| value.as_str())
        .map(|value| value.trim_start_matches('~').to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("SourceHut user response did not include username"))
}

fn sourcehut_repository_exists(body: &str) -> Result<bool> {
    let json: serde_json::Value =
        serde_json::from_str(body).context("parsing SourceHut repository lookup")?;
    Ok(!json
        .pointer("/data/repositoryByDiskPath")
        .unwrap_or(&serde_json::Value::Null)
        .is_null())
}

fn curl_http_json(request: &HttpRequestPlan) -> Result<HttpResponse> {
    let mut command = Command::new("curl");
    command.args(["-sS", "-L", "-X", request.method]);
    for (name, value) in &request.headers {
        command.arg("-H").arg(format!("{name}: {value}"));
    }
    if let Some(body) = &request.body {
        command.arg("-d").arg(body);
    }
    command.args(["-w", "\nrepo-manager-http-status:%{http_code}"]);
    command.arg(&request.url);
    let output = command
        .output()
        .with_context(|| format!("requesting {}", request.url))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "curl failed with status {}: {}",
            output.status,
            stderr.trim()
        );
    }
    let stdout = String::from_utf8(output.stdout).context("HTTP response is not UTF-8")?;
    let Some((body, status)) = stdout.rsplit_once("\nrepo-manager-http-status:") else {
        bail!("curl response did not include HTTP status marker");
    };
    Ok(HttpResponse {
        status: status.trim().parse().context("parsing HTTP status")?,
        body: body.to_string(),
    })
}

fn trim_http_body(body: &str) -> String {
    let body = body.trim();
    let mut chars = body.chars();
    let trimmed = chars.by_ref().take(500).collect::<String>();
    if chars.next().is_some() {
        format!("{trimmed}...")
    } else {
        body.to_string()
    }
}

fn manage_repo(config: &Config, db: &Store, output: &Output, args: ManageArgs) -> Result<()> {
    warn_pending_related(db)?;
    let original_root = git_worktree_root(&args.path)?;
    let remotes = git_remotes(&original_root)?;
    let assume_origin_as_canonical =
        args.assume_origin_as_canonical || config.assume_origin_as_canonical;
    let choice = choose_manage_choice(&remotes, assume_origin_as_canonical)?;
    let checkout_locator = Locator::parse(&choice.checkout_url)?;
    let canonical_locator = Locator::parse(&choice.canonical_url)?;
    let (repo_root, moved_from) =
        move_repo_into_managed_path(config, &original_root, &checkout_locator)?;
    record_manage_remote_relationships(
        config,
        db,
        ManageRemoteRelationship {
            checkout_locator: &checkout_locator,
            canonical_locator: &canonical_locator,
            checkout_url: &choice.checkout_url,
            canonical_url: &choice.canonical_url,
            repo_root: &repo_root,
            remotes: &remotes,
            relationship: choice.relationship,
            materialize_canonical: choice.materialize_canonical,
        },
    )?;
    let history_review_requested =
        request_daemon_history_review(config, &checkout_locator, &repo_root, &choice.canonical_url);
    output_manage(
        output,
        &ManageResult {
            action: "manage",
            locator: checkout_locator,
            canonical_url: choice.canonical_url,
            relationship: choice.relationship.as_str(),
            path: repo_root,
            moved_from,
            history_review_requested,
        },
    )
}

fn choose_manage_choice(
    remotes: &[GitRemote],
    assume_origin_as_canonical: bool,
) -> Result<ManageChoice> {
    let origin = remotes.iter().find(|remote| remote.name == "origin");
    if let Some(origin) = origin
        && (assume_origin_as_canonical || confirm_origin_as_canonical(origin)?)
    {
        return Ok(ManageChoice {
            checkout_url: origin.url.clone(),
            canonical_url: origin.url.clone(),
            relationship: ManageRelationship::Canonical,
            materialize_canonical: false,
        });
    }
    let canonical_choice = prompt_canonical_url(remotes)?;
    match canonical_choice {
        CanonicalPromptAnswer::Remote(canonical_url) => {
            let Some(origin) = origin else {
                return Ok(ManageChoice {
                    checkout_url: canonical_url.clone(),
                    canonical_url,
                    relationship: ManageRelationship::Canonical,
                    materialize_canonical: false,
                });
            };
            if canonical_url == origin.url {
                return Ok(ManageChoice {
                    checkout_url: origin.url.clone(),
                    canonical_url,
                    relationship: ManageRelationship::Canonical,
                    materialize_canonical: false,
                });
            }
            Ok(ManageChoice {
                checkout_url: origin.url.clone(),
                canonical_url,
                relationship: prompt_dependent_relationship()?,
                materialize_canonical: true,
            })
        }
        CanonicalPromptAnswer::NoCanonicalRemote => {
            let checkout_url = match origin {
                Some(origin) => origin.url.clone(),
                None => prompt_checkout_url(remotes)?,
            };
            let relationship = prompt_dependent_relationship()?;
            let canonical_url = prompt_required_url("canonical URL (not cloned now)")?;
            Ok(ManageChoice {
                checkout_url,
                canonical_url,
                relationship,
                materialize_canonical: false,
            })
        }
        CanonicalPromptAnswer::Invalid => unreachable!("prompt returns only valid selections"),
    }
}

fn confirm_origin_as_canonical(origin: &GitRemote) -> Result<bool> {
    loop {
        eprint!("Use origin as canonical? [{}] [Y/n] ", origin.url);
        io::stderr().flush().context("flushing prompt")?;
        let answer = read_prompt_line()?;
        match answer.trim().to_ascii_lowercase().as_str() {
            "" | "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => eprintln!("enter y or n"),
        }
    }
}

fn prompt_canonical_url(remotes: &[GitRemote]) -> Result<CanonicalPromptAnswer> {
    if remotes.is_empty() {
        return Ok(CanonicalPromptAnswer::NoCanonicalRemote);
    }
    eprintln!("Select the canonical remote:");
    for (index, remote) in remotes.iter().enumerate() {
        eprintln!("  {}. {} {}", index + 1, remote.name, remote.url);
    }
    eprintln!("  skip. Canonical is not configured as a remote");
    eprintln!("  Or paste a canonical URL directly.");

    loop {
        eprint!("canonical remote [1-{}], URL, or skip: ", remotes.len());
        io::stderr().flush().context("flushing prompt")?;
        let answer = read_prompt_line()?;
        match parse_canonical_prompt_answer(&answer, remotes) {
            answer @ (CanonicalPromptAnswer::Remote(_)
            | CanonicalPromptAnswer::NoCanonicalRemote) => {
                return Ok(answer);
            }
            CanonicalPromptAnswer::Invalid => {
                eprintln!(
                    "enter a number from 1 to {}, a canonical URL, or skip",
                    remotes.len()
                );
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CanonicalPromptAnswer {
    Remote(String),
    NoCanonicalRemote,
    Invalid,
}

fn parse_canonical_prompt_answer(answer: &str, remotes: &[GitRemote]) -> CanonicalPromptAnswer {
    let answer = answer.trim();
    if answer.eq_ignore_ascii_case("skip") || answer.eq_ignore_ascii_case("none") {
        return CanonicalPromptAnswer::NoCanonicalRemote;
    }
    if let Ok(index) = answer.parse::<usize>() {
        if (1..=remotes.len()).contains(&index) {
            return CanonicalPromptAnswer::Remote(remotes[index - 1].url.clone());
        }
        return CanonicalPromptAnswer::Invalid;
    }
    if !answer.is_empty() && Locator::parse(answer).is_ok() {
        return CanonicalPromptAnswer::Remote(answer.to_string());
    }
    CanonicalPromptAnswer::Invalid
}

fn prompt_required_url(label: &str) -> Result<String> {
    loop {
        eprint!("{label}: ");
        io::stderr().flush().context("flushing prompt")?;
        let answer = read_prompt_line()?;
        let url = answer.trim();
        if !url.is_empty() {
            return Ok(url.to_string());
        }
        eprintln!("{label} is required");
    }
}

fn prompt_checkout_url(remotes: &[GitRemote]) -> Result<String> {
    if remotes.is_empty() {
        return prompt_required_url("checkout URL");
    }
    eprintln!("Select the remote that identifies this checkout:");
    for (index, remote) in remotes.iter().enumerate() {
        eprintln!("  {}. {} {}", index + 1, remote.name, remote.url);
    }
    eprintln!("  Or paste a checkout URL directly.");

    loop {
        eprint!("checkout remote [1-{}] or URL: ", remotes.len());
        io::stderr().flush().context("flushing prompt")?;
        let answer = read_prompt_line()?;
        match parse_checkout_prompt_answer(&answer, remotes) {
            Some(url) => return Ok(url),
            None => eprintln!(
                "enter a number from 1 to {} or a checkout URL",
                remotes.len()
            ),
        }
    }
}

fn parse_checkout_prompt_answer(answer: &str, remotes: &[GitRemote]) -> Option<String> {
    let answer = answer.trim();
    if let Ok(index) = answer.parse::<usize>()
        && (1..=remotes.len()).contains(&index)
    {
        return Some(remotes[index - 1].url.clone());
    }
    (!answer.is_empty() && Locator::parse(answer).is_ok()).then(|| answer.to_string())
}

fn prompt_dependent_relationship() -> Result<ManageRelationship> {
    eprintln!(
        "This checkout is not canonical. Select its relationship to the canonical repository:"
    );
    eprintln!("  1. fork");
    eprintln!("  2. mirror");
    loop {
        eprint!("relationship [1-2, fork, or mirror]: ");
        io::stderr().flush().context("flushing prompt")?;
        let answer = read_prompt_line()?;
        match parse_dependent_relationship_answer(&answer) {
            Some(relationship) => return Ok(relationship),
            None => eprintln!("enter fork or mirror"),
        }
    }
}

fn parse_dependent_relationship_answer(answer: &str) -> Option<ManageRelationship> {
    match answer.trim().to_ascii_lowercase().as_str() {
        "1" | "f" | "fork" => Some(ManageRelationship::Fork),
        "2" | "m" | "mirror" => Some(ManageRelationship::Mirror),
        _ => None,
    }
}

fn read_prompt_line() -> Result<String> {
    let mut answer = String::new();
    let bytes_read = io::stdin()
        .read_line(&mut answer)
        .context("reading prompt response")?;
    if bytes_read == 0 {
        bail!("prompt response ended before a canonical URL was selected");
    }
    Ok(answer)
}

fn record_manage_remote_relationships(
    config: &Config,
    db: &Store,
    plan: ManageRemoteRelationship<'_>,
) -> Result<()> {
    if plan.relationship != ManageRelationship::Canonical {
        if plan.materialize_canonical {
            let canonical_path = locator_path(&config.clone_root, plan.canonical_locator);
            if !canonical_path.exists() {
                return seed_canonical_from_dependent_checkout(
                    db,
                    SeedCanonicalPlan {
                        dependent_locator: plan.checkout_locator,
                        dependent_path: plan.repo_root,
                        dependent_url: plan.checkout_url,
                        controlling_locator: plan.canonical_locator,
                        controlling_path: &canonical_path,
                        controlling_url: plan.canonical_url,
                        relationship: plan.relationship.as_str(),
                    },
                )
                .map(|_| ());
            }
            ensure_remote(&canonical_path, "origin", plan.canonical_url)?;
            materialize_related_shared_git_dir(
                db,
                plan.checkout_locator,
                plan.repo_root,
                plan.canonical_locator,
                &canonical_path,
                plan.relationship.as_str(),
                config.clone_as_bare,
            )?;
            return Ok(());
        }

        ensure_deferred_dependent_remotes(
            plan.repo_root,
            plan.checkout_locator,
            plan.checkout_url,
            plan.canonical_url,
            plan.relationship,
        )?;
        let canonical_path = locator_path(&config.clone_root, plan.canonical_locator);
        let canonical_id = db.upsert_repo(plan.canonical_locator, &canonical_path, None)?;
        let checkout_id = db.upsert_repo(
            plan.checkout_locator,
            plan.repo_root,
            Some(&plan.canonical_locator.key()),
        )?;
        match plan.relationship {
            ManageRelationship::Fork => db.record_fork(checkout_id, canonical_id)?,
            ManageRelationship::Mirror => {
                db.record_resolved_related(checkout_id, canonical_id, "mirror")?;
            }
            ManageRelationship::Canonical => unreachable!("handled above"),
        }
        return Ok(());
    }

    let canonical_id = db.upsert_repo(plan.canonical_locator, plan.repo_root, None)?;
    for remote in plan.remotes {
        let Ok(remote_locator) = Locator::parse(&remote.url) else {
            debug!(
                "skipping non-locator-compatible remote {}: {}",
                remote.name, remote.url
            );
            continue;
        };
        if remote_locator == *plan.canonical_locator {
            continue;
        }
        let remote_id = db.upsert_repo(
            &remote_locator,
            plan.repo_root,
            Some(&plan.canonical_locator.key()),
        )?;
        db.record_fork(remote_id, canonical_id)?;
    }
    Ok(())
}

fn seed_canonical_from_dependent_checkout(
    db: &Store,
    plan: SeedCanonicalPlan<'_>,
) -> Result<SharedGitDirResolution> {
    if plan.controlling_path.exists() {
        bail!(
            "canonical checkout already exists: {}",
            plan.controlling_path.display()
        );
    }
    if !plan.dependent_path.exists() {
        bail!(
            "{} checkout does not exist: {}",
            plan.relationship,
            plan.dependent_path.display()
        );
    }

    let dependent_remote = related_remote_name(plan.relationship, plan.dependent_locator);
    let default_branch = dependent_default_branch(plan.dependent_path)?;
    let local_branch =
        dependent_local_branch(plan.relationship, plan.dependent_locator, &default_branch);
    let remote_branch = format!("{dependent_remote}/{default_branch}");

    fs::create_dir_all(
        plan.controlling_path
            .parent()
            .context("canonical repository path has no parent")?,
    )
    .with_context(|| {
        format!(
            "creating canonical repository parent for {}",
            plan.controlling_path.display()
        )
    })?;
    fs::rename(plan.dependent_path, plan.controlling_path).with_context(|| {
        format!(
            "moving seed checkout {} to canonical path {}",
            plan.dependent_path.display(),
            plan.controlling_path.display()
        )
    })?;

    let seed_result = (|| -> Result<SharedGitDirResolution> {
        ensure_seed_controlling_remotes(
            plan.controlling_path,
            &dependent_remote,
            plan.dependent_url,
            plan.controlling_url,
        )?;
        ensure_tracking_branch(plan.controlling_path, &local_branch, &remote_branch)?;
        run_git_in(
            plan.controlling_path,
            [
                "worktree",
                "add",
                &plan.dependent_path.display().to_string(),
                &local_branch,
            ],
        )?;
        restore_dependent_worktree_state(plan.controlling_path, plan.dependent_path)?;
        checkout_controlling_default_branch(plan.controlling_path, &default_branch)?;

        let controlling_id =
            db.upsert_repo(plan.controlling_locator, plan.controlling_path, None)?;
        let dependent_id = db.upsert_repo(
            plan.dependent_locator,
            plan.dependent_path,
            Some(&plan.controlling_locator.key()),
        )?;
        if plan.relationship == "fork" {
            db.record_fork(dependent_id, controlling_id)?;
        } else {
            db.record_resolved_related(dependent_id, controlling_id, plan.relationship)?;
        }

        Ok(SharedGitDirResolution {
            dependent_locator: plan.dependent_locator.clone(),
            controlling_locator: plan.controlling_locator.clone(),
            dependent_path: plan.dependent_path.to_path_buf(),
            controlling_path: plan.controlling_path.to_path_buf(),
            dependent_remote,
            dependent_url: plan.dependent_url.to_string(),
            local_branch,
            remote_branch,
            converted_to_worktree: true,
        })
    })();

    match seed_result {
        Ok(resolution) => Ok(resolution),
        Err(error) => {
            if !plan.dependent_path.exists() && plan.controlling_path.exists() {
                let _ = fs::rename(plan.controlling_path, plan.dependent_path);
            }
            Err(error)
        }
    }
}

fn ensure_seed_controlling_remotes(
    controlling_path: &Path,
    dependent_remote: &str,
    dependent_url: &str,
    controlling_url: &str,
) -> Result<()> {
    if git_remote_url(controlling_path, dependent_remote)?.is_none()
        && git_remote_url(controlling_path, "origin")?.is_some()
    {
        run_git_in(
            controlling_path,
            ["remote", "rename", "origin", dependent_remote],
        )?;
    }
    ensure_remote(controlling_path, dependent_remote, dependent_url)?;
    ensure_remote(controlling_path, "origin", controlling_url)?;
    run_git_in(controlling_path, ["fetch", "origin"])?;
    let _ = run_git_in(controlling_path, ["remote", "set-head", "origin", "-a"]);
    let _ = run_git_in(controlling_path, ["fetch", dependent_remote]);
    let _ = run_git_in(
        controlling_path,
        ["remote", "set-head", dependent_remote, "-a"],
    );
    Ok(())
}

fn restore_dependent_worktree_state(seed_path: &Path, dependent_path: &Path) -> Result<()> {
    clear_worktree_contents(dependent_path)?;
    copy_worktree_contents(seed_path, dependent_path)?;
    let seed_index = git_dir(seed_path)?.join("index");
    if seed_index.exists() {
        let dependent_index = git_dir(dependent_path)?.join("index");
        fs::copy(&seed_index, &dependent_index).with_context(|| {
            format!(
                "copying index {} to {}",
                seed_index.display(),
                dependent_index.display()
            )
        })?;
    }
    Ok(())
}

fn checkout_controlling_default_branch(
    controlling_path: &Path,
    fallback_branch: &str,
) -> Result<()> {
    let default_branch = remote_default_branch(controlling_path, "origin")?
        .unwrap_or_else(|| fallback_branch.to_string());
    let remote_branch = format!("origin/{default_branch}");
    run_git_in(
        controlling_path,
        ["checkout", "-B", &default_branch, &remote_branch],
    )?;
    run_git_in(
        controlling_path,
        [
            "branch",
            "--set-upstream-to",
            &remote_branch,
            &default_branch,
        ],
    )?;
    run_git_in(controlling_path, ["reset", "--hard"])?;
    run_git_in(controlling_path, ["clean", "-fdx"])?;
    Ok(())
}

fn clear_worktree_contents(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let entry = entry?;
        if entry.file_name() == ".git" {
            continue;
        }
        let entry_path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() && !file_type.is_symlink() {
            fs::remove_dir_all(&entry_path)
                .with_context(|| format!("removing {}", entry_path.display()))?;
        } else {
            fs::remove_file(&entry_path)
                .with_context(|| format!("removing {}", entry_path.display()))?;
        }
    }
    Ok(())
}

fn copy_worktree_contents(from: &Path, to: &Path) -> Result<()> {
    for entry in fs::read_dir(from).with_context(|| format!("reading {}", from.display()))? {
        let entry = entry?;
        if entry.file_name() == ".git" {
            continue;
        }
        copy_path(&entry.path(), &to.join(entry.file_name()))?;
    }
    Ok(())
}

fn copy_path(from: &Path, to: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(from)
        .with_context(|| format!("reading metadata for {}", from.display()))?;
    if metadata.file_type().is_symlink() {
        let target =
            fs::read_link(from).with_context(|| format!("reading symlink {}", from.display()))?;
        symlink_any(&target, to)?;
    } else if metadata.is_dir() {
        fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
        for entry in fs::read_dir(from).with_context(|| format!("reading {}", from.display()))? {
            let entry = entry?;
            copy_path(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else {
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::copy(from, to)
            .with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
        fs::set_permissions(to, metadata.permissions())
            .with_context(|| format!("copying permissions to {}", to.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn symlink_any(target: &Path, link: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link)
        .with_context(|| format!("symlinking {} -> {}", link.display(), target.display()))
}

#[cfg(windows)]
fn symlink_any(target: &Path, link: &Path) -> Result<()> {
    if target.is_dir() {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        std::os::windows::fs::symlink_file(target, link)
    }
    .with_context(|| format!("symlinking {} -> {}", link.display(), target.display()))
}

fn ensure_deferred_dependent_remotes(
    repo_root: &Path,
    checkout_locator: &Locator,
    checkout_url: &str,
    canonical_url: &str,
    relationship: ManageRelationship,
) -> Result<()> {
    ensure_remote(repo_root, "canonical", canonical_url)?;
    ensure_remote(
        repo_root,
        &related_remote_name(relationship.as_str(), checkout_locator),
        checkout_url,
    )?;
    if git_remote_url(repo_root, "origin")?.is_none() {
        ensure_remote(repo_root, "origin", checkout_url)?;
    }
    Ok(())
}

fn move_repo_into_managed_path(
    config: &Config,
    repo_root: &Path,
    locator: &Locator,
) -> Result<(PathBuf, Option<PathBuf>)> {
    let expected_path = locator_path(&config.clone_root, locator);
    if comparable_path(repo_root) == comparable_path(&expected_path) {
        return Ok((repo_root.to_path_buf(), None));
    }
    if expected_path.exists() {
        bail!(
            "managed path for {} already exists: {}",
            locator.key(),
            expected_path.display()
        );
    }
    if expected_path.starts_with(repo_root) {
        bail!(
            "cannot move {} into its own subtree at {}",
            repo_root.display(),
            expected_path.display()
        );
    }
    fs::create_dir_all(
        expected_path
            .parent()
            .context("managed repository path has no parent")?,
    )
    .with_context(|| {
        format!(
            "creating managed repository parent for {}",
            expected_path.display()
        )
    })?;
    fs::rename(repo_root, &expected_path).with_context(|| {
        format!(
            "moving existing checkout {} to {}",
            repo_root.display(),
            expected_path.display()
        )
    })?;
    if let Some(parent) = repo_root.parent() {
        prune_empty_parent_dirs(parent, &config.root)?;
    }
    Ok((expected_path, Some(repo_root.to_path_buf())))
}

fn prune_empty_parent_dirs(start: &Path, stop_at: &Path) -> Result<()> {
    let stop_at = comparable_path(stop_at);
    let mut current = start.to_path_buf();
    while path_is_under(&current, &stop_at) && comparable_path(&current) != stop_at {
        match fs::remove_dir(&current) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                break;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("removing empty parent directory {}", current.display())
                });
            }
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    Ok(())
}

fn request_daemon_history_review(
    config: &Config,
    locator: &Locator,
    path: &Path,
    canonical_url: &str,
) -> bool {
    send_rpc_event_best_effort(
        &config.rpc_url,
        &RpcEvent::ManageRequested(ManageRequestedEvent {
            client_id: config.client_id.clone(),
            url: canonical_url.to_string(),
            locator: locator.clone(),
            path: path.to_path_buf(),
            scan_root: config.clone_root.clone(),
        }),
    )
}

fn git_worktree_root(path: &Path) -> Result<PathBuf> {
    let output = git_command(path)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .with_context(|| format!("resolving Git worktree root from {}", path.display()))?;
    if !output.status.success() {
        bail!("not a Git working tree: {}", path.display());
    }
    let root = String::from_utf8(output.stdout).context("Git worktree root is not UTF-8")?;
    Ok(PathBuf::from(root.trim()))
}

fn ghq_get_command(root: &Path, url: &str) -> Command {
    let mut command = Command::new("ghq");
    command.env("GHQ_ROOT", root).arg("get").arg(url);
    command
}

fn git_clone_command(url: &str, path: &Path, bare: bool) -> Command {
    let mut command = Command::new("git");
    command
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_PREFIX")
        .arg("clone");
    if bare {
        command.arg("--bare");
    }
    command.arg(url).arg(path);
    command
}

fn run_git_clone(url: &str, path: &Path, bare: bool) -> Result<()> {
    let status = git_clone_command(url, path, bare)
        .status()
        .with_context(|| format!("cloning {url} to {}", path.display()))?;
    if !status.success() {
        bail!("git clone failed with status {status}");
    }
    Ok(())
}

#[derive(Debug)]
struct CloneLifecycle {
    rpc_url: String,
    client_id: String,
    url: String,
    locator: Locator,
    path: PathBuf,
    scan_root: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloneCommandOutcome {
    Success,
    Failed,
}

impl CloneCommandOutcome {
    fn into_result(self) -> Result<()> {
        match self {
            Self::Success => Ok(()),
            Self::Failed => bail!("clone command failed"),
        }
    }
}

fn run_clone_command_with_cancellation(
    mut command: Command,
    label: &str,
    lifecycle: &CloneLifecycle,
) -> Result<CloneCommandOutcome> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let signal_ids = register_clone_cancel_signals(Arc::clone(&cancelled))?;
    let mut child = command
        .spawn()
        .with_context(|| format!("starting {label}"))?;
    let outcome = loop {
        if cancelled.load(Ordering::Relaxed) {
            send_clone_cancelled(lifecycle, "client received termination signal");
            let _ = child.kill();
            let _ = child.wait();
            break Err(anyhow!("{label} cancelled by signal"));
        }
        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("waiting for {label}"))?
        {
            break Ok(if status.success() {
                CloneCommandOutcome::Success
            } else {
                CloneCommandOutcome::Failed
            });
        }
        thread::sleep(Duration::from_millis(100));
    };
    unregister_clone_cancel_signals(signal_ids);
    outcome
}

fn register_clone_cancel_signals(cancelled: Arc<AtomicBool>) -> Result<Vec<signal_hook::SigId>> {
    let signals = [
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
    ];
    signals
        .into_iter()
        .map(|signal| {
            signal_hook::flag::register(signal, Arc::clone(&cancelled))
                .with_context(|| format!("registering signal handler for {signal}"))
        })
        .collect()
}

fn unregister_clone_cancel_signals(signal_ids: Vec<signal_hook::SigId>) {
    for signal_id in signal_ids {
        signal_hook::low_level::unregister(signal_id);
    }
}

fn send_clone_cancelled(lifecycle: &CloneLifecycle, reason: &str) {
    send_rpc_event_best_effort(
        &lifecycle.rpc_url,
        &RpcEvent::Cancelled(CloneCancelledEvent {
            client_id: lifecycle.client_id.clone(),
            url: lifecycle.url.clone(),
            locator: lifecycle.locator.clone(),
            path: lifecycle.path.clone(),
            reason: reason.to_string(),
            scan_root: lifecycle.scan_root.clone(),
        }),
    );
}

fn fork_repo(
    config: &Config,
    db: &Store,
    output: &Output,
    fork_url: &str,
    canonical_url: &str,
) -> Result<()> {
    warn_pending_related(db)?;
    let fork_locator = Locator::parse(fork_url)?;
    let parent = ensure_type_change_parent(config, db, canonical_url)?;
    if parent.parent.current == fork_locator {
        bail!("repository cannot be its own parent repository");
    }
    if parent.storage.current == fork_locator {
        bail!("repository cannot depend on one of its own descendants");
    }
    let parent_locator = parent.parent.current.clone();
    let parent_path = parent.parent.path.clone();
    let canonical_locator = parent.storage.current.clone();
    let fork_path = locator_path(&config.clone_root, &fork_locator);
    let canonical_path = parent.storage.path.clone();
    let fork_remote = fork_remote_name(&fork_locator);
    fs::create_dir_all(fork_path.parent().context("fork path has no parent")?)?;
    if config.clone_as_bare {
        ensure_managed_canonical_is_bare(&parent.storage)?;
        let view = materialize_namespace_view(
            &fork_locator,
            &fork_path,
            &canonical_locator,
            &canonical_path,
            "fork",
            fork_url,
        )?;
        db.upsert_repo(&canonical_locator, &canonical_path, None)?;
        let fork_id = db.upsert_repo(&fork_locator, &fork_path, Some(&canonical_locator.key()))?;
        db.record_dependent_relationship(fork_id, parent.parent.id, "fork")?;
        return output_fork(
            output,
            &ForkResult {
                action: "fork",
                fork_locator,
                parent_locator,
                parent_path,
                canonical_locator,
                fork_path,
                canonical_path,
                fork_remote: "origin".to_string(),
                refs_prefix: Some(view.refs_prefix),
            },
        );
    }
    ensure_remote(&canonical_path, "origin", canonical_url)?;
    ensure_remote(&canonical_path, &fork_remote, fork_url)?;
    run_git_in(&canonical_path, ["fetch", &fork_remote])?;
    let status = git_command(&canonical_path)
        .args(["remote", "set-head", &fork_remote, "-a"])
        .status()
        .context("detecting fork default branch")?;
    if !status.success() {
        eprintln!("warning: could not determine fork default branch; using {fork_remote}/HEAD");
    }
    let fork_head = format!("{fork_remote}/HEAD");
    run_git_in(
        &canonical_path,
        [
            "worktree",
            "add",
            &fork_path.display().to_string(),
            &fork_head,
        ],
    )?;
    db.upsert_repo(&canonical_locator, &canonical_path, None)?;
    let fork_id = db.upsert_repo(&fork_locator, &fork_path, Some(&canonical_locator.key()))?;
    db.record_dependent_relationship(fork_id, parent.parent.id, "fork")?;
    output_fork(
        output,
        &ForkResult {
            action: "fork",
            fork_locator,
            parent_locator,
            parent_path,
            canonical_locator,
            fork_path,
            canonical_path,
            fork_remote,
            refs_prefix: None,
        },
    )
}

fn ensure_canonical_bare_repo(path: &Path, url: &str) -> Result<()> {
    fs::create_dir_all(path.parent().context("canonical path has no parent")?)?;
    if path.exists() {
        ensure_bare_repository(path)?;
        ensure_remote(path, "origin", url)?;
    } else {
        run_git_clone(url, path, true)?;
    }
    Ok(())
}

fn materialize_namespace_view(
    locator: &Locator,
    path: &Path,
    canonical_locator: &Locator,
    canonical_path: &Path,
    relationship: &str,
    origin_url: &str,
) -> Result<RepoViewMetadata> {
    ensure_bare_repository(canonical_path)?;
    let default_branch =
        remote_default_branch_from_url(origin_url)?.unwrap_or_else(|| "main".into());
    let refs_prefix = repo_view_refs_prefix(relationship, locator);
    let view = RepoViewMetadata {
        version: REPO_VIEW_METADATA_VERSION,
        relationship: relationship.to_string(),
        locator: locator.clone(),
        path: path.to_path_buf(),
        canonical_locator: canonical_locator.clone(),
        canonical_path: canonical_path.to_path_buf(),
        refs_prefix,
        origin_url: origin_url.to_string(),
        default_branch,
    };

    if path.exists() && read_repo_view_metadata(path)?.is_none() {
        let backup_path = unique_backup_path(path)?;
        fs::rename(path, &backup_path).with_context(|| {
            format!(
                "moving existing dependent checkout {} to {}",
                path.display(),
                backup_path.display()
            )
        })?;
    }
    fs::create_dir_all(path).with_context(|| format!("creating repo view {}", path.display()))?;
    write_repo_view_metadata(&view)?;
    fetch_repo_view(&view, "origin")?;
    ensure_repo_view_default_branch(&view)?;
    Ok(view)
}

fn repo_view_refs_prefix(relationship: &str, locator: &Locator) -> String {
    let plural = match relationship {
        "mirror" => "mirrors",
        _ => "forks",
    };
    format!(
        "refs/repo-manager/{plural}/{}",
        sanitize_remote_name(&locator.key())
    )
}

fn write_repo_view_metadata(view: &RepoViewMetadata) -> Result<()> {
    fs::create_dir_all(&view.path)
        .with_context(|| format!("creating repo view {}", view.path.display()))?;
    let path = view.path.join(REPO_VIEW_METADATA_FILE);
    let json = serde_json::to_string_pretty(view)?;
    fs::write(&path, json).with_context(|| format!("writing {}", path.display()))
}

fn read_repo_view_metadata(path: &Path) -> Result<Option<RepoViewMetadata>> {
    let metadata_path = path.join(REPO_VIEW_METADATA_FILE);
    if !metadata_path.exists() {
        return Ok(None);
    }
    let view: RepoViewMetadata = serde_json::from_str(
        &fs::read_to_string(&metadata_path)
            .with_context(|| format!("reading {}", metadata_path.display()))?,
    )
    .with_context(|| format!("parsing {}", metadata_path.display()))?;
    if view.version != REPO_VIEW_METADATA_VERSION {
        bail!(
            "unsupported repo-manager view version {} in {}",
            view.version,
            metadata_path.display()
        );
    }
    Ok(Some(view))
}

fn fetch_repo_view(view: &RepoViewMetadata, remote: &str) -> Result<()> {
    if remote != "origin" {
        bail!("repo-manager namespace views currently support only `origin`, got `{remote}`");
    }
    let heads_refspec = format!("+refs/heads/*:{}/remotes/origin/*", view.refs_prefix);
    let tags_refspec = format!("+refs/tags/*:{}/tags/*", view.refs_prefix);
    let status = git_dir_command(&view.canonical_path)
        .args(["fetch", "--prune", "--no-tags", &view.origin_url])
        .arg(heads_refspec)
        .arg(tags_refspec)
        .status()
        .with_context(|| {
            format!(
                "fetching {} into namespace {}",
                view.origin_url, view.refs_prefix
            )
        })?;
    if !status.success() {
        bail!("git fetch failed with status {status}");
    }
    Ok(())
}

fn ensure_repo_view_default_branch(view: &RepoViewMetadata) -> Result<()> {
    let remote_ref = format!(
        "{}/remotes/origin/{}",
        view.refs_prefix, view.default_branch
    );
    let local_ref = format!("{}/heads/{}", view.refs_prefix, view.default_branch);
    if git_dir_ref_exists(&view.canonical_path, &remote_ref)?
        && !git_dir_ref_exists(&view.canonical_path, &local_ref)?
    {
        let target = git_dir_output(
            &view.canonical_path,
            ["rev-parse", &remote_ref],
            "resolving repo view default branch",
        )?;
        git_dir_update_ref(&view.canonical_path, &local_ref, target.trim())?;
    }
    Ok(())
}

fn remote_default_branch_from_url(url: &str) -> Result<Option<String>> {
    let output = Command::new("git")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_PREFIX")
        .args(["ls-remote", "--symref", url, "HEAD"])
        .output()
        .with_context(|| format!("reading remote default branch for {url}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8(output.stdout).context("git ls-remote output is not UTF-8")?;
    Ok(stdout.lines().find_map(|line| {
        let rest = line.strip_prefix("ref: refs/heads/")?;
        let (branch, head) = rest.split_once('\t')?;
        (head == "HEAD").then(|| branch.to_string())
    }))
}

fn repo_context(config: &Config, db: &Store, dir: Option<&Path>) -> Result<RepoContext> {
    let dir = match dir {
        Some(dir) => dir.to_path_buf(),
        None => env::current_dir().context("reading current directory")?,
    };
    let dir = if dir.exists() {
        comparable_path(&dir)
    } else {
        dir
    };
    if let Some(view) = read_repo_view_metadata(&dir)? {
        return Ok(RepoContext::View(view));
    }
    if let Some(repo) = db.repo_by_path(&dir)? {
        return Ok(RepoContext::Managed(repo));
    }
    if path_is_under(&dir, &config.clone_root)
        && let Some(locator) = repo_locator_from_origin(&dir)?
        && let Some(repo) = db.find_repo(&locator.key())?
    {
        return Ok(RepoContext::Managed(ManagedRepoRecord {
            id: repo.id,
            current: repo.current,
            path: dir,
        }));
    }
    bail!(
        "could not resolve repo context for {}; pass --dir pointing at a managed repo or repo-manager view",
        dir.display()
    )
}

fn fetch_repo(
    config: &Config,
    db: &Store,
    output: &Output,
    dir: Option<&Path>,
    args: FetchArgs,
) -> Result<()> {
    let context = repo_context(config, db, dir)?;
    match context {
        RepoContext::View(view) => {
            fetch_repo_view(&view, &args.remote)?;
            ensure_repo_view_default_branch(&view)?;
            output_fetch(
                output,
                &FetchResult {
                    action: "fetch",
                    locator: view.locator,
                    path: view.path,
                    remote: args.remote,
                    refs_prefix: Some(view.refs_prefix),
                },
            )
        }
        RepoContext::Managed(repo) => {
            run_git_in(&repo.path, ["fetch", "--prune", &args.remote])?;
            output_fetch(
                output,
                &FetchResult {
                    action: "fetch",
                    locator: repo.current,
                    path: repo.path,
                    remote: args.remote,
                    refs_prefix: None,
                },
            )
        }
    }
}

fn branch_repo(
    config: &Config,
    db: &Store,
    output: &Output,
    dir: Option<&Path>,
    args: BranchArgs,
) -> Result<()> {
    let context = repo_context(config, db, dir)?;
    match context {
        RepoContext::View(view) => branch_repo_view(output, &view, args),
        RepoContext::Managed(repo) => branch_managed(output, &repo, args),
    }
}

fn branch_repo_view(output: &Output, view: &RepoViewMetadata, args: BranchArgs) -> Result<()> {
    let created = if let Some(branch) = args.branch {
        let start_ref = resolve_repo_view_start_ref(view, args.start_point.as_deref())?;
        let target = git_dir_output(
            &view.canonical_path,
            ["rev-parse", &start_ref],
            "resolving branch start point",
        )?;
        let branch_ref = format!("{}/heads/{branch}", view.refs_prefix);
        git_dir_update_ref(&view.canonical_path, &branch_ref, target.trim())?;
        Some(branch)
    } else {
        None
    };
    let branches = repo_view_branches(view)?;
    output_branch(
        output,
        &BranchResult {
            action: "branch",
            locator: view.locator.clone(),
            path: view.path.clone(),
            refs_prefix: Some(view.refs_prefix.clone()),
            branches,
            created,
        },
    )
}

fn branch_managed(output: &Output, repo: &ManagedRepoRecord, args: BranchArgs) -> Result<()> {
    let created = if let Some(branch) = args.branch {
        let mut command = git_command(&repo.path);
        command.arg("branch").arg(&branch);
        if let Some(start_point) = args.start_point {
            command.arg(start_point);
        }
        let status = command
            .status()
            .with_context(|| format!("creating branch in {}", repo.path.display()))?;
        if !status.success() {
            bail!("git branch failed with status {status}");
        }
        Some(branch)
    } else {
        None
    };
    let branches = git_lines(
        &repo.path,
        ["branch", "--list", "--format=%(refname:short)"],
        "listing branches",
    )?;
    output_branch(
        output,
        &BranchResult {
            action: "branch",
            locator: repo.current.clone(),
            path: repo.path.clone(),
            refs_prefix: None,
            branches,
            created,
        },
    )
}

fn repo_view_branches(view: &RepoViewMetadata) -> Result<Vec<String>> {
    let prefix = format!("{}/heads", view.refs_prefix);
    let output = git_dir_output(
        &view.canonical_path,
        ["for-each-ref", "--format=%(refname)", &prefix],
        "listing repo view branches",
    )?;
    Ok(output
        .lines()
        .filter_map(|line| line.strip_prefix(&(prefix.clone() + "/")))
        .map(str::to_string)
        .collect())
}

fn resolve_repo_view_start_ref(
    view: &RepoViewMetadata,
    start_point: Option<&str>,
) -> Result<String> {
    let candidates = match start_point {
        Some(start_point) => {
            let remote_branch = start_point.strip_prefix("origin/").unwrap_or(start_point);
            vec![
                format!("{}/heads/{start_point}", view.refs_prefix),
                format!("{}/remotes/origin/{remote_branch}", view.refs_prefix),
                start_point.to_string(),
            ]
        }
        None => vec![
            format!("{}/heads/{}", view.refs_prefix, view.default_branch),
            format!(
                "{}/remotes/origin/{}",
                view.refs_prefix, view.default_branch
            ),
        ],
    };
    for candidate in candidates {
        if git_dir_command(&view.canonical_path)
            .args(["rev-parse", "--verify", &candidate])
            .output()
            .with_context(|| format!("checking start point {candidate}"))?
            .status
            .success()
        {
            return Ok(candidate);
        }
    }
    bail!("could not resolve repo view start point")
}

fn dump_check(config: &Config, db: &Store) -> Result<()> {
    print_json(&check_dump_report(config, db)?)
}

fn check_dump_report(config: &Config, db: &Store) -> Result<CheckDump> {
    let background_fetch = db.background_fetch_state_by_repo_id()?;
    let current_repos = managed_repository_dumps(config, db)?;
    let mut tracked_by_path = HashMap::new();
    let mut repository_by_db_id = HashMap::new();
    let mut tracked_repositories = Vec::new();

    for repo in &current_repos {
        let link = GitDirectoryRepositoryLink {
            id: repo.id.clone(),
            repo_type: repo.repo_type.clone(),
            locator: repo.locator.clone(),
            namespace: repo_namespace(&repo.path)?,
        };
        tracked_by_path.insert(
            comparable_path(&repo.path),
            GitDirectoryLink {
                id: Some(repo.id.clone()),
                tracked: true,
                managed: true,
                worktree_name: None,
                repository: Some(link.clone()),
            },
        );
        repository_by_db_id.insert(repo.db_id, link);
    }

    let mut managed_worktrees_by_path = HashMap::new();
    for mut worktree in db.managed_worktrees()? {
        if let Some(repository) = repository_by_db_id.get(&worktree.repo_id) {
            worktree.repo_type = repository.repo_type.clone();
            managed_worktrees_by_path.insert(
                comparable_path(&worktree.path),
                GitDirectoryLink {
                    id: Some(repository_path_id(config, &worktree.path)),
                    tracked: true,
                    managed: true,
                    worktree_name: worktree.name.clone(),
                    repository: Some(GitDirectoryRepositoryLink {
                        id: repository.id.clone(),
                        repo_type: repository.repo_type.clone(),
                        locator: worktree.repo_locator.clone(),
                        namespace: worktree
                            .refs_prefix
                            .clone()
                            .or_else(|| repository.namespace.clone()),
                    }),
                },
            );
        }
    }

    for repo in current_repos {
        let exists = repo.path.exists();
        let checkout_kind = if exists {
            git_directory_kind(&repo.path).unwrap_or_else(|_| "unknown".to_string())
        } else {
            "missing".to_string()
        };
        tracked_repositories.push(TrackedRepositoryDump {
            id: repo.id,
            repo_type: repo.repo_type,
            fork_depth: repo.fork_depth,
            locator: repo.locator,
            path: repo.path,
            exists,
            checkout_kind,
            parent: repo.parent,
            canonical: repo.canonical,
            dependents: repo.dependents,
            background_fetch: background_fetch.get(&repo.db_id).cloned(),
        });
    }

    let linked_worktree_links =
        linked_worktree_directory_links(&tracked_by_path, &managed_worktrees_by_path)?;
    let mut git_directory_paths = discover_git_repositories(&config.clone_root)?;
    git_directory_paths.extend(discover_git_repositories(&config.dev_worktree_root)?);
    git_directory_paths.extend(linked_worktree_links.keys().cloned());
    git_directory_paths.sort();
    git_directory_paths.dedup_by(|left, right| comparable_path(left) == comparable_path(right));

    let git_directories = git_directory_paths
        .into_iter()
        .map(|path| {
            git_directory_dump(
                config,
                path,
                &tracked_by_path,
                &managed_worktrees_by_path,
                &linked_worktree_links,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CheckDump {
        action: "check-dump",
        generated_at_epoch_seconds: epoch_seconds()?,
        roots: CheckDumpRoots {
            clone_root: config.clone_root.clone(),
            dev_worktree_root: config.dev_worktree_root.clone(),
        },
        tracked_repositories,
        git_directories,
    })
}

fn linked_worktree_directory_links(
    tracked_by_path: &HashMap<PathBuf, GitDirectoryLink>,
    managed_worktrees_by_path: &HashMap<PathBuf, GitDirectoryLink>,
) -> Result<HashMap<PathBuf, GitDirectoryLink>> {
    let mut links = HashMap::new();
    for (path, link) in tracked_by_path {
        let Some(repository) = &link.repository else {
            continue;
        };
        if !path.exists() {
            continue;
        }
        let Ok(worktrees) = linked_worktree_paths(path) else {
            continue;
        };
        for worktree_path in worktrees {
            let comparable = comparable_path(&worktree_path);
            if tracked_by_path.contains_key(&comparable) {
                continue;
            }
            if let Some(managed) = managed_worktrees_by_path.get(&comparable) {
                links.insert(comparable, managed.clone());
            } else {
                links.insert(
                    comparable,
                    GitDirectoryLink {
                        id: None,
                        tracked: false,
                        managed: false,
                        worktree_name: None,
                        repository: Some(repository.clone()),
                    },
                );
            }
        }
    }
    Ok(links)
}

fn git_directory_dump(
    config: &Config,
    path: PathBuf,
    tracked_by_path: &HashMap<PathBuf, GitDirectoryLink>,
    managed_worktrees_by_path: &HashMap<PathBuf, GitDirectoryLink>,
    linked_worktree_links: &HashMap<PathBuf, GitDirectoryLink>,
) -> Result<GitDirectoryDump> {
    let comparable = comparable_path(&path);
    let link = tracked_by_path
        .get(&comparable)
        .or_else(|| managed_worktrees_by_path.get(&comparable))
        .or_else(|| linked_worktree_links.get(&comparable));
    let view = read_repo_view_metadata(&path)?;
    let locator = match (link.and_then(|link| link.repository.as_ref()), &view) {
        (Some(repository), _) => Some(repository.locator.clone()),
        (None, Some(view)) => Some(view.locator.clone()),
        (None, None) => repo_locator_from_origin(&path)?,
    };
    let (kind, error) = match git_directory_kind(&path) {
        Ok(kind) => (Some(kind), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let repository = link.and_then(|link| link.repository.as_ref());
    Ok(GitDirectoryDump {
        id: link.and_then(|link| link.id.clone()).or_else(|| {
            path_is_under(&path, &config.root).then(|| repository_path_id(config, &path))
        }),
        path,
        kind,
        tracked: link.is_some_and(|link| link.tracked),
        managed: link.is_some_and(|link| link.managed),
        worktree_name: link.and_then(|link| link.worktree_name.clone()),
        repository: repository.map(|repo| repo.id.clone()),
        repository_type: repository.map(|repo| repo.repo_type.clone()),
        namespace: repository.and_then(|repo| repo.namespace.clone()),
        locator,
        error,
    })
}

fn repo_namespace(path: &Path) -> Result<Option<String>> {
    Ok(read_repo_view_metadata(path)?.map(|view| view.refs_prefix))
}

fn git_directory_kind(path: &Path) -> Result<String> {
    if read_repo_view_metadata(path)?.is_some() {
        return Ok("repo-manager-view".to_string());
    }
    if is_bare_repository(path)? {
        return Ok("bare".to_string());
    }
    if is_git_worktree(path)? {
        return Ok("worktree".to_string());
    }
    Ok("non-bare".to_string())
}

fn is_git_worktree(path: &Path) -> Result<bool> {
    if !path.join(".git").is_file() {
        return Ok(false);
    }
    let git_dir = git_dir(path)?;
    let common_dir = git_common_dir(path)?;
    Ok(comparable_path(&git_dir) != comparable_path(&common_dir))
}

fn managed_repository_dumps(config: &Config, db: &Store) -> Result<Vec<ManagedRepositoryDump>> {
    let repos = db.current_repos()?;
    let relationships = db.shared_git_dir_relationships()?;
    let summaries = relationship_summaries(&relationships)?;
    let mut dependents_by_parent: HashMap<i64, Vec<&SharedGitDirRelationship>> = HashMap::new();
    for relationship in &relationships {
        dependents_by_parent
            .entry(relationship.controlling_repo_id)
            .or_default()
            .push(relationship);
    }

    Ok(repos
        .into_iter()
        .map(|repo| {
            let summary = summaries.get(&repo.id);
            let parent = summary.map(|summary| {
                relationship_endpoint(
                    config,
                    summary.parent_locator.clone(),
                    summary.parent_path.clone(),
                    summary.relationship.clone(),
                )
            });
            let canonical = summary.map(|summary| {
                relationship_endpoint(
                    config,
                    summary.canonical_locator.clone(),
                    summary.canonical_path.clone(),
                    summary.relationship.clone(),
                )
            });
            let repo_type = summary
                .map(|summary| summary.relationship.clone())
                .unwrap_or_else(|| "canonical".to_string());
            let dependents = dependents_by_parent
                .get(&repo.id)
                .into_iter()
                .flat_map(|relationships| relationships.iter())
                .map(|relationship| {
                    relationship_endpoint(
                        config,
                        relationship.dependent_locator.clone(),
                        relationship.dependent_path.clone(),
                        relationship.relationship.clone(),
                    )
                })
                .collect();
            ManagedRepositoryDump {
                id: repository_path_id(config, &repo.path),
                db_id: repo.id,
                repo_type,
                fork_depth: summary.map(|summary| summary.depth).unwrap_or(0),
                locator: repo.current,
                checkout_kind: if repo.path.exists() {
                    git_directory_kind(&repo.path).unwrap_or_else(|_| "unknown".to_string())
                } else {
                    "missing".to_string()
                },
                path: repo.path,
                parent,
                canonical,
                dependents,
            }
        })
        .collect())
}

fn relationship_summaries(
    relationships: &[SharedGitDirRelationship],
) -> Result<HashMap<i64, RepositoryRelationshipSummary>> {
    let mut direct_by_dependent = HashMap::new();
    for relationship in relationships {
        if direct_by_dependent
            .insert(relationship.dependent_repo_id, relationship)
            .is_some()
        {
            bail!(
                "repository has multiple fork/mirror parents: {}",
                relationship.dependent_locator.key()
            );
        }
    }

    let mut summaries = HashMap::new();
    for relationship in relationships {
        let mut seen = HashSet::new();
        seen.insert(relationship.dependent_repo_id);
        let mut depth = 1;
        let mut canonical_repo_id = relationship.controlling_repo_id;
        let mut canonical_locator = relationship.controlling_locator.clone();
        let mut canonical_path = relationship.controlling_path.clone();

        while let Some(parent_relationship) = direct_by_dependent.get(&canonical_repo_id) {
            if !seen.insert(canonical_repo_id) {
                bail!(
                    "fork/mirror relationship cycle involving {}",
                    parent_relationship.dependent_locator.key()
                );
            }
            depth += 1;
            canonical_repo_id = parent_relationship.controlling_repo_id;
            canonical_locator = parent_relationship.controlling_locator.clone();
            canonical_path = parent_relationship.controlling_path.clone();
        }

        summaries.insert(
            relationship.dependent_repo_id,
            RepositoryRelationshipSummary {
                relationship: relationship.relationship.clone(),
                parent_locator: relationship.controlling_locator.clone(),
                parent_path: relationship.controlling_path.clone(),
                canonical_repo_id,
                canonical_locator,
                canonical_path,
                depth,
            },
        );
    }
    Ok(summaries)
}

fn relationship_endpoint(
    config: &Config,
    locator: Locator,
    path: PathBuf,
    relationship: String,
) -> RepositoryRelationEndpoint {
    RepositoryRelationEndpoint {
        id: repository_path_id(config, &path),
        locator,
        path,
        relationship,
    }
}

fn repository_path_id(config: &Config, path: &Path) -> String {
    path.strip_prefix(&config.root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn managed_repo_by_path_id(
    config: &Config,
    db: &Store,
    repo_path: &Path,
) -> Result<ManagedRepoRecord> {
    let expected_path = if repo_path.is_absolute() {
        repo_path.to_path_buf()
    } else {
        config.root.join(repo_path)
    };
    db.current_repos()?
        .into_iter()
        .find(|repo| comparable_path(&repo.path) == comparable_path(&expected_path))
        .ok_or_else(|| anyhow!("unknown managed repository path: {}", repo_path.display()))
}

fn repos_set_type(
    config: &Config,
    db: &Store,
    output: &Output,
    args: RepoSetTypeArgs,
) -> Result<()> {
    let repo_type = args.repo_type.trim().to_ascii_lowercase();
    if !matches!(repo_type.as_str(), "canonical" | "fork" | "mirror") {
        bail!(
            "invalid repository type: {}; expected canonical, fork, or mirror",
            args.repo_type
        );
    }
    let repo = managed_repo_by_path_id(config, db, &args.repo_path)?;
    let before = managed_repository_dumps(config, db)?
        .into_iter()
        .find(|entry| comparable_path(&entry.path) == comparable_path(&repo.path))
        .ok_or_else(|| anyhow!("managed repository disappeared while changing type"))?;
    db.ensure_no_dependents(repo.id)?;

    let shared_git_dir = match repo_type.as_str() {
        "canonical" => {
            if read_repo_view_metadata(&repo.path)?.is_some() {
                bail!(
                    "cannot mark repo-manager namespace view as canonical without a canonical checkout: {}",
                    repo.path.display()
                );
            }
            if args.canonical.is_some() {
                bail!("--canonical is only valid when setting type to fork or mirror");
            }
            db.clear_dependent_relationships(repo.id)?;
            None
        }
        "fork" | "mirror" => {
            let canonical_ref = args.canonical.as_deref().ok_or_else(|| {
                anyhow!("--canonical is required when setting type to {repo_type}")
            })?;
            let parent = ensure_type_change_parent(config, db, canonical_ref)?;
            if parent.parent.id == repo.id {
                bail!("repository cannot be its own parent repository");
            }
            if parent.storage.id == repo.id {
                bail!("repository cannot depend on one of its own descendants");
            }
            db.clear_dependent_relationships(repo.id)?;
            let resolution = materialize_related_shared_git_dir(
                db,
                &repo.current,
                &repo.path,
                &parent.storage.current,
                &parent.storage.path,
                &repo_type,
                true,
            )?;
            if parent.parent.id != parent.storage.id {
                let repo_id = db.upsert_repo(
                    &repo.current,
                    &repo.path,
                    Some(&parent.storage.current.key()),
                )?;
                db.clear_dependent_relationships(repo_id)?;
                db.record_dependent_relationship(repo_id, parent.parent.id, &repo_type)?;
            }
            Some(resolution)
        }
        _ => unreachable!(),
    };

    let after_repo = managed_repo_by_path_id(config, db, &args.repo_path).or_else(|_| {
        db.repo_by_id(repo.id)?
            .ok_or_else(|| anyhow!("repository disappeared"))
    })?;
    let after = managed_repository_dumps(config, db)?
        .into_iter()
        .find(|entry| comparable_path(&entry.path) == comparable_path(&after_repo.path))
        .ok_or_else(|| anyhow!("managed repository disappeared after changing type"))?;
    let result = RepoTypeChangeResult {
        action: "repo-set-type",
        id: after.id.clone(),
        previous_type: before.repo_type,
        new_type: after.repo_type.clone(),
        repository: after,
        shared_git_dir,
    };
    output_repo_type_change(output, &result)
}

fn ensure_type_change_parent(
    config: &Config,
    db: &Store,
    parent_ref: &str,
) -> Result<RelationshipParent> {
    if let Ok(repo) = managed_repo_by_path_id(config, db, Path::new(parent_ref)) {
        return resolve_relationship_parent_storage(db, repo);
    }
    if let Ok(Some(record)) = db.find_repo(parent_ref)
        && let Some(repo) = db.repo_by_id(record.id)?
    {
        return resolve_relationship_parent_storage(db, repo);
    }
    if let Some(path) = existing_path_reference(config, parent_ref) {
        if let Some(view) = read_repo_view_metadata(&path)? {
            let repo_id = db.upsert_repo(
                &view.locator,
                &view.path,
                Some(&view.canonical_locator.key()),
            )?;
            let parent = ManagedRepoRecord {
                id: repo_id,
                current: view.locator,
                path: view.path,
            };
            let storage_id = db.upsert_repo(&view.canonical_locator, &view.canonical_path, None)?;
            let storage = ManagedRepoRecord {
                id: storage_id,
                current: view.canonical_locator,
                path: view.canonical_path,
            };
            ensure_managed_canonical_is_bare(&storage)?;
            return Ok(RelationshipParent { parent, storage });
        }
        if !is_git_repository_path(&path) {
            bail!("parent path is not a Git repository: {}", path.display());
        }
        let locator = repo_locator_from_origin(&path)?.ok_or_else(|| {
            anyhow!(
                "parent path has no locator-compatible origin remote: {}",
                path.display()
            )
        })?;
        let repo_id = db.upsert_repo(&locator, &path, None)?;
        let repo = ManagedRepoRecord {
            id: repo_id,
            current: locator,
            path,
        };
        return resolve_relationship_parent_storage(db, repo);
    }

    let locator = Locator::parse(parent_ref)?;
    let path = locator_path(&config.clone_root, &locator);
    let url = if parent_ref.contains("://") || parse_scp_like(parent_ref).is_some() {
        parent_ref.to_string()
    } else {
        remote_url_for_locator(None, &locator)
    };
    ensure_canonical_bare_repo(&path, &url)?;
    let repo_id = db.upsert_repo(&locator, &path, None)?;
    let parent = ManagedRepoRecord {
        id: repo_id,
        current: locator,
        path,
    };
    Ok(RelationshipParent {
        parent: parent.clone(),
        storage: parent,
    })
}

fn resolve_relationship_parent_storage(
    db: &Store,
    parent: ManagedRepoRecord,
) -> Result<RelationshipParent> {
    if let Some(view) = read_repo_view_metadata(&parent.path)? {
        let storage_id = db.upsert_repo(&view.canonical_locator, &view.canonical_path, None)?;
        let storage = ManagedRepoRecord {
            id: storage_id,
            current: view.canonical_locator,
            path: view.canonical_path,
        };
        ensure_managed_canonical_is_bare(&storage)?;
        return Ok(RelationshipParent { parent, storage });
    }

    let summaries = relationship_summaries(&db.shared_git_dir_relationships()?)?;
    if let Some(summary) = summaries.get(&parent.id) {
        let storage = ManagedRepoRecord {
            id: db
                .find_repo(&summary.canonical_locator.key())?
                .map(|record| record.id)
                .unwrap_or(summary.canonical_repo_id),
            current: summary.canonical_locator.clone(),
            path: summary.canonical_path.clone(),
        };
        ensure_managed_canonical_is_bare(&storage)?;
        return Ok(RelationshipParent { parent, storage });
    }

    ensure_managed_canonical_is_bare(&parent)?;
    Ok(RelationshipParent {
        parent: parent.clone(),
        storage: parent,
    })
}

fn existing_path_reference(config: &Config, value: &str) -> Option<PathBuf> {
    let path = Path::new(value);
    if path.is_absolute() {
        return path.exists().then(|| path.to_path_buf());
    }
    let path = config.root.join(path);
    path.exists().then_some(path)
}

fn ensure_managed_canonical_is_bare(repo: &ManagedRepoRecord) -> Result<()> {
    if read_repo_view_metadata(&repo.path)?.is_some() {
        bail!(
            "canonical repository cannot be a repo-manager namespace view: {}",
            repo.path.display()
        );
    }
    if repo.path.exists() && !is_bare_repository(&repo.path)? {
        convert_standalone_checkout_to_bare_repository(&repo.path)?;
    }
    ensure_bare_repository(&repo.path)
}

fn repair_repos(config: &Config, db: &Store, output: &Output, check: bool) -> Result<()> {
    let report = build_repair_report(config, db, check)?;
    output_repair(output, &report)?;
    if check && repair_report_needs_repair(&report) {
        bail!("repo check found repairable issues; run `repo check --repair`");
    }
    Ok(())
}

fn repair_repos_interactive(config: &Config, db: &Store, output: &Output) -> Result<()> {
    let check_report = build_repair_report(config, db, true)?;
    if !repair_report_needs_repair(&check_report) {
        output_repair(output, &check_report)?;
        return Ok(());
    }

    let operations = repair_operations_from_report(&check_report);
    let plan_path = write_repair_plan_file(&operations)?;
    open_repair_plan_in_editor(&plan_path)?;
    let selected = parse_repair_plan_file(&plan_path, &operations)?;
    let _ = fs::remove_file(&plan_path);

    println!(
        "selected {} repair operation(s), skipped {}",
        selected.len(),
        operations.len().saturating_sub(selected.len())
    );
    if selected.is_empty() {
        return Ok(());
    }

    let report = apply_repair_operations(config, db, &operations, &selected)?;
    output_repair(output, &report)
}

#[derive(Debug, Clone)]
struct RepairOperation {
    id: usize,
    summary: String,
    kind: RepairOperationKind,
}

#[derive(Debug, Clone)]
enum RepairOperationKind {
    PruneStale(RepairStalePath),
    ConvertRepositoryFormat(RepairRepositoryFormat),
    TrackUnmanagedCheckout(RepairUntrackedCheckout),
    RepairRelationship(Box<RepairRelationship>),
}

fn repair_operations_from_report(report: &RepairReport) -> Vec<RepairOperation> {
    let mut operations = Vec::new();
    let mut next_id = 1;

    for stale in &report.stale_paths {
        if !matches!(stale.status, RepairStalePathStatus::NeedsPrune) {
            continue;
        }
        operations.push(RepairOperation {
            id: next_id,
            summary: format!("prune stale managed path {}", stale.locator.key()),
            kind: RepairOperationKind::PruneStale(stale.clone()),
        });
        next_id += 1;
    }

    let mut format_groups: BTreeMap<PathBuf, Vec<&RepairRepositoryFormat>> = BTreeMap::new();
    for format in &report.repository_formats {
        if matches!(
            format.status,
            RepairRepositoryFormatStatus::NeedsBareConversion
        ) {
            format_groups
                .entry(format.path.clone())
                .or_default()
                .push(format);
        }
    }
    for (path, formats) in format_groups {
        let Some(first) = formats.first() else {
            continue;
        };
        operations.push(RepairOperation {
            id: next_id,
            summary: format!("convert clone-root repository to bare {}", path.display()),
            kind: RepairOperationKind::ConvertRepositoryFormat((*first).clone()),
        });
        next_id += 1;
    }

    for checkout in &report.untracked_checkouts {
        if !matches!(
            checkout.status,
            RepairUntrackedCheckoutStatus::NeedsTracking
        ) {
            continue;
        }
        let Some(locator) = &checkout.locator else {
            continue;
        };
        operations.push(RepairOperation {
            id: next_id,
            summary: format!("track unmanaged clone-root checkout {}", locator.key()),
            kind: RepairOperationKind::TrackUnmanagedCheckout(checkout.clone()),
        });
        next_id += 1;
    }

    for relationship in &report.relationships {
        if !matches!(relationship.status, RepairStatus::NeedsRepair) {
            continue;
        }
        operations.push(RepairOperation {
            id: next_id,
            summary: format!(
                "repair {} {} -> {}",
                relationship.relationship,
                relationship.dependent_locator.key(),
                relationship.controlling_locator.key()
            ),
            kind: RepairOperationKind::RepairRelationship(Box::new(relationship.clone())),
        });
        next_id += 1;
    }

    operations
}

fn write_repair_plan_file(operations: &[RepairOperation]) -> Result<PathBuf> {
    let plan_path = env::temp_dir().join(format!(
        "repo-manager-repair-plan-{}.txt",
        std::process::id()
    ));
    let text = format_repair_plan(operations);
    fs::write(&plan_path, text)
        .with_context(|| format!("writing repair plan {}", plan_path.display()))?;
    Ok(plan_path)
}

fn format_repair_plan(operations: &[RepairOperation]) -> String {
    let mut text = String::new();
    writeln!(text, "# repo-manager repair plan").unwrap();
    writeln!(text, "#").unwrap();
    writeln!(
        text,
        "# Leave `pick` to apply an operation. Change `pick` to `drop`,"
    )
    .unwrap();
    writeln!(
        text,
        "# or delete an operation line, to skip that operation."
    )
    .unwrap();
    writeln!(text, "# Lines beginning with `#` are ignored.").unwrap();
    writeln!(text).unwrap();
    for operation in operations {
        writeln!(text, "pick {} {}", operation.id, operation.summary).unwrap();
    }
    text
}

fn open_repair_plan_in_editor(plan_path: &Path) -> Result<()> {
    let editor = env::var("VISUAL")
        .or_else(|_| env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string());
    let mut parts = editor.split_whitespace();
    let program = parts.next().filter(|part| !part.is_empty()).unwrap_or("vi");
    let mut command = Command::new(program);
    command.args(parts);
    command.arg(plan_path);
    let status = command
        .status()
        .with_context(|| format!("opening repair plan with editor `{editor}`"))?;
    if !status.success() {
        bail!("repair plan editor exited with status {status}");
    }
    Ok(())
}

fn parse_repair_plan_file(
    plan_path: &Path,
    operations: &[RepairOperation],
) -> Result<HashSet<usize>> {
    let contents = fs::read_to_string(plan_path)
        .with_context(|| format!("reading edited repair plan {}", plan_path.display()))?;
    parse_repair_plan(&contents, operations)
}

fn parse_repair_plan(contents: &str, operations: &[RepairOperation]) -> Result<HashSet<usize>> {
    let valid_ids = operations
        .iter()
        .map(|operation| operation.id)
        .collect::<HashSet<_>>();
    let mut selected = HashSet::new();
    let mut seen = HashSet::new();
    for (line_idx, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let command = parts
            .next()
            .ok_or_else(|| anyhow!("empty repair plan line {}", line_idx + 1))?;
        let id_text = parts.next().ok_or_else(|| {
            anyhow!(
                "repair plan line {} is missing an operation id",
                line_idx + 1
            )
        })?;
        let id = id_text.parse::<usize>().with_context(|| {
            format!(
                "repair plan line {} has invalid operation id `{id_text}`",
                line_idx + 1
            )
        })?;
        if !valid_ids.contains(&id) {
            bail!(
                "repair plan line {} references unknown operation id {id}",
                line_idx + 1
            );
        }
        if !seen.insert(id) {
            bail!(
                "repair plan line {} repeats operation id {id}",
                line_idx + 1
            );
        }
        match command {
            "pick" | "p" | "apply" => {
                selected.insert(id);
            }
            "drop" | "d" | "skip" | "s" => {}
            _ => bail!(
                "repair plan line {} has unknown command `{command}`; use `pick` or `drop`",
                line_idx + 1
            ),
        }
    }
    Ok(selected)
}

fn apply_repair_operations(
    config: &Config,
    db: &Store,
    operations: &[RepairOperation],
    selected: &HashSet<usize>,
) -> Result<RepairReport> {
    let mut report = RepairReport {
        action: "check",
        check: false,
        repository_formats: Vec::new(),
        stale_paths: Vec::new(),
        untracked_checkouts: Vec::new(),
        relationships: Vec::new(),
        skipped: Vec::new(),
    };

    for operation in operations {
        if !selected.contains(&operation.id) {
            continue;
        }
        match &operation.kind {
            RepairOperationKind::PruneStale(stale) => {
                db.delete_repo(stale.repo_id)?;
                report.stale_paths.push(RepairStalePath {
                    repo_id: stale.repo_id,
                    locator: stale.locator.clone(),
                    path: stale.path.clone(),
                    status: RepairStalePathStatus::Pruned,
                    reasons: vec!["removed stale managed checkout metadata".to_string()],
                    blocking_dependents: Vec::new(),
                });
            }
            RepairOperationKind::ConvertRepositoryFormat(format) => {
                let repo = ManagedRepoRecord {
                    id: format.repo_id,
                    current: format.locator.clone(),
                    path: format.path.clone(),
                };
                report
                    .repository_formats
                    .push(convert_repo_format_to_bare(&repo));
            }
            RepairOperationKind::TrackUnmanagedCheckout(checkout) => {
                if let Some(locator) = &checkout.locator {
                    db.upsert_repo(locator, &checkout.path, None)?;
                    report.untracked_checkouts.push(RepairUntrackedCheckout {
                        locator: Some(locator.clone()),
                        path: checkout.path.clone(),
                        status: RepairUntrackedCheckoutStatus::Tracked,
                        reasons: vec![
                            "recorded previously unmanaged clone-root checkout".to_string(),
                        ],
                    });
                } else {
                    report.untracked_checkouts.push(RepairUntrackedCheckout {
                        locator: None,
                        path: checkout.path.clone(),
                        status: RepairUntrackedCheckoutStatus::Skipped,
                        reasons: vec![
                            "origin remote is missing or is not a parseable repository locator"
                                .to_string(),
                        ],
                    });
                }
            }
            RepairOperationKind::RepairRelationship(relationship) => {
                match materialize_related_shared_git_dir(
                    db,
                    &relationship.dependent_locator,
                    &relationship.dependent_path,
                    &relationship.controlling_locator,
                    &relationship.controlling_path,
                    &relationship.relationship,
                    config.clone_as_bare,
                ) {
                    Ok(shared_git_dir) => report.relationships.push(RepairRelationship {
                        relationship: relationship.relationship.clone(),
                        dependent_locator: relationship.dependent_locator.clone(),
                        controlling_locator: relationship.controlling_locator.clone(),
                        dependent_path: relationship.dependent_path.clone(),
                        controlling_path: relationship.controlling_path.clone(),
                        status: RepairStatus::Repaired,
                        reasons: relationship.reasons.clone(),
                        shared_git_dir: Some(shared_git_dir),
                    }),
                    Err(error) => report.skipped.push(RepairSkip {
                        relationship: relationship.relationship.clone(),
                        dependent_locator: relationship.dependent_locator.clone(),
                        controlling_locator: relationship.controlling_locator.clone(),
                        reason: error.to_string(),
                    }),
                }
            }
        }
    }

    Ok(report)
}

fn build_repair_report(config: &Config, db: &Store, check: bool) -> Result<RepairReport> {
    let current_repos = db.current_repos()?;
    let mut current_by_locator = HashMap::new();
    let mut current_paths = HashSet::new();
    for repo in &current_repos {
        current_by_locator.insert(repo.current.key(), repo.clone());
        current_paths.insert(comparable_path(&repo.path));
    }

    let mut untracked_checkouts = Vec::new();
    let mut relocated_locator_keys = HashSet::new();
    for checkout_path in discover_git_repositories(&config.clone_root)? {
        if current_paths.contains(&comparable_path(&checkout_path)) {
            continue;
        }

        let Some(locator) = repo_locator_from_origin(&checkout_path)? else {
            untracked_checkouts.push(RepairUntrackedCheckout {
                locator: None,
                path: checkout_path,
                status: RepairUntrackedCheckoutStatus::Skipped,
                reasons: vec![
                    "origin remote is missing or is not a parseable repository locator".to_string(),
                ],
            });
            continue;
        };

        let locator_key = locator.key();
        if let Some(current) = current_by_locator.get(&locator_key) {
            if current.path.exists() {
                untracked_checkouts.push(RepairUntrackedCheckout {
                    locator: Some(locator),
                    path: checkout_path,
                    status: RepairUntrackedCheckoutStatus::Skipped,
                    reasons: vec![format!(
                        "locator is already tracked at existing path {}",
                        current.path.display()
                    )],
                });
                continue;
            }

            relocated_locator_keys.insert(locator_key);
            if check {
                untracked_checkouts.push(RepairUntrackedCheckout {
                    locator: Some(locator),
                    path: checkout_path,
                    status: RepairUntrackedCheckoutStatus::NeedsTracking,
                    reasons: vec![format!(
                        "matches stale managed locator previously recorded at {}",
                        current.path.display()
                    )],
                });
            } else {
                db.upsert_repo(&locator, &checkout_path, None)?;
                untracked_checkouts.push(RepairUntrackedCheckout {
                    locator: Some(locator),
                    path: checkout_path,
                    status: RepairUntrackedCheckoutStatus::Tracked,
                    reasons: vec![format!(
                        "updated stale managed locator previously recorded at {}",
                        current.path.display()
                    )],
                });
            }
            continue;
        }

        if check {
            untracked_checkouts.push(RepairUntrackedCheckout {
                locator: Some(locator),
                path: checkout_path,
                status: RepairUntrackedCheckoutStatus::NeedsTracking,
                reasons: vec![
                    "checkout is under the managed clone root but absent from metadata".to_string(),
                ],
            });
        } else {
            db.upsert_repo(&locator, &checkout_path, None)?;
            untracked_checkouts.push(RepairUntrackedCheckout {
                locator: Some(locator),
                path: checkout_path,
                status: RepairUntrackedCheckoutStatus::Tracked,
                reasons: vec!["recorded previously unmanaged clone-root checkout".to_string()],
            });
        }
    }

    let relationship_snapshot = db.shared_git_dir_relationships()?;
    let relationship_dependent_repo_ids = relationship_snapshot
        .iter()
        .map(|relationship| relationship.dependent_repo_id)
        .collect::<HashSet<_>>();
    let mut repository_formats = Vec::new();
    if config.clone_as_bare {
        for repo in &current_repos {
            if !repo.path.exists() || !path_is_under(&repo.path, &config.clone_root) {
                continue;
            }
            if read_repo_view_metadata(&repo.path)?.is_some() {
                continue;
            }
            match is_bare_repository(&repo.path) {
                Ok(true) => {}
                Ok(false) => {
                    if relationship_dependent_repo_ids.contains(&repo.id)
                        && checkout_uses_shared_git_dir(&repo.path)
                    {
                        continue;
                    }
                    if check {
                        repository_formats.push(bare_conversion_needed(repo));
                    } else {
                        repository_formats.push(convert_repo_format_to_bare(repo));
                    }
                }
                Err(error) => repository_formats.push(RepairRepositoryFormat {
                    repo_id: repo.id,
                    locator: repo.current.clone(),
                    path: repo.path.clone(),
                    status: RepairRepositoryFormatStatus::Skipped,
                    reasons: vec![error.to_string()],
                }),
            }
        }
    }

    let mut stale_paths = Vec::new();
    let mut stale_repo_ids = HashSet::new();
    for repo in current_repos {
        if relocated_locator_keys.contains(&repo.current.key()) {
            continue;
        }
        if !repo.path.exists() {
            stale_repo_ids.insert(repo.id);
            let blocking_dependents = relationship_snapshot
                .iter()
                .filter(|relationship| {
                    relationship.controlling_repo_id == repo.id
                        && relationship.dependent_path.exists()
                })
                .map(|relationship| RepairStaleDependent {
                    relationship: relationship.relationship.clone(),
                    locator: relationship.dependent_locator.clone(),
                    path: relationship.dependent_path.clone(),
                })
                .collect::<Vec<_>>();
            let mut reasons = vec!["recorded checkout path does not exist".to_string()];
            let status = if blocking_dependents.is_empty() {
                if check {
                    RepairStalePathStatus::NeedsPrune
                } else {
                    db.delete_repo(repo.id)?;
                    RepairStalePathStatus::Pruned
                }
            } else {
                for dependent in &blocking_dependents {
                    reasons.push(format!(
                        "stale checkout is canonical for existing {} checkout {}; choose a new canonical before pruning this row",
                        dependent.relationship,
                        dependent.locator.key()
                    ));
                }
                RepairStalePathStatus::Blocked
            };
            stale_paths.push(RepairStalePath {
                repo_id: repo.id,
                locator: repo.current,
                path: repo.path,
                status,
                reasons,
                blocking_dependents,
            });
        }
    }

    let mut relationships = Vec::new();
    let mut skipped = Vec::new();
    let relationship_source = if check {
        relationship_snapshot
    } else {
        db.shared_git_dir_relationships()?
    };
    for relationship in relationship_source {
        if stale_repo_ids.contains(&relationship.dependent_repo_id)
            || stale_repo_ids.contains(&relationship.controlling_repo_id)
        {
            continue;
        }
        if !relationship.dependent_path.exists() {
            let reason = format!(
                "{} checkout does not exist: {}",
                relationship.relationship,
                relationship.dependent_path.display()
            );
            skipped.push(RepairSkip {
                relationship: relationship.relationship,
                dependent_locator: relationship.dependent_locator,
                controlling_locator: relationship.controlling_locator,
                reason,
            });
            continue;
        }
        if !relationship.controlling_path.exists() {
            skipped.push(RepairSkip {
                relationship: relationship.relationship,
                dependent_locator: relationship.dependent_locator,
                controlling_locator: relationship.controlling_locator,
                reason: format!(
                    "canonical checkout does not exist: {}",
                    relationship.controlling_path.display()
                ),
            });
            continue;
        }

        let repair_reasons =
            match shared_git_dir_relationship_repair_reasons(&relationship, config.clone_as_bare) {
                Ok(repair_reasons) => repair_reasons,
                Err(error) => {
                    skipped.push(RepairSkip {
                        relationship: relationship.relationship,
                        dependent_locator: relationship.dependent_locator,
                        controlling_locator: relationship.controlling_locator,
                        reason: error.to_string(),
                    });
                    continue;
                }
            };

        if repair_reasons.is_empty() {
            relationships.push(RepairRelationship {
                relationship: relationship.relationship,
                dependent_locator: relationship.dependent_locator,
                controlling_locator: relationship.controlling_locator,
                dependent_path: relationship.dependent_path,
                controlling_path: relationship.controlling_path,
                status: RepairStatus::Ok,
                reasons: Vec::new(),
                shared_git_dir: None,
            });
            continue;
        }

        if check {
            relationships.push(RepairRelationship {
                relationship: relationship.relationship,
                dependent_locator: relationship.dependent_locator,
                controlling_locator: relationship.controlling_locator,
                dependent_path: relationship.dependent_path,
                controlling_path: relationship.controlling_path,
                status: RepairStatus::NeedsRepair,
                reasons: repair_reasons,
                shared_git_dir: None,
            });
            continue;
        }

        match materialize_related_shared_git_dir(
            db,
            &relationship.dependent_locator,
            &relationship.dependent_path,
            &relationship.controlling_locator,
            &relationship.controlling_path,
            &relationship.relationship,
            config.clone_as_bare,
        ) {
            Ok(shared_git_dir) => relationships.push(RepairRelationship {
                relationship: relationship.relationship,
                dependent_locator: relationship.dependent_locator,
                controlling_locator: relationship.controlling_locator,
                dependent_path: relationship.dependent_path,
                controlling_path: relationship.controlling_path,
                status: RepairStatus::Repaired,
                reasons: repair_reasons,
                shared_git_dir: Some(shared_git_dir),
            }),
            Err(error) => skipped.push(RepairSkip {
                relationship: relationship.relationship,
                dependent_locator: relationship.dependent_locator,
                controlling_locator: relationship.controlling_locator,
                reason: error.to_string(),
            }),
        }
    }

    Ok(RepairReport {
        action: "check",
        check,
        repository_formats,
        stale_paths,
        untracked_checkouts,
        relationships,
        skipped,
    })
}

fn repair_report_needs_repair(report: &RepairReport) -> bool {
    report.repository_formats.iter().any(|format| {
        matches!(
            format.status,
            RepairRepositoryFormatStatus::NeedsBareConversion
        )
    }) || report
        .stale_paths
        .iter()
        .any(|stale| matches!(stale.status, RepairStalePathStatus::NeedsPrune))
        || report.untracked_checkouts.iter().any(|checkout| {
            matches!(
                checkout.status,
                RepairUntrackedCheckoutStatus::NeedsTracking
            )
        })
        || report
            .relationships
            .iter()
            .any(|relationship| matches!(relationship.status, RepairStatus::NeedsRepair))
}

fn checkout_uses_shared_git_dir(path: &Path) -> bool {
    let expected_git_dir = path.join(".git");
    git_dir(path)
        .map(|actual| comparable_path(&actual) != comparable_path(&expected_git_dir))
        .unwrap_or(false)
}

fn bare_conversion_needed(repo: &ManagedRepoRecord) -> RepairRepositoryFormat {
    RepairRepositoryFormat {
        repo_id: repo.id,
        locator: repo.current.clone(),
        path: repo.path.clone(),
        status: RepairRepositoryFormatStatus::NeedsBareConversion,
        reasons: vec![
            "clone-as-bare is enabled but this managed clone-root repository is non-bare"
                .to_string(),
        ],
    }
}

fn convert_repo_format_to_bare(repo: &ManagedRepoRecord) -> RepairRepositoryFormat {
    match convert_standalone_checkout_to_bare_repository(&repo.path) {
        Ok(()) => RepairRepositoryFormat {
            repo_id: repo.id,
            locator: repo.current.clone(),
            path: repo.path.clone(),
            status: RepairRepositoryFormatStatus::ConvertedToBare,
            reasons: vec![
                "converted clean managed clone-root checkout to a bare repository".to_string(),
            ],
        },
        Err(error) => RepairRepositoryFormat {
            repo_id: repo.id,
            locator: repo.current.clone(),
            path: repo.path.clone(),
            status: RepairRepositoryFormatStatus::Skipped,
            reasons: vec![error.to_string()],
        },
    }
}

fn shared_git_dir_relationship_repair_reasons(
    relationship: &SharedGitDirRelationship,
    clone_as_bare: bool,
) -> Result<Vec<String>> {
    let mut reasons = Vec::new();
    let controlling_origin = git_origin_url(&relationship.controlling_path)?;
    let dependent_url = remote_url_for_locator(
        controlling_origin.as_deref(),
        &relationship.dependent_locator,
    );
    if clone_as_bare {
        if let Some(view) = read_repo_view_metadata(&relationship.dependent_path)? {
            if comparable_path(&view.canonical_path)
                != comparable_path(&relationship.controlling_path)
            {
                reasons.push(format!(
                    "{} view points at canonical {}, expected {}",
                    relationship.relationship,
                    view.canonical_path.display(),
                    relationship.controlling_path.display()
                ));
            }
            if view.origin_url
                != remote_url_for_locator(
                    controlling_origin.as_deref(),
                    &relationship.dependent_locator,
                )
            {
                reasons.push(format!(
                    "{} view origin is {}, expected {}",
                    relationship.relationship,
                    view.origin_url,
                    remote_url_for_locator(
                        controlling_origin.as_deref(),
                        &relationship.dependent_locator,
                    )
                ));
            }
            return Ok(reasons);
        }
        reasons.push(format!(
            "{} checkout is not a repo-manager namespace view: {}",
            relationship.relationship,
            relationship.dependent_path.display()
        ));
        let dependent_is_bare = is_bare_repository(&relationship.dependent_path)?;
        let controlling_is_bare = is_bare_repository(&relationship.controlling_path)?;
        if !controlling_is_bare {
            reasons.push(format!(
                "canonical checkout is non-bare but clone-as-bare is enabled: {}",
                relationship.controlling_path.display()
            ));
        }
        if !dependent_is_bare {
            reasons.push(format!(
                "{} checkout is non-bare but clone-as-bare is enabled: {}",
                relationship.relationship,
                relationship.dependent_path.display()
            ));
        }
        return Ok(reasons);
    }
    let dependent_remote =
        related_remote_name(&relationship.relationship, &relationship.dependent_locator);
    let current_dependent_remote =
        git_remote_url(&relationship.controlling_path, &dependent_remote)?;
    if current_dependent_remote.as_deref() != Some(dependent_url.as_str()) {
        reasons.push(match current_dependent_remote {
            Some(current) => format!(
                "canonical checkout remote for {} `{dependent_remote}` points at {current}, expected {dependent_url}",
                relationship.relationship
            ),
            None => format!(
                "canonical checkout is missing {} remote `{dependent_remote}` -> {dependent_url}",
                relationship.relationship
            ),
        });
    }
    let dependent_common_dir = git_common_dir(&relationship.dependent_path)?;
    let controlling_common_dir = git_common_dir(&relationship.controlling_path)?;
    if dependent_common_dir != controlling_common_dir {
        reasons.push(format!(
            "{} does not use canonical Git directory; {} uses {}, canonical uses {}",
            relationship.relationship,
            relationship.relationship,
            dependent_common_dir.display(),
            controlling_common_dir.display()
        ));
    }
    let default_branch = shared_dependent_default_branch(
        &relationship.controlling_path,
        &relationship.dependent_path,
        &dependent_remote,
    )?;
    let local_branch = dependent_local_branch(
        &relationship.relationship,
        &relationship.dependent_locator,
        &default_branch,
    );
    let remote_branch = format!("{dependent_remote}/{default_branch}");
    let branch_action = format!("reading {} current branch", relationship.relationship);
    let current_branch = git_output(
        &relationship.dependent_path,
        ["branch", "--show-current"],
        &branch_action,
    )?;
    if current_branch.trim() != local_branch {
        reasons.push(format!(
            "{} checkout is on branch `{}`, expected `{local_branch}`",
            relationship.relationship,
            current_branch.trim()
        ));
    }
    let upstream_action = format!("reading {} upstream", relationship.relationship);
    let upstream = git_output_optional(
        &relationship.dependent_path,
        [
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{upstream}",
        ],
        &upstream_action,
    )?;
    if upstream.as_deref().map(str::trim) != Some(remote_branch.as_str()) {
        reasons.push(match upstream {
            Some(upstream) => format!(
                "{} branch upstream is `{}`, expected `{remote_branch}`",
                relationship.relationship,
                upstream.trim()
            ),
            None => format!(
                "{} branch has no upstream, expected `{remote_branch}`",
                relationship.relationship
            ),
        });
    }
    Ok(reasons)
}

fn add_worktree(
    config: &Config,
    db: &Store,
    output: &Output,
    dir: Option<&Path>,
    args: WorktreeAddArgs,
) -> Result<()> {
    warn_pending_related(db)?;
    if let Some(context) = worktree_context(config, db, dir, &args)? {
        return match context {
            RepoContext::Managed(repo) => {
                add_managed_context_worktree(config, db, output, &repo, args)
            }
            RepoContext::View(view) => add_repo_view_worktree(config, db, output, &view, args),
        };
    }
    let canonical_url = args.repo_or_name.clone();
    let name = args.name_or_start_point.as_deref().ok_or_else(|| {
        anyhow!("worktree name is required unless cwd or --dir selects a repository")
    })?;
    let locator = Locator::parse(&canonical_url)?;
    let plan = plan_worktree_add(
        &config.clone_root,
        &config.dev_worktree_root,
        locator,
        name,
        WorktreeAddOptions {
            start_point: args.start_point.as_deref(),
            branch: args.branch.as_deref(),
            detach: args.detach,
            force: args.force,
        },
    )?;
    fs::create_dir_all(
        plan.worktree_path
            .parent()
            .context("worktree path has no parent")?,
    )?;
    let arg_refs: Vec<&str> = plan.git_args.iter().map(String::as_str).collect();
    run_git_in(&plan.canonical_path, arg_refs)?;
    if args.reset {
        let start = args
            .start_point
            .as_deref()
            .ok_or_else(|| anyhow!("--reset requires a start point"))?;
        run_git_in(&plan.worktree_path, ["reset", "--hard", start])?;
    }
    let repo_id = db.upsert_repo(&plan.canonical_locator, &plan.canonical_path, None)?;
    db.record_worktree(repo_id, &plan.worktree_path, Some(name), None)?;
    output_worktree(output, &plan)
}

fn worktree_context(
    config: &Config,
    db: &Store,
    dir: Option<&Path>,
    args: &WorktreeAddArgs,
) -> Result<Option<RepoContext>> {
    if dir.is_some() {
        return repo_context(config, db, dir).map(Some);
    }
    if Locator::parse(&args.repo_or_name).is_ok() {
        return Ok(None);
    }
    match repo_context(config, db, None) {
        Ok(context) => Ok(Some(context)),
        Err(_) => Ok(None),
    }
}

fn add_managed_context_worktree(
    config: &Config,
    db: &Store,
    output: &Output,
    repo: &ManagedRepoRecord,
    args: WorktreeAddArgs,
) -> Result<()> {
    let name = args.repo_or_name;
    let start_point = args
        .name_or_start_point
        .as_deref()
        .or(args.start_point.as_deref());
    validate_worktree_name(&name)?;
    let worktree_path = locator_path(&config.dev_worktree_root, &repo.current).join(&name);
    fs::create_dir_all(
        worktree_path
            .parent()
            .context("worktree path has no parent")?,
    )?;
    let mut git_args = vec!["worktree".to_string(), "add".to_string()];
    if args.force {
        git_args.push("--force".to_string());
    }
    if let Some(branch) = args.branch.as_deref() {
        git_args.push("-b".to_string());
        git_args.push(branch.to_string());
    }
    if args.detach {
        git_args.push("--detach".to_string());
    }
    git_args.push(worktree_path.display().to_string());
    if let Some(start_point) = start_point {
        git_args.push(start_point.to_string());
    }
    let arg_refs: Vec<&str> = git_args.iter().map(String::as_str).collect();
    run_git_in(&repo.path, arg_refs)?;
    if args.reset {
        let start = start_point.ok_or_else(|| anyhow!("--reset requires a start point"))?;
        run_git_in(&worktree_path, ["reset", "--hard", start])?;
    }
    let repo_id = db.upsert_repo(&repo.current, &repo.path, None)?;
    db.record_worktree(repo_id, &worktree_path, Some(&name), None)?;
    output_worktree(
        output,
        &WorktreePlan {
            canonical_locator: repo.current.clone(),
            canonical_path: repo.path.clone(),
            worktree_path,
            git_args,
        },
    )
}

fn add_repo_view_worktree(
    config: &Config,
    db: &Store,
    output: &Output,
    view: &RepoViewMetadata,
    args: WorktreeAddArgs,
) -> Result<()> {
    let name = args.repo_or_name;
    let start_point = args
        .name_or_start_point
        .as_deref()
        .or(args.start_point.as_deref());
    let start_ref = resolve_repo_view_start_ref(view, start_point)?;
    let branch_ref = if let Some(branch) = &args.branch {
        let target = git_dir_output(
            &view.canonical_path,
            ["rev-parse", &start_ref],
            "resolving worktree branch start point",
        )?;
        let branch_ref = format!("{}/heads/{branch}", view.refs_prefix);
        git_dir_update_ref(&view.canonical_path, &branch_ref, target.trim())?;
        Some(branch_ref)
    } else if start_point.is_none() {
        Some(format!(
            "{}/heads/{}",
            view.refs_prefix, view.default_branch
        ))
    } else if start_ref.starts_with(&format!("{}/heads/", view.refs_prefix)) {
        Some(start_ref.clone())
    } else {
        None
    };
    let checkout_ref = branch_ref.clone().unwrap_or_else(|| start_ref.clone());
    let worktree_path = locator_path(&config.dev_worktree_root, &view.locator).join(&name);
    fs::create_dir_all(
        worktree_path
            .parent()
            .context("worktree path has no parent")?,
    )?;
    let mut command = git_dir_command(&view.canonical_path);
    command.args(["worktree", "add"]);
    if args.force {
        command.arg("--force");
    }
    command
        .arg("--detach")
        .arg(&worktree_path)
        .arg(&checkout_ref);
    let status = command
        .status()
        .with_context(|| format!("creating worktree {}", worktree_path.display()))?;
    if !status.success() {
        bail!("git worktree add failed with status {status}");
    }
    if let Some(branch_ref) = branch_ref {
        set_worktree_head(&worktree_path, &branch_ref)?;
    }
    if args.reset {
        run_git_in(&worktree_path, ["reset", "--hard", checkout_ref.as_str()])?;
    }
    let repo_id = db.upsert_repo(
        &view.locator,
        &view.path,
        Some(&view.canonical_locator.key()),
    )?;
    db.record_worktree(
        repo_id,
        &worktree_path,
        Some(&name),
        Some(&view.refs_prefix),
    )?;
    output_worktree(
        output,
        &WorktreePlan {
            canonical_locator: view.locator.clone(),
            canonical_path: view.canonical_path.clone(),
            worktree_path,
            git_args: vec![
                "worktree".to_string(),
                "add".to_string(),
                "--detach".to_string(),
                checkout_ref,
            ],
        },
    )
}

fn set_worktree_head(worktree_path: &Path, refname: &str) -> Result<()> {
    let git_file = fs::read_to_string(worktree_path.join(".git"))
        .with_context(|| format!("reading {}", worktree_path.join(".git").display()))?;
    let git_dir = git_file
        .trim()
        .strip_prefix("gitdir: ")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("worktree .git file is not a gitdir pointer"))?;
    let git_dir = if git_dir.is_absolute() {
        git_dir
    } else {
        worktree_path.join(git_dir)
    };
    fs::write(git_dir.join("HEAD"), format!("ref: {refname}\n"))
        .with_context(|| format!("writing {}", git_dir.join("HEAD").display()))
}

fn move_repo(
    config: &Config,
    db: &Store,
    output: &Output,
    repo_ref: &str,
    new_url: &str,
) -> Result<()> {
    warn_pending_related(db)?;
    let new_locator = Locator::parse(new_url)?;
    let (repo_id, old_locator, historical) = match db.find_repo(repo_ref)? {
        Some(record) => {
            let historical = db.historical_locators(record.id)?;
            (record.id, record.current, historical)
        }
        None => {
            let old_locator = Locator::parse(repo_ref)?;
            let old_path = locator_path(&config.clone_root, &old_locator);
            let repo_id = db.upsert_repo(&old_locator, &old_path, None)?;
            (repo_id, old_locator.clone(), vec![old_locator])
        }
    };
    let plan = plan_move(&config.clone_root, old_locator, new_locator, &historical);
    apply_filesystem_move(&plan)?;
    ensure_remote(&plan.new_path, "origin", new_url)?;
    db.apply_move_metadata(repo_id, &plan)?;
    output_move(output, &plan)
}

fn reconcile(config: &Config, db: &Store, output: &Output) -> Result<()> {
    warn_pending_related(db)?;
    let report = reconcile_repos(config, db)?;
    output_reconcile(output, &report)
}

fn reconcile_repos(config: &Config, db: &Store) -> Result<ReconcileReport> {
    let mut planned_moves = Vec::new();
    let mut skipped = Vec::new();

    for repo in db.current_repos()? {
        if !repo.path.exists() {
            skipped.push(ReconcileSkip {
                repo_id: repo.id,
                repo_path: repo.path,
                reason: "current path does not exist".to_string(),
            });
            continue;
        }

        let origin_url = git_origin_url(&repo.path)?;

        if let Some(forge_locator) = github_redirect_locator(&config.cache_root, &repo.current)?
            && forge_locator != repo.current
        {
            let historical = db.historical_locators(repo.id)?;
            let plan = plan_move(
                &config.clone_root,
                repo.current.clone(),
                forge_locator,
                &historical,
            );
            apply_filesystem_move(&plan)?;
            let new_origin_url = remote_url_for_locator(origin_url.as_deref(), &plan.new_locator);
            ensure_remote(&plan.new_path, "origin", &new_origin_url)?;
            db.apply_move_metadata(repo.id, &plan)?;
            planned_moves.push(ReconcileMove {
                repo_id: repo.id,
                repo_path: repo.path,
                evidence: "github-api".to_string(),
                plan,
            });
            continue;
        }

        let Some(origin_url) = origin_url else {
            skipped.push(ReconcileSkip {
                repo_id: repo.id,
                repo_path: repo.path,
                reason: "origin remote is not configured".to_string(),
            });
            continue;
        };

        let origin_locator = match Locator::parse(&origin_url) {
            Ok(locator) => locator,
            Err(error) => {
                skipped.push(ReconcileSkip {
                    repo_id: repo.id,
                    repo_path: repo.path,
                    reason: format!("origin URL is not a supported Git locator: {error}"),
                });
                continue;
            }
        };

        if origin_locator == repo.current {
            continue;
        }

        let historical = db.historical_locators(repo.id)?;
        let plan = plan_move(
            &config.clone_root,
            repo.current.clone(),
            origin_locator,
            &historical,
        );
        apply_filesystem_move(&plan)?;
        ensure_remote(&plan.new_path, "origin", &origin_url)?;
        db.apply_move_metadata(repo.id, &plan)?;
        planned_moves.push(ReconcileMove {
            repo_id: repo.id,
            repo_path: repo.path,
            evidence: format!("origin-url:{origin_url}"),
            plan,
        });
    }

    Ok(ReconcileReport {
        action: "reconcile",
        planned_moves,
        skipped,
    })
}

fn successor_set(
    config: &Config,
    db: &Store,
    output: &Output,
    old_ref: &str,
    new_url: &str,
) -> Result<()> {
    warn_pending_related(db)?;
    let new_locator = Locator::parse(new_url)?;
    db.record_successor(old_ref, &new_locator)?;
    output_successor(
        output,
        &SuccessorResult {
            action: "successor-set",
            old_ref: old_ref.to_string(),
            new_path: locator_path(&config.clone_root, &new_locator),
            new_locator,
        },
    )
}

fn aliases_list(db: &Store, output: &Output, repo_ref: &str) -> Result<()> {
    output_aliases(output, &db.aliases(repo_ref)?)
}

fn related_list(db: &Store, output: &Output) -> Result<()> {
    output_related(output, &db.related_suggestions(true)?)
}

fn related_resolve(
    config: &Config,
    db: &Store,
    output: &Output,
    id: i64,
    kind: &str,
) -> Result<()> {
    validate_relationship_kind(kind)?;
    let shared_git_dir = matches!(kind, "fork" | "mirror")
        .then(|| resolve_related_shared_git_dir(config, db, id, kind))
        .transpose()?;
    db.resolve_related(id, kind)?;
    output_related_resolution(
        output,
        &RelatedResolution {
            action: "related-resolve",
            id,
            resolution: kind.to_string(),
            shared_git_dir,
        },
    )
}

fn resolve_related_shared_git_dir(
    config: &Config,
    db: &Store,
    id: i64,
    relationship: &str,
) -> Result<SharedGitDirResolution> {
    let suggestion = db.related_suggestion(id)?;
    if let Some(resolution) = suggestion.resolution {
        bail!("related-history suggestion #{id} is already resolved as {resolution}");
    }
    materialize_related_shared_git_dir(
        db,
        &suggestion.repo_locator,
        &suggestion.repo_path,
        &suggestion.related_locator,
        &suggestion.related_path,
        relationship,
        config.clone_as_bare,
    )
}

fn materialize_related_shared_git_dir(
    db: &Store,
    dependent_locator: &Locator,
    dependent_path: &Path,
    controlling_locator: &Locator,
    controlling_path: &Path,
    relationship: &str,
    clone_as_bare: bool,
) -> Result<SharedGitDirResolution> {
    if !dependent_path.exists() {
        bail!(
            "{relationship} checkout does not exist: {}",
            dependent_path.display()
        );
    }
    if !controlling_path.exists() {
        bail!(
            "canonical checkout does not exist: {}",
            controlling_path.display()
        );
    }

    if clone_as_bare {
        return materialize_related_bare_repo(
            db,
            dependent_locator,
            dependent_path,
            controlling_locator,
            controlling_path,
            relationship,
        );
    }

    let controlling_origin = git_origin_url(controlling_path)?;
    let controlling_url =
        remote_url_for_locator(controlling_origin.as_deref(), controlling_locator);
    ensure_remote(controlling_path, "origin", &controlling_url)?;
    let dependent_url = remote_url_for_locator(controlling_origin.as_deref(), dependent_locator);
    let dependent_remote = related_remote_name(relationship, dependent_locator);
    let already_shared = git_common_dir(dependent_path)? == git_common_dir(controlling_path)?;

    ensure_remote(controlling_path, &dependent_remote, &dependent_url)?;
    let default_branch = if already_shared {
        fetch_remote_refs(controlling_path, &dependent_remote)?;
        shared_dependent_default_branch(controlling_path, dependent_path, &dependent_remote)?
    } else {
        dependent_default_branch(dependent_path)?
    };
    let local_branch = dependent_local_branch(relationship, dependent_locator, &default_branch);
    let remote_branch = format!("{dependent_remote}/{default_branch}");

    let converted_to_worktree = if already_shared {
        ensure_tracking_branch(controlling_path, &local_branch, &remote_branch)?;
        checkout_branch(dependent_path, &local_branch)?;
        false
    } else {
        convert_checkout_to_worktree(
            controlling_path,
            dependent_path,
            &dependent_remote,
            &local_branch,
            &remote_branch,
        )?
    };

    let controlling_id = db.upsert_repo(controlling_locator, controlling_path, None)?;
    let dependent_id = db.upsert_repo(
        dependent_locator,
        dependent_path,
        Some(&controlling_locator.key()),
    )?;
    if relationship == "fork" {
        db.record_fork(dependent_id, controlling_id)?;
    }

    Ok(SharedGitDirResolution {
        dependent_locator: dependent_locator.clone(),
        controlling_locator: controlling_locator.clone(),
        dependent_path: dependent_path.to_path_buf(),
        controlling_path: controlling_path.to_path_buf(),
        dependent_remote,
        dependent_url,
        local_branch,
        remote_branch,
        converted_to_worktree,
    })
}

fn materialize_related_bare_repo(
    db: &Store,
    dependent_locator: &Locator,
    dependent_path: &Path,
    controlling_locator: &Locator,
    controlling_path: &Path,
    relationship: &str,
) -> Result<SharedGitDirResolution> {
    if !controlling_path.exists() {
        bail!(
            "canonical bare repository does not exist: {}",
            controlling_path.display()
        );
    }
    ensure_bare_repository(controlling_path)?;

    let controlling_origin = git_origin_url(controlling_path)?;
    let controlling_url =
        remote_url_for_locator(controlling_origin.as_deref(), controlling_locator);
    ensure_remote(controlling_path, "origin", &controlling_url)?;
    let dependent_url = remote_url_for_locator(controlling_origin.as_deref(), dependent_locator);
    if dependent_path.exists() && read_repo_view_metadata(dependent_path)?.is_none() {
        fetch_existing_dependent_into_namespace(
            controlling_path,
            dependent_path,
            relationship,
            dependent_locator,
        )?;
    }
    let view = materialize_namespace_view(
        dependent_locator,
        dependent_path,
        controlling_locator,
        controlling_path,
        relationship,
        &dependent_url,
    )?;
    let local_branch = format!("{}/heads/{}", view.refs_prefix, view.default_branch);
    let remote_branch = format!(
        "{}/remotes/origin/{}",
        view.refs_prefix, view.default_branch
    );

    let controlling_id = db.upsert_repo(controlling_locator, controlling_path, None)?;
    let dependent_id = db.upsert_repo(
        dependent_locator,
        dependent_path,
        Some(&controlling_locator.key()),
    )?;
    if relationship == "fork" {
        db.record_fork(dependent_id, controlling_id)?;
    } else {
        db.record_resolved_related(dependent_id, controlling_id, relationship)?;
    }

    Ok(SharedGitDirResolution {
        dependent_locator: dependent_locator.clone(),
        controlling_locator: controlling_locator.clone(),
        dependent_path: dependent_path.to_path_buf(),
        controlling_path: controlling_path.to_path_buf(),
        dependent_remote: "origin".to_string(),
        dependent_url,
        local_branch,
        remote_branch,
        converted_to_worktree: false,
    })
}

fn fetch_existing_dependent_into_namespace(
    controlling_path: &Path,
    dependent_path: &Path,
    relationship: &str,
    dependent_locator: &Locator,
) -> Result<()> {
    let refs_prefix = repo_view_refs_prefix(relationship, dependent_locator);
    let heads_refspec = format!("+refs/heads/*:{refs_prefix}/heads/*");
    let remote_heads_refspec = format!("+refs/remotes/origin/*:{refs_prefix}/remotes/origin/*");
    let tags_refspec = format!("+refs/tags/*:{refs_prefix}/tags/*");
    let status = git_dir_command(controlling_path)
        .args(["fetch", "--no-tags"])
        .arg(dependent_path)
        .arg(heads_refspec)
        .arg(remote_heads_refspec)
        .arg(tags_refspec)
        .status()
        .with_context(|| {
            format!(
                "fetching existing dependent refs from {} into {}",
                dependent_path.display(),
                controlling_path.display()
            )
        })?;
    if !status.success() {
        bail!("git fetch from existing dependent failed with status {status}");
    }
    Ok(())
}

fn convert_checkout_to_worktree(
    controlling_path: &Path,
    dependent_path: &Path,
    dependent_remote: &str,
    local_branch: &str,
    remote_branch: &str,
) -> Result<bool> {
    ensure_clean_checkout(dependent_path)?;
    fetch_local_dependent_refs(controlling_path, dependent_path, dependent_remote)?;
    ensure_tracking_branch(controlling_path, local_branch, remote_branch)?;
    let backup_path = unique_backup_path(dependent_path)?;
    fs::rename(dependent_path, &backup_path).with_context(|| {
        format!(
            "moving existing managed checkout {} to {}",
            dependent_path.display(),
            backup_path.display()
        )
    })?;

    let add_result = run_git_in(
        controlling_path,
        [
            "worktree",
            "add",
            &dependent_path.display().to_string(),
            local_branch,
        ],
    );
    if let Err(error) = add_result {
        if !dependent_path.exists() {
            let _ = fs::rename(&backup_path, dependent_path);
        }
        return Err(error).with_context(|| {
            format!(
                "creating managed worktree {} from canonical checkout {}",
                dependent_path.display(),
                controlling_path.display()
            )
        });
    }

    fs::remove_dir_all(&backup_path)
        .with_context(|| format!("removing replaced checkout {}", backup_path.display()))?;
    Ok(true)
}

fn ensure_clean_checkout(path: &Path) -> Result<()> {
    let status = git_output(
        path,
        ["status", "--porcelain=v1", "--untracked-files=all"],
        "checking checkout cleanliness",
    )?;
    if !status.trim().is_empty() {
        bail!(
            "checkout has uncommitted or untracked changes and cannot be converted safely: {}",
            path.display()
        );
    }
    Ok(())
}

fn convert_standalone_checkout_to_bare_repository(path: &Path) -> Result<()> {
    ensure_clean_checkout(path)?;
    let expected_git_dir = path.join(".git");
    let actual_git_dir = git_dir(path)?;
    if comparable_path(&actual_git_dir) != comparable_path(&expected_git_dir)
        || !expected_git_dir.is_dir()
    {
        bail!(
            "checkout uses a shared Git directory and cannot be converted as a standalone repository: {}",
            path.display()
        );
    }

    let linked_worktrees = linked_worktree_paths(path)?
        .into_iter()
        .filter(|worktree_path| comparable_path(worktree_path) != comparable_path(path))
        .collect::<Vec<_>>();
    let backup_path = unique_backup_path(path)?;
    fs::rename(path, &backup_path).with_context(|| {
        format!(
            "moving existing checkout {} to {}",
            path.display(),
            backup_path.display()
        )
    })?;

    let backup_git_dir = backup_path.join(".git");
    if let Err(error) = fs::rename(&backup_git_dir, path).with_context(|| {
        format!(
            "moving Git directory {} to bare repository {}",
            backup_git_dir.display(),
            path.display()
        )
    }) {
        let _ = fs::rename(&backup_path, path);
        return Err(error);
    }

    let configure_result = (|| -> Result<()> {
        set_git_config_value(&path.join("config"), "core.bare", "true")?;
        unset_git_config_value(&path.join("config"), "core.worktree")?;
        for worktree_path in linked_worktrees {
            let worktree_arg = worktree_path.display().to_string();
            run_git_in(path, ["worktree", "repair", worktree_arg.as_str()])?;
        }
        Ok(())
    })();
    if let Err(error) = configure_result {
        let _ = fs::rename(path, &backup_git_dir);
        let _ = fs::rename(&backup_path, path);
        return Err(error);
    }

    fs::remove_dir_all(&backup_path)
        .with_context(|| format!("removing replaced checkout {}", backup_path.display()))?;
    Ok(())
}

fn dependent_default_branch(path: &Path) -> Result<String> {
    if let Some(remote_head) = git_output_optional(
        path,
        [
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ],
        "reading origin default branch",
    )? && let Some(branch) = remote_head.trim().strip_prefix("origin/")
        && !branch.is_empty()
    {
        return Ok(branch.to_string());
    }
    if let Some(branch) =
        git_output_optional(path, ["branch", "--show-current"], "reading current branch")?
    {
        let branch = branch.trim();
        if !branch.is_empty() {
            return Ok(branch.to_string());
        }
    }
    bail!(
        "could not determine managed checkout default branch from origin/HEAD or current branch: {}",
        path.display()
    )
}

fn shared_dependent_default_branch(
    controlling_path: &Path,
    dependent_path: &Path,
    dependent_remote: &str,
) -> Result<String> {
    if let Some(remote_head) = remote_default_branch(controlling_path, dependent_remote)? {
        return Ok(remote_head);
    }
    let current = git_output(
        dependent_path,
        ["branch", "--show-current"],
        "reading managed current branch",
    )?;
    current
        .trim()
        .rsplit('/')
        .next()
        .filter(|branch| !branch.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            anyhow!(
                "could not determine managed default branch for {}",
                dependent_path.display()
            )
        })
}

fn remote_default_branch(cwd: &Path, remote: &str) -> Result<Option<String>> {
    let remote_head_ref = format!("refs/remotes/{remote}/HEAD");
    let Some(remote_head) = git_output_optional(
        cwd,
        ["symbolic-ref", "--quiet", "--short", &remote_head_ref],
        "reading managed remote default branch",
    )?
    else {
        return Ok(None);
    };
    Ok(remote_head
        .trim()
        .strip_prefix(&format!("{remote}/"))
        .filter(|branch| !branch.is_empty())
        .map(str::to_string))
}

fn fetch_remote_refs(cwd: &Path, remote: &str) -> Result<()> {
    run_git_in(cwd, ["fetch", "--no-tags", remote])?;
    let _ = run_git_in(cwd, ["remote", "set-head", remote, "-a"]);
    Ok(())
}

fn dependent_local_branch(
    relationship: &str,
    dependent_locator: &Locator,
    default_branch: &str,
) -> String {
    let plural = match relationship {
        "mirror" => "mirrors",
        _ => "forks",
    };
    format!(
        "repo-manager/{plural}/{}/{}",
        sanitize_remote_name(&dependent_locator.key()),
        default_branch
    )
}

fn ensure_tracking_branch(cwd: &Path, local_branch: &str, remote_branch: &str) -> Result<()> {
    if git_ref_exists(cwd, &format!("refs/heads/{local_branch}"))? {
        run_git_in(
            cwd,
            ["branch", "--set-upstream-to", remote_branch, local_branch],
        )
    } else {
        run_git_in(cwd, ["branch", "--track", local_branch, remote_branch])
    }
}

fn checkout_branch(cwd: &Path, local_branch: &str) -> Result<()> {
    let current = git_output(cwd, ["branch", "--show-current"], "reading current branch")?;
    if current.trim() == local_branch {
        return Ok(());
    }
    run_git_in(cwd, ["checkout", local_branch])
}

fn fetch_local_dependent_refs(
    controlling_path: &Path,
    dependent_path: &Path,
    dependent_remote: &str,
) -> Result<()> {
    let heads_refspec = format!("+refs/heads/*:refs/remotes/{dependent_remote}/*");
    let head_refspec = format!("+HEAD:refs/remotes/{dependent_remote}/HEAD");
    let tags_refspec =
        format!("+refs/tags/*:refs/repo-manager/dependents/{dependent_remote}/tags/*");
    let status = git_command(controlling_path)
        .args(["fetch", "--no-tags"])
        .arg(dependent_path)
        .arg(heads_refspec)
        .arg(head_refspec)
        .arg(tags_refspec)
        .status()
        .with_context(|| {
            format!(
                "fetching local managed-checkout refs from {} into {}",
                dependent_path.display(),
                controlling_path.display()
            )
        })?;
    if !status.success() {
        bail!("git fetch from local managed checkout failed with status {status}");
    }
    Ok(())
}

fn unique_backup_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().context("fork path has no parent")?;
    let leaf = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("fork path has no UTF-8 leaf name")?;
    for index in 0..1000 {
        let candidate = parent.join(format!(
            ".repo-manager-replaced-{leaf}-{}-{index}",
            std::process::id()
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("could not allocate backup path for {}", path.display())
}

fn validate_relationship_kind(kind: &str) -> Result<()> {
    match kind {
        "mirror" | "fork" | "canonical" | "moved" | "successor" | "unrelated" => Ok(()),
        _ => bail!(
            "invalid relationship kind: {kind}; expected mirror, fork, canonical, moved, successor, or unrelated"
        ),
    }
}

fn warn_pending_related(db: &Store) -> Result<()> {
    let count = db.pending_related_count()?;
    if count > 0 {
        eprintln!(
            "repo-manager: {count} unresolved shared-history suggestion(s); run `repo related list`"
        );
    }
    Ok(())
}

#[derive(Debug)]
struct RateLimiter {
    min_interval: Option<Duration>,
    last_seen: HashMap<String, Instant>,
}

impl RateLimiter {
    fn new(requests_per_second: u32) -> Self {
        let min_interval = (requests_per_second > 0)
            .then(|| Duration::from_secs_f64(1.0 / f64::from(requests_per_second)));
        Self {
            min_interval,
            last_seen: HashMap::new(),
        }
    }

    fn allow(&mut self, key: &str) -> bool {
        let Some(min_interval) = self.min_interval else {
            return true;
        };
        let now = Instant::now();
        match self.last_seen.get(key) {
            Some(last_seen) if now.duration_since(*last_seen) < min_interval => false,
            _ => {
                self.last_seen.insert(key.to_string(), now);
                true
            }
        }
    }
}

#[derive(Debug)]
struct DaemonState {
    rate_limiter: Mutex<RateLimiter>,
    clone_starts: Mutex<HashMap<String, InProgressClone>>,
    clone_start_ttl: Duration,
}

impl DaemonState {
    fn new(rate_limit_per_second: u32, clone_start_ttl_minutes: u64) -> Self {
        Self {
            rate_limiter: Mutex::new(RateLimiter::new(rate_limit_per_second)),
            clone_starts: Mutex::new(HashMap::new()),
            clone_start_ttl: Duration::from_secs(clone_start_ttl_minutes.saturating_mul(60)),
        }
    }
}

#[derive(Debug)]
struct InProgressClone {
    event: CloneStartedEvent,
    started_at: Instant,
}

fn parse_rpc_endpoint(input: &str) -> Result<PathBuf> {
    let url = Url::parse(input).with_context(|| format!("invalid RPC endpoint URL: {input}"))?;
    match url.scheme() {
        "unix" => {
            let path = PathBuf::from(url.path());
            if path.as_os_str().is_empty() {
                bail!("unix RPC endpoint requires a socket path");
            }
            Ok(path)
        }
        scheme => bail!("unsupported RPC endpoint scheme: {scheme}; expected unix"),
    }
}

fn check_daemon_reachable(endpoint: &str) -> Result<()> {
    let path = parse_rpc_endpoint(endpoint)?;
    #[cfg(unix)]
    {
        UnixStream::connect(&path)
            .with_context(|| format!("connecting to repo-manager daemon at {endpoint}"))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        bail!("unix RPC endpoints are not supported on this platform")
    }
}

fn daemon_ping(config: &Config, output: &Output) -> Result<()> {
    check_daemon_reachable(&config.rpc_url)?;
    output_daemon_ping(
        output,
        &DaemonPingResult {
            action: "daemon-ping",
            rpc_url: config.rpc_url.clone(),
            reachable: true,
        },
    )
}

fn send_rpc_event(endpoint: &str, event: &RpcEvent) -> Result<()> {
    let mut message = Vec::new();
    event
        .to_proto()
        .encode_length_delimited(&mut message)
        .context("encoding RPC clone event")?;
    let path = parse_rpc_endpoint(endpoint)?;
    #[cfg(unix)]
    {
        let mut stream = UnixStream::connect(path)?;
        stream.write_all(&message)?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        bail!("unix RPC endpoints are not supported on this platform")
    }
}

fn send_rpc_event_best_effort(endpoint: &str, event: &RpcEvent) -> bool {
    match send_rpc_event(endpoint, event) {
        Ok(()) => {
            debug!("sent RPC event to {endpoint}: {event:?}");
            true
        }
        Err(error) => {
            warn!("could not send RPC event to {endpoint}: {error:#}");
            false
        }
    }
}

fn run_daemon(config: &DaemonConfig, rpc_url: &str, args: DaemonArgs) -> Result<()> {
    let path = parse_rpc_endpoint(args.listen.as_deref().unwrap_or(rpc_url))?;
    let daemon_state = Arc::new(DaemonState::new(
        config.rpc_rate_limit_per_second,
        config.clone_start_ttl_minutes,
    ));
    spawn_clone_ttl_cleanup(Arc::clone(&daemon_state));
    if let Some(minimum_interval_seconds) = config.background_fetch_minimum_interval_seconds {
        spawn_background_fetch(config.clone(), minimum_interval_seconds);
    }
    run_unix_daemon(config, &path, daemon_state)
}

fn spawn_background_fetch(config: DaemonConfig, minimum_interval_seconds: u64) {
    thread::spawn(move || {
        let sleep_for = Duration::from_secs(minimum_interval_seconds.clamp(
            BACKGROUND_FETCH_MIN_WAKE_SECONDS,
            BACKGROUND_FETCH_MAX_WAKE_SECONDS,
        ));
        loop {
            if let Err(error) = background_fetch_once(&config, minimum_interval_seconds) {
                warn!("background fetch pass failed: {error:#}");
            }
            thread::sleep(sleep_for);
        }
    });
}

fn background_fetch_once(config: &DaemonConfig, minimum_interval_seconds: u64) -> Result<usize> {
    let store = Store::open(&config.state)?;
    let now = epoch_seconds()?;
    let candidates = store.background_fetch_candidates(now, minimum_interval_seconds)?;
    let mut fetched = 0;
    for candidate in candidates {
        if !candidate.path.exists() {
            store.record_background_fetch(
                candidate.repo_id,
                now,
                minimum_interval_seconds,
                false,
                Some("tracked repository path does not exist"),
            )?;
            continue;
        }
        match background_fetch_repo(&candidate.path) {
            Ok(changed) => {
                store.record_background_fetch(
                    candidate.repo_id,
                    epoch_seconds()?,
                    minimum_interval_seconds,
                    changed,
                    None,
                )?;
                fetched += 1;
                debug!(
                    "background fetched {} changed={changed}",
                    candidate.locator.key()
                );
            }
            Err(error) => {
                warn!(
                    "background fetch failed for {} at {}: {error:#}",
                    candidate.locator.key(),
                    candidate.path.display()
                );
                store.record_background_fetch(
                    candidate.repo_id,
                    epoch_seconds()?,
                    minimum_interval_seconds,
                    false,
                    Some(&error.to_string()),
                )?;
            }
        }
    }
    Ok(fetched)
}

fn background_fetch_repo(path: &Path) -> Result<bool> {
    let before = git_ref_fingerprint(path)?;
    if is_bare_repository(path)? {
        background_fetch_bare_repo(path)?;
    } else {
        run_git_in(path, ["fetch", "--all", "--prune"])?;
    }
    let after = git_ref_fingerprint(path)?;
    Ok(before != after)
}

fn background_fetch_bare_repo(path: &Path) -> Result<()> {
    let remotes = git_remotes(path)?;
    for remote in remotes {
        if remote.name == "origin" {
            run_git_in(
                path,
                [
                    "fetch",
                    "--prune",
                    "origin",
                    "+refs/heads/*:refs/heads/*",
                    "+refs/tags/*:refs/tags/*",
                ],
            )?;
        } else {
            let heads_refspec = format!("+refs/heads/*:refs/remotes/{}/*", remote.name);
            let tags_refspec = format!(
                "+refs/tags/*:refs/repo-manager/remotes/{}/tags/*",
                remote.name
            );
            run_git_in(
                path,
                [
                    "fetch",
                    "--prune",
                    &remote.name,
                    &heads_refspec,
                    &tags_refspec,
                ],
            )?;
        }
    }
    Ok(())
}

fn git_ref_fingerprint(path: &Path) -> Result<String> {
    git_output(
        path,
        [
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads",
            "refs/remotes",
            "refs/tags",
        ],
        "reading Git refs",
    )
}

fn epoch_seconds() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?;
    Ok(duration.as_secs().min(i64::MAX as u64) as i64)
}

fn spawn_clone_ttl_cleanup(daemon_state: Arc<DaemonState>) {
    thread::spawn(move || {
        let sleep_for = daemon_state
            .clone_start_ttl
            .min(Duration::from_secs(60))
            .max(Duration::from_secs(1));
        loop {
            thread::sleep(sleep_for);
            if let Err(error) = prune_expired_clone_starts(&daemon_state) {
                warn!("could not prune expired clone-start events: {error:#}");
            }
        }
    });
}

fn prune_expired_clone_starts(daemon_state: &DaemonState) -> Result<usize> {
    let ttl = daemon_state.clone_start_ttl;
    let now = Instant::now();
    let mut clone_starts = daemon_state
        .clone_starts
        .lock()
        .map_err(|_| anyhow!("daemon clone-start lock poisoned"))?;
    let before = clone_starts.len();
    clone_starts.retain(|_key, clone| {
        let keep = now.duration_since(clone.started_at) < ttl;
        if !keep {
            debug!(
                "clone-start event expired for client {}: {} -> {}",
                clone.event.client_id,
                clone.event.locator.key(),
                clone.event.path.display()
            );
        }
        keep
    });
    let pruned = before - clone_starts.len();
    if pruned > 0 {
        debug!("pruned {pruned} expired clone-start event(s)");
    }
    Ok(pruned)
}

#[cfg(unix)]
fn run_unix_daemon(
    config: &DaemonConfig,
    path: &Path,
    daemon_state: Arc<DaemonState>,
) -> Result<()> {
    if path.exists() {
        fs::remove_file(path)
            .with_context(|| format!("removing stale RPC socket {}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating RPC socket directory {}", parent.display()))?;
    }
    let listener =
        UnixListener::bind(path).with_context(|| format!("listening on {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting RPC socket permissions on {}", path.display()))?;
    println!("repo-manager daemon listening on unix://{}", path.display());
    for stream in listener.incoming() {
        let stream = stream.context("accepting unix RPC connection")?;
        let peer = unix_peer_description(&stream, path);
        let config = config.clone();
        let daemon_state = Arc::clone(&daemon_state);
        thread::spawn(move || {
            if let Err(error) = handle_rpc_stream(&config, stream, peer, daemon_state) {
                eprintln!("repo-manager daemon: {error:#}");
            }
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn run_unix_daemon(
    _config: &DaemonConfig,
    _path: &Path,
    _daemon_state: Arc<DaemonState>,
) -> Result<()> {
    bail!("unix RPC endpoints are not supported on this platform")
}

fn handle_rpc_stream<R: Read>(
    config: &DaemonConfig,
    mut stream: R,
    peer: String,
    daemon_state: Arc<DaemonState>,
) -> Result<()> {
    let mut message = Vec::new();
    stream
        .read_to_end(&mut message)
        .with_context(|| format!("reading RPC message from {peer}"))?;
    if message.is_empty() {
        return Ok(());
    }
    debug!("received RPC message from {peer}: {} bytes", message.len());
    let event = decode_rpc_event(&message)?;
    if allow_rpc_event(&daemon_state, &event, &peer)? {
        handle_rpc_event(config, &daemon_state, event)?;
    }

    Ok(())
}

fn decode_rpc_event(message: &[u8]) -> Result<RpcEvent> {
    let event = api::CloneEvent::decode_length_delimited(message).context("decoding RPC event")?;
    RpcEvent::from_proto(event)
}

fn allow_rpc_event(daemon_state: &DaemonState, event: &RpcEvent, peer: &str) -> Result<bool> {
    let mut limiter = daemon_state
        .rate_limiter
        .lock()
        .map_err(|_| anyhow!("RPC rate limiter lock poisoned"))?;
    let key = format!("{}:{}", event.client_id(), event.event_name());
    let allowed = limiter.allow(&key);
    if !allowed {
        warn!(
            "rate limited RPC message from client {} ({peer})",
            event.client_id()
        );
    }
    Ok(allowed)
}

#[cfg(unix)]
fn unix_peer_description(stream: &UnixStream, socket_path: &Path) -> String {
    let addr = stream
        .peer_addr()
        .ok()
        .and_then(|addr| addr.as_pathname().map(|path| path.display().to_string()))
        .unwrap_or_else(|| "unnamed-peer".to_string());
    format!("unix://{} peer={addr}", socket_path.display())
}

fn handle_rpc_event(
    config: &DaemonConfig,
    daemon_state: &DaemonState,
    event: RpcEvent,
) -> Result<()> {
    prune_expired_clone_starts(daemon_state)?;
    match event {
        RpcEvent::Started(event) => {
            debug!(
                "clone started from client {}: {} -> {} scan_root={}",
                event.client_id,
                event.locator.key(),
                event.path.display(),
                event.scan_root.display()
            );
            daemon_state
                .clone_starts
                .lock()
                .map_err(|_| anyhow!("daemon clone-start lock poisoned"))?
                .insert(
                    clone_event_key(&event.client_id, &event.locator, &event.path),
                    InProgressClone {
                        event,
                        started_at: Instant::now(),
                    },
                );
            Ok(())
        }
        RpcEvent::Finished(event) => {
            debug!(
                "clone finished from client {}: {} -> {} success={} scan_root={}",
                event.client_id,
                event.locator.key(),
                event.path.display(),
                event.success,
                event.scan_root.display()
            );
            let started = daemon_state
                .clone_starts
                .lock()
                .map_err(|_| anyhow!("daemon clone-start lock poisoned"))?
                .remove(&clone_event_key(
                    &event.client_id,
                    &event.locator,
                    &event.path,
                ));
            if event.success && config.detect_related && started.is_some() {
                review_related_history(config, &event.locator, &event.path, &event.scan_root)?;
            } else if event.success && config.detect_related {
                debug!(
                    "skipping related-history review for {} because no matching clone-start event was observed",
                    event.locator.key()
                );
            } else if event.success {
                debug!(
                    "skipping related-history review for {} because shared-history detection is disabled",
                    event.locator.key()
                );
            }
            Ok(())
        }
        RpcEvent::Cancelled(event) => {
            debug!(
                "clone cancelled from client {}: {} -> {} reason={} scan_root={}",
                event.client_id,
                event.locator.key(),
                event.path.display(),
                event.reason,
                event.scan_root.display()
            );
            daemon_state
                .clone_starts
                .lock()
                .map_err(|_| anyhow!("daemon clone-start lock poisoned"))?
                .remove(&clone_event_key(
                    &event.client_id,
                    &event.locator,
                    &event.path,
                ));
            Ok(())
        }
        RpcEvent::ManageRequested(event) => {
            debug!(
                "manage requested from client {}: {} -> {} scan_root={}",
                event.client_id,
                event.locator.key(),
                event.path.display(),
                event.scan_root.display()
            );
            if config.detect_related {
                review_related_history(config, &event.locator, &event.path, &event.scan_root)
            } else {
                debug!(
                    "skipping related-history review for {} because shared-history detection is disabled",
                    event.locator.key()
                );
                Ok(())
            }
        }
    }
}

fn clone_event_key(client_id: &str, locator: &Locator, path: &Path) -> String {
    format!("{}\n{}\n{}", client_id, locator.key(), path.display())
}

fn review_related_history(
    config: &DaemonConfig,
    locator: &Locator,
    path: &Path,
    scan_root: &Path,
) -> Result<()> {
    debug!(
        "reviewing related history for {} under client scan root {}",
        locator.key(),
        scan_root.display()
    );
    let store = Store::open(&config.state)?;
    let count = detect_related_history_under_code(&store, locator, path, scan_root)?;
    debug!(
        "related-history review for {} found {} candidate(s)",
        locator.key(),
        count
    );
    if count > 0 {
        notify_related_history(count, locator);
    }
    Ok(())
}

fn detect_related_history_under_code(
    store: &Store,
    locator: &Locator,
    path: &Path,
    scan_root: &Path,
) -> Result<usize> {
    let current_id = store.upsert_repo(locator, path, None)?;
    let current_roots = git_root_commits(path)?.into_iter().collect::<HashSet<_>>();
    if current_roots.is_empty() {
        return Ok(0);
    }
    let current_path = comparable_path(path);
    let mut detected = 0;

    for other_path in discover_git_repositories(scan_root)? {
        if comparable_path(&other_path) == current_path {
            continue;
        }
        let shared = shared_root_evidence(&current_roots, &other_path)?;
        if shared.is_empty() {
            continue;
        }
        let Some(other_locator) = repo_locator_from_origin(&other_path)? else {
            debug!(
                "skipping shared-history candidate without parseable origin: {}",
                other_path.display()
            );
            continue;
        };
        let other_id = store.upsert_repo(&other_locator, &other_path, None)?;
        store.record_related_history(current_id, other_id, &shared)?;
        detected += 1;
    }

    Ok(detected)
}

fn discover_git_repositories(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut repos = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        if is_git_repository_path(&path) {
            repos.push(path);
            continue;
        }
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) => {
                debug!(
                    "skipping unreadable scan directory {}: {error}",
                    path.display()
                );
                continue;
            }
        };
        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            let name = entry.file_name();
            if should_prune_scan_dir(&name) {
                continue;
            }
            stack.push(entry.path());
        }
    }
    repos.sort();
    Ok(repos)
}

fn is_git_repository_path(path: &Path) -> bool {
    path.join(REPO_VIEW_METADATA_FILE).is_file()
        || path.join(".git").exists()
        || (path.join("HEAD").is_file()
            && path.join("objects").is_dir()
            && path.join("refs").is_dir())
}

fn should_prune_scan_dir(name: &std::ffi::OsStr) -> bool {
    matches!(
        name.to_str(),
        Some(".git" | ".direnv" | ".jj" | "target" | "node_modules")
    )
}

fn repo_locator_from_origin(path: &Path) -> Result<Option<Locator>> {
    if let Some(view) = read_repo_view_metadata(path)? {
        return Ok(Some(view.locator));
    }
    let Some(origin) = git_origin_url(path)? else {
        return Ok(None);
    };
    match Locator::parse(&origin) {
        Ok(locator) => Ok(Some(locator)),
        Err(error) => {
            debug!(
                "origin for {} is not a locator-compatible Git URL: {error:#}",
                path.display()
            );
            Ok(None)
        }
    }
}

fn comparable_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn path_is_under(path: &Path, root: &Path) -> bool {
    comparable_path(path).starts_with(comparable_path(root))
}

fn git_root_commits(path: &Path) -> Result<Vec<String>> {
    git_lines(
        path,
        ["rev-list", "--max-parents=0", "--all"],
        "reading Git root commits",
    )
}

fn git_lines<const N: usize>(path: &Path, args: [&str; N], action: &str) -> Result<Vec<String>> {
    let output = git_command(path)
        .args(args)
        .output()
        .with_context(|| format!("{action} in {}", path.display()))?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    let stdout = String::from_utf8(output.stdout).context("Git commits contain invalid UTF-8")?;
    Ok(stdout.lines().map(str::to_string).collect())
}

fn shared_root_evidence(current_roots: &HashSet<String>, other_path: &Path) -> Result<Vec<String>> {
    Ok(git_root_commits(other_path)?
        .into_iter()
        .filter(|object| current_roots.contains(object))
        .take(3)
        .map(|object| format!("shared root commit {}", short_hash(&object)))
        .collect())
}

fn short_hash(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

#[cfg(not(test))]
fn notify_related_history(count: usize, locator: &Locator) {
    let body = format!(
        "{} shares Git history with {count} managed repo(s). Run `repo related list`.",
        locator.key()
    );
    match Command::new("notify-send")
        .arg("repo-manager")
        .arg(&body)
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(status) => debug!("notify-send exited with {status}"),
        Err(error) => debug!("could not run notify-send: {error}"),
    }
}

#[cfg(test)]
fn notify_related_history(_count: usize, _locator: &Locator) {}

fn apply_filesystem_move(plan: &MovePlan) -> Result<()> {
    if plan.old_path != plan.new_path {
        fs::create_dir_all(plan.new_path.parent().context("new path has no parent")?)?;
        if plan.old_path.exists() && !plan.new_path.exists() {
            fs::rename(&plan.old_path, &plan.new_path).with_context(|| {
                format!(
                    "moving {} to {}",
                    plan.old_path.display(),
                    plan.new_path.display()
                )
            })?;
        }
    }
    for alias in &plan.aliases {
        if alias.alias_path == alias.target_path {
            continue;
        }
        if alias.alias_path.exists() || alias.alias_path.is_symlink() {
            if alias.alias_path.is_dir() && !alias.alias_path.is_symlink() {
                continue;
            }
            fs::remove_file(&alias.alias_path)
                .with_context(|| format!("removing old alias {}", alias.alias_path.display()))?;
        }
        fs::create_dir_all(
            alias
                .alias_path
                .parent()
                .context("alias path has no parent")?,
        )?;
        symlink_dir(&alias.target_path, &alias.alias_path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn symlink_dir(target: &Path, alias: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, alias)
        .with_context(|| format!("symlinking {} -> {}", alias.display(), target.display()))
}

#[cfg(windows)]
fn symlink_dir(target: &Path, alias: &Path) -> Result<()> {
    std::os::windows::fs::symlink_dir(target, alias)
        .with_context(|| format!("symlinking {} -> {}", alias.display(), target.display()))
}

fn ensure_remote(cwd: &Path, name: &str, url: &str) -> Result<()> {
    if git_remote_url(cwd, name)?.is_some() {
        run_git_in(cwd, ["remote", "set-url", name, url])
    } else {
        run_git_in(cwd, ["remote", "add", name, url])
    }
}

fn fork_remote_name(locator: &Locator) -> String {
    related_remote_name("fork", locator)
}

fn related_remote_name(relationship: &str, locator: &Locator) -> String {
    format!("{}-{}", relationship, sanitize_remote_name(&locator.key()))
}

fn sanitize_remote_name(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    let mut previous_was_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_') {
            sanitized.push(ch);
            previous_was_dash = false;
        } else if !previous_was_dash {
            sanitized.push('-');
            previous_was_dash = true;
        }
    }
    sanitized.trim_matches('-').to_string()
}

fn remote_url_for_locator(existing_url: Option<&str>, locator: &Locator) -> String {
    if let Some(existing_url) = existing_url {
        let trimmed = existing_url.trim();
        let suffix = if trimmed.trim_end_matches('/').ends_with(".git") {
            ".git"
        } else {
            ""
        };

        if let Some((prefix, _)) = trimmed.split_once(':')
            && parse_scp_like(trimmed).is_some()
        {
            return format!("{prefix}:{}{suffix}", locator.remote_path);
        }

        if let Ok(mut url) = Url::parse(trimmed)
            && url.scheme() == "file"
        {
            url.set_path(&format!("/{}{}", locator.remote_path, suffix));
            return url.to_string();
        }

        if let Ok(mut url) = Url::parse(trimmed)
            && matches!(url.scheme(), "git" | "http" | "https" | "ssh")
        {
            let (host, port) = split_authority_port(&locator.authority);
            if url.set_host(Some(host)).is_ok() && url.set_port(port).is_ok() {
                url.set_path(&format!("/{}{}", locator.remote_path, suffix));
                return url.to_string();
            }
        }
    }

    format!("https://{}/{}.git", locator.authority, locator.remote_path)
}

fn split_authority_port(authority: &str) -> (&str, Option<u16>) {
    if let Some((host, port)) = authority.rsplit_once(':')
        && let Ok(port) = port.parse()
    {
        return (host, Some(port));
    }
    (authority, None)
}

fn run_git_in<I, S>(cwd: &Path, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let status = git_command(cwd)
        .args(args)
        .status()
        .with_context(|| format!("running git in {}", cwd.display()))?;
    if !status.success() {
        bail!("git command failed with status {status}");
    }
    Ok(())
}

fn git_output<const N: usize>(cwd: &Path, args: [&str; N], action: &str) -> Result<String> {
    let output = git_command(cwd)
        .args(args)
        .output()
        .with_context(|| format!("{action} in {}", cwd.display()))?;
    if !output.status.success() {
        bail!("{action} failed with status {}", output.status);
    }
    String::from_utf8(output.stdout).with_context(|| format!("{action} output is not UTF-8"))
}

fn git_output_optional<const N: usize>(
    cwd: &Path,
    args: [&str; N],
    action: &str,
) -> Result<Option<String>> {
    let output = git_command(cwd)
        .args(args)
        .output()
        .with_context(|| format!("{action} in {}", cwd.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    String::from_utf8(output.stdout)
        .map(Some)
        .with_context(|| format!("{action} output is not UTF-8"))
}

fn git_ref_exists(cwd: &Path, refname: &str) -> Result<bool> {
    let status = git_command(cwd)
        .args(["show-ref", "--verify", "--quiet", refname])
        .status()
        .with_context(|| format!("checking Git ref {refname} in {}", cwd.display()))?;
    Ok(status.success())
}

fn git_dir_ref_exists(git_dir: &Path, refname: &str) -> Result<bool> {
    let status = git_dir_command(git_dir)
        .args(["show-ref", "--verify", "--quiet", refname])
        .status()
        .with_context(|| format!("checking Git ref {refname} in {}", git_dir.display()))?;
    Ok(status.success())
}

fn git_dir_update_ref(git_dir: &Path, refname: &str, target: &str) -> Result<()> {
    let status = git_dir_command(git_dir)
        .args(["update-ref", refname, target])
        .status()
        .with_context(|| format!("updating Git ref {refname} in {}", git_dir.display()))?;
    if !status.success() {
        bail!("git update-ref failed with status {status}");
    }
    Ok(())
}

fn git_dir_output<const N: usize>(git_dir: &Path, args: [&str; N], action: &str) -> Result<String> {
    let output = git_dir_command(git_dir)
        .args(args)
        .output()
        .with_context(|| format!("{action} in {}", git_dir.display()))?;
    if !output.status.success() {
        bail!("{action} failed with status {}", output.status);
    }
    String::from_utf8(output.stdout).with_context(|| format!("{action} output is not UTF-8"))
}

fn git_command(cwd: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_PREFIX")
        .arg("-C")
        .arg(cwd);
    command
}

fn git_dir_command(git_dir: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_PREFIX")
        .arg("--git-dir")
        .arg(git_dir);
    command
}

fn set_git_config_value(config_path: &Path, key: &str, value: &str) -> Result<()> {
    let status = Command::new("git")
        .args(["config", "--file"])
        .arg(config_path)
        .arg(key)
        .arg(value)
        .status()
        .with_context(|| format!("setting {key} in {}", config_path.display()))?;
    if !status.success() {
        bail!("git config failed with status {status}");
    }
    Ok(())
}

fn unset_git_config_value(config_path: &Path, key: &str) -> Result<()> {
    let status = Command::new("git")
        .args(["config", "--file"])
        .arg(config_path)
        .args(["--unset", key])
        .status()
        .with_context(|| format!("unsetting {key} in {}", config_path.display()))?;
    if status.success() {
        return Ok(());
    }
    Ok(())
}

fn git_dir(cwd: &Path) -> Result<PathBuf> {
    let git_dir = git_output(cwd, ["rev-parse", "--git-dir"], "reading Git dir")?;
    let path = PathBuf::from(git_dir.trim());
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(cwd.join(path))
    }
}

fn git_common_dir(cwd: &Path) -> Result<PathBuf> {
    let common_dir = git_output(
        cwd,
        ["rev-parse", "--git-common-dir"],
        "reading Git common dir",
    )?;
    let common_dir = PathBuf::from(common_dir.trim());
    if common_dir.is_absolute() {
        Ok(common_dir)
    } else {
        Ok(cwd.join(common_dir))
    }
}

fn linked_worktree_paths(cwd: &Path) -> Result<Vec<PathBuf>> {
    let output = git_command(cwd)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .with_context(|| format!("listing Git worktrees in {}", cwd.display()))?;
    if !output.status.success() {
        bail!("listing Git worktrees failed with status {}", output.status);
    }
    let stdout = String::from_utf8(output.stdout).context("Git worktree output is not UTF-8")?;
    Ok(stdout
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .map(PathBuf::from)
        .collect())
}

fn is_bare_repository(path: &Path) -> Result<bool> {
    let output = git_command(path)
        .args(["rev-parse", "--is-bare-repository"])
        .output()
        .with_context(|| format!("checking whether {} is bare", path.display()))?;
    if !output.status.success() {
        return Ok(false);
    }
    let value = String::from_utf8(output.stdout).context("Git bare status is not UTF-8")?;
    Ok(value.trim() == "true")
}

fn ensure_bare_repository(path: &Path) -> Result<()> {
    if is_bare_repository(path)? {
        Ok(())
    } else {
        bail!("repository is not bare: {}", path.display())
    }
}

fn github_redirect_locator(cache_root: &Path, locator: &Locator) -> Result<Option<Locator>> {
    if locator.authority != "github.com" {
        return Ok(None);
    }
    let parts: Vec<&str> = locator.remote_path.split('/').collect();
    if parts.len() != 2 {
        return Ok(None);
    }
    if let Some(locator) = read_cached_github_locator(cache_root, locator)? {
        return Ok(Some(locator));
    }
    let api_url = format!("https://api.github.com/repos/{}/{}", parts[0], parts[1]);
    let output = Command::new("curl")
        .args([
            "-fsSL",
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "User-Agent: repo-manager",
            &api_url,
        ])
        .output();
    let Ok(output) = output else {
        return Ok(None);
    };
    if !output.status.success() {
        return Ok(None);
    }
    let body = String::from_utf8(output.stdout).context("GitHub API response is not UTF-8")?;
    write_cached_github_response(cache_root, locator, &body)?;
    github_locator_from_api_json(&body)
}

fn read_cached_github_locator(cache_root: &Path, locator: &Locator) -> Result<Option<Locator>> {
    let path = github_cache_path(cache_root, locator);
    if !path.exists() {
        return Ok(None);
    }
    let body = match fs::read_to_string(&path) {
        Ok(body) => body,
        Err(_) => return Ok(None),
    };
    Ok(github_locator_from_api_json(&body).ok().flatten())
}

fn write_cached_github_response(cache_root: &Path, locator: &Locator, body: &str) -> Result<()> {
    let path = github_cache_path(cache_root, locator);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating cache directory {}", parent.display()))?;
    }
    fs::write(&path, body).with_context(|| format!("writing cache file {}", path.display()))
}

fn github_cache_path(cache_root: &Path, locator: &Locator) -> PathBuf {
    let mut path = cache_root.join("github.com");
    let mut parts: Vec<&str> = locator.remote_path.split('/').collect();
    if let Some(leaf) = parts.pop() {
        for part in parts {
            path = path.join(part);
        }
        path.join(format!("{leaf}.json"))
    } else {
        path.join("unknown.json")
    }
}

fn github_locator_from_api_json(body: &str) -> Result<Option<Locator>> {
    let json: serde_json::Value = serde_json::from_str(body).context("parsing GitHub API JSON")?;
    let Some(full_name) = json.get("full_name").and_then(|value| value.as_str()) else {
        return Ok(None);
    };
    Ok(Some(Locator::new("github.com", full_name)?))
}

fn git_origin_url(cwd: &Path) -> Result<Option<String>> {
    git_remote_url(cwd, "origin")
}

fn git_remotes(cwd: &Path) -> Result<Vec<GitRemote>> {
    let output = git_command(cwd)
        .args(["config", "--get-regexp", r"^remote\..*\.url$"])
        .output()
        .with_context(|| format!("reading Git remotes in {}", cwd.display()))?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    let stdout = String::from_utf8(output.stdout).context("Git remote output is not UTF-8")?;
    let mut remotes = Vec::new();
    for line in stdout.lines() {
        let Some((key, url)) = line.split_once(' ') else {
            continue;
        };
        let Some(name) = key
            .strip_prefix("remote.")
            .and_then(|key| key.strip_suffix(".url"))
        else {
            continue;
        };
        let url = url.trim();
        if !name.is_empty() && !url.is_empty() {
            remotes.push(GitRemote {
                name: name.to_string(),
                url: url.to_string(),
            });
        }
    }
    remotes.sort_by(|first, second| first.name.cmp(&second.name));
    Ok(remotes)
}

fn git_remote_url(cwd: &Path, name: &str) -> Result<Option<String>> {
    let output = git_command(cwd)
        .args(["config", "--get"])
        .arg(format!("remote.{name}.url"))
        .output()
        .with_context(|| format!("reading {name} remote URL in {}", cwd.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    let url = String::from_utf8(output.stdout)
        .context("origin URL contains invalid UTF-8")?
        .trim()
        .to_string();
    Ok((!url.is_empty()).then_some(url))
}

fn output_setup(output: &Output, result: &SetupResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!("saved config: {}", result.config_path.display());
    println!("{}", result.note);
    Ok(())
}

fn output_daemon_ping(output: &Output, result: &DaemonPingResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!("repod reachable: {}", result.rpc_url);
    Ok(())
}

fn output_clone(output: &Output, result: &CloneResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!(
        "cloned {} -> {}",
        result.locator.key(),
        result.path.display()
    );
    Ok(())
}

fn output_create(output: &Output, result: &CreateResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!(
        "created {} {} repository {} -> {}",
        match result.visibility {
            RepoVisibility::Private => "private",
            RepoVisibility::Public => "public",
        },
        match result.backend {
            ForgeBackend::Github => "GitHub",
            ForgeBackend::Sourcehut => "SourceHut",
            ForgeBackend::Forgejo => "Forgejo",
        },
        result.locator.key(),
        result.path.display()
    );
    Ok(())
}

fn output_manage(output: &Output, result: &ManageResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!(
        "managed {} -> {}",
        result.locator.key(),
        result.path.display()
    );
    if let Some(moved_from) = &result.moved_from {
        println!("moved from: {}", moved_from.display());
    }
    if result.history_review_requested {
        println!("shared-history review requested via daemon");
    } else {
        println!("shared-history review not requested; daemon unavailable");
    }
    Ok(())
}

fn output_fork(output: &Output, result: &ForkResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!(
        "created fork worktree {} -> {}",
        result.fork_locator.key(),
        result.fork_path.display()
    );
    println!(
        "registered fork remote `{}` on {}",
        result.fork_remote,
        result.canonical_path.display()
    );
    if result.parent_locator != result.canonical_locator {
        println!("parent: {}", result.parent_locator.key());
        println!("canonical storage: {}", result.canonical_locator.key());
    }
    Ok(())
}

fn output_worktree(output: &Output, plan: &WorktreePlan) -> Result<()> {
    if output.json {
        return print_json(plan);
    }
    println!(
        "created worktree {} -> {}",
        plan.canonical_locator.key(),
        plan.worktree_path.display()
    );
    Ok(())
}

fn output_move(output: &Output, plan: &MovePlan) -> Result<()> {
    if output.json {
        return print_json(plan);
    }
    println!(
        "moved {} -> {}",
        plan.old_locator.key(),
        plan.new_locator.key()
    );
    println!("current path: {}", plan.new_path.display());
    for alias in &plan.aliases {
        println!(
            "alias: {} -> {}",
            alias.alias_path.display(),
            alias.target_path.display()
        );
    }
    Ok(())
}

fn output_fetch(output: &Output, result: &FetchResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!("fetched {} for {}", result.remote, result.locator.key());
    if let Some(refs_prefix) = &result.refs_prefix {
        println!("refs: {refs_prefix}");
    }
    Ok(())
}

fn output_branch(output: &Output, result: &BranchResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    if let Some(created) = &result.created {
        println!("created branch {created}");
    }
    for branch in &result.branches {
        println!("{branch}");
    }
    Ok(())
}

fn output_reconcile(output: &Output, report: &ReconcileReport) -> Result<()> {
    if output.json {
        return print_json(report);
    }
    println!("applied {} move(s)", report.planned_moves.len());
    if !report.skipped.is_empty() {
        println!("skipped {} repo(s)", report.skipped.len());
    }
    Ok(())
}

fn output_successor(output: &Output, result: &SuccessorResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!(
        "recorded successor: {} -> {}",
        result.old_ref,
        result.new_locator.key()
    );
    Ok(())
}

fn output_aliases(output: &Output, aliases: &[AliasPlan]) -> Result<()> {
    if output.json {
        return print_json(&aliases);
    }
    if aliases.is_empty() {
        println!("no aliases");
        return Ok(());
    }
    for alias in aliases {
        println!(
            "{} -> {}",
            alias.alias_path.display(),
            alias.target_path.display()
        );
    }
    Ok(())
}

fn output_repair(output: &Output, report: &RepairReport) -> Result<()> {
    if output.json {
        return print_json(report);
    }
    print!("{}", format_repair_report(report));
    Ok(())
}

fn format_repair_report(report: &RepairReport) -> String {
    let mut text = String::new();
    let needs_repair = report
        .relationships
        .iter()
        .filter(|relationship| matches!(relationship.status, RepairStatus::NeedsRepair))
        .count();
    let repaired = report
        .relationships
        .iter()
        .filter(|relationship| matches!(relationship.status, RepairStatus::Repaired))
        .count();
    let format_needs_conversion = report
        .repository_formats
        .iter()
        .filter(|format| {
            matches!(
                format.status,
                RepairRepositoryFormatStatus::NeedsBareConversion
            )
        })
        .count();
    let format_converted = report
        .repository_formats
        .iter()
        .filter(|format| matches!(format.status, RepairRepositoryFormatStatus::ConvertedToBare))
        .count();
    let format_skipped = report
        .repository_formats
        .iter()
        .filter(|format| matches!(format.status, RepairRepositoryFormatStatus::Skipped))
        .count();
    let stale_needs_prune = report
        .stale_paths
        .iter()
        .filter(|stale| matches!(stale.status, RepairStalePathStatus::NeedsPrune))
        .count();
    let stale_pruned = report
        .stale_paths
        .iter()
        .filter(|stale| matches!(stale.status, RepairStalePathStatus::Pruned))
        .count();
    let stale_blocked = report
        .stale_paths
        .iter()
        .filter(|stale| matches!(stale.status, RepairStalePathStatus::Blocked))
        .count();
    let untracked_needs_tracking = report
        .untracked_checkouts
        .iter()
        .filter(|checkout| {
            matches!(
                checkout.status,
                RepairUntrackedCheckoutStatus::NeedsTracking
            )
        })
        .count();
    let untracked_tracked = report
        .untracked_checkouts
        .iter()
        .filter(|checkout| matches!(checkout.status, RepairUntrackedCheckoutStatus::Tracked))
        .count();
    let untracked_skipped = report
        .untracked_checkouts
        .iter()
        .filter(|checkout| matches!(checkout.status, RepairUntrackedCheckoutStatus::Skipped))
        .count();
    writeln!(
        text,
        "checked {} managed relationship(s)",
        report.relationships.len() + report.skipped.len()
    )
    .unwrap();
    if report.check {
        writeln!(text, "{needs_repair} relationship(s) need repair").unwrap();
        writeln!(
            text,
            "{format_needs_conversion} repository format(s) need conversion"
        )
        .unwrap();
        writeln!(text, "{stale_needs_prune} stale path(s) need pruning").unwrap();
        writeln!(
            text,
            "{untracked_needs_tracking} unmanaged checkout(s) need tracking"
        )
        .unwrap();
    } else {
        writeln!(text, "repaired {repaired} relationship(s)").unwrap();
        writeln!(text, "converted {format_converted} repository format(s)").unwrap();
        writeln!(text, "pruned {stale_pruned} stale managed path(s)").unwrap();
        writeln!(text, "tracked {untracked_tracked} unmanaged checkout(s)").unwrap();
    }
    if format_skipped > 0 {
        writeln!(
            text,
            "{format_skipped} repository format conversion(s) skipped"
        )
        .unwrap();
    }
    if stale_blocked > 0 {
        writeln!(
            text,
            "{stale_blocked} stale path(s) blocked by existing fork/mirror checkout(s)"
        )
        .unwrap();
    }
    if untracked_skipped > 0 {
        writeln!(text, "{untracked_skipped} unmanaged checkout(s) skipped").unwrap();
    }

    let mut issue_number = 1;
    if !report.stale_paths.is_empty() {
        writeln!(
            text,
            "\nstale managed path(s): {}",
            report.stale_paths.len()
        )
        .unwrap();
        for stale in &report.stale_paths {
            let status = match stale.status {
                RepairStalePathStatus::NeedsPrune => "needs prune",
                RepairStalePathStatus::Pruned => "pruned",
                RepairStalePathStatus::Blocked => "blocked",
            };
            writeln!(text, "  [{issue_number}] {status}: {}", stale.locator.key(),).unwrap();
            writeln!(text, "      path: {}", stale.path.display()).unwrap();
            write_reasons(
                &mut text,
                &stale.reasons,
                &["recorded checkout path does not exist"],
            );
            for dependent in &stale.blocking_dependents {
                writeln!(
                    text,
                    "      blocks {}: {} {}",
                    dependent.relationship,
                    dependent.locator.key(),
                    dependent.path.display()
                )
                .unwrap();
            }
            issue_number += 1;
        }
    }
    if !report.repository_formats.is_empty() {
        let grouped_formats = grouped_repository_formats(&report.repository_formats);
        writeln!(
            text,
            "\nmanaged repository format issue(s): {}",
            grouped_formats.len()
        )
        .unwrap();
        for format in grouped_formats {
            let status = match format.status {
                RepairRepositoryFormatStatus::NeedsBareConversion => "needs bare conversion",
                RepairRepositoryFormatStatus::ConvertedToBare => "converted to bare",
                RepairRepositoryFormatStatus::Skipped => "skipped",
            };
            writeln!(
                text,
                "  [{issue_number}] {status}: {}",
                format.path.display()
            )
            .unwrap();
            writeln!(text, "      locator(s): {}", format.locators.join(", ")).unwrap();
            write_reasons(
                &mut text,
                &format.reasons,
                &["clone-as-bare is enabled but this managed clone-root repository is non-bare"],
            );
            issue_number += 1;
        }
    }
    if !report.untracked_checkouts.is_empty() {
        writeln!(
            text,
            "\nunmanaged clone-root checkout(s): {}",
            report.untracked_checkouts.len()
        )
        .unwrap();
        for checkout in &report.untracked_checkouts {
            let status = match checkout.status {
                RepairUntrackedCheckoutStatus::NeedsTracking => "needs tracking",
                RepairUntrackedCheckoutStatus::Tracked => "tracked",
                RepairUntrackedCheckoutStatus::Skipped => "skipped",
            };
            let locator = checkout
                .locator
                .as_ref()
                .map(Locator::key)
                .unwrap_or_else(|| "<unknown>".to_string());
            writeln!(text, "  [{issue_number}] {status}: {locator}").unwrap();
            writeln!(text, "      path: {}", checkout.path.display()).unwrap();
            write_reasons(&mut text, &checkout.reasons, &[]);
            issue_number += 1;
        }
    }
    let relationship_issues = report
        .relationships
        .iter()
        .filter(|relationship| !matches!(relationship.status, RepairStatus::Ok))
        .collect::<Vec<_>>();
    if !relationship_issues.is_empty() {
        writeln!(
            text,
            "\nrelationship issue(s): {}",
            relationship_issues.len()
        )
        .unwrap();
        for relationship in relationship_issues {
            let status = match relationship.status {
                RepairStatus::Ok => "ok",
                RepairStatus::NeedsRepair => "needs repair",
                RepairStatus::Repaired => "repaired",
            };
            writeln!(
                text,
                "  [{issue_number}] {status}: {} {} -> {}",
                relationship.relationship,
                relationship.dependent_locator.key(),
                relationship.controlling_locator.key()
            )
            .unwrap();
            write_reasons(
                &mut text,
                &summarized_relationship_reasons(relationship),
                &[],
            );
            issue_number += 1;
        }
    }
    if !report.skipped.is_empty() {
        writeln!(text, "\nskipped relationship(s): {}", report.skipped.len()).unwrap();
        for skipped in &report.skipped {
            writeln!(
                text,
                "  [{issue_number}] {} {} -> {}",
                skipped.relationship,
                skipped.dependent_locator.key(),
                skipped.controlling_locator.key()
            )
            .unwrap();
            writeln!(text, "      reason: {}", skipped.reason).unwrap();
            issue_number += 1;
        }
    }

    text
}

#[derive(Debug)]
struct GroupedRepositoryFormat {
    path: PathBuf,
    status: RepairRepositoryFormatStatus,
    locators: Vec<String>,
    reasons: Vec<String>,
}

fn grouped_repository_formats(formats: &[RepairRepositoryFormat]) -> Vec<GroupedRepositoryFormat> {
    let mut grouped: BTreeMap<(PathBuf, RepairRepositoryFormatStatus), GroupedRepositoryFormat> =
        BTreeMap::new();
    for format in formats {
        let key = (format.path.clone(), format.status);
        let entry = grouped
            .entry(key)
            .or_insert_with(|| GroupedRepositoryFormat {
                path: format.path.clone(),
                status: format.status,
                locators: Vec::new(),
                reasons: Vec::new(),
            });
        entry.locators.push(format.locator.key());
        for reason in &format.reasons {
            if !entry.reasons.contains(reason) {
                entry.reasons.push(reason.clone());
            }
        }
    }
    grouped.into_values().collect()
}

fn write_reasons(text: &mut String, reasons: &[String], omitted: &[&str]) {
    for reason in reasons {
        if omitted.iter().any(|omitted| *omitted == reason) {
            continue;
        }
        writeln!(text, "      reason: {reason}").unwrap();
    }
}

fn summarized_relationship_reasons(relationship: &RepairRelationship) -> Vec<String> {
    let mut summarized = Vec::new();
    let mut canonical_non_bare = false;
    let mut dependent_non_bare = false;

    for reason in &relationship.reasons {
        if reason.starts_with("canonical checkout is non-bare but clone-as-bare is enabled:") {
            canonical_non_bare = true;
        } else if reason.contains(" checkout is non-bare but clone-as-bare is enabled:") {
            dependent_non_bare = true;
        } else if !summarized.contains(reason) {
            summarized.push(reason.clone());
        }
    }

    match (canonical_non_bare, dependent_non_bare) {
        (true, true) => summarized.push(format!(
            "canonical and {} repositories are non-bare while clone-as-bare is enabled",
            relationship.relationship
        )),
        (true, false) => summarized
            .push("canonical repository is non-bare while clone-as-bare is enabled".to_string()),
        (false, true) => summarized.push(format!(
            "{} repository is non-bare while clone-as-bare is enabled",
            relationship.relationship
        )),
        (false, false) => {}
    }

    summarized
}

fn output_related(output: &Output, suggestions: &[RelatedSuggestion]) -> Result<()> {
    let report = related_list_report(suggestions);
    if output.json {
        return print_json(&report);
    }
    if report.suggestions.is_empty() {
        println!("no unresolved shared-history suggestions");
        return Ok(());
    }
    println!(
        "unresolved shared-history suggestions: {}",
        report.unresolved_count
    );
    for suggestion in &report.suggestions {
        let [repo, related] = &suggestion.repositories;
        println!();
        println!("#{}  {}", suggestion.id, repo.locator.key());
        println!("    {}", related.locator.key());
        println!("    evidence: {}", suggestion.evidence.summary);
        println!("    resolve:  {}", suggestion.resolve_command);
    }
    Ok(())
}

fn related_list_report(suggestions: &[RelatedSuggestion]) -> RelatedListReport {
    RelatedListReport {
        action: "related-list",
        unresolved_count: suggestions.len(),
        suggestions: suggestions
            .iter()
            .map(|suggestion| RelatedSuggestionReport {
                id: suggestion.id,
                repositories: [
                    RelatedRepositoryReport {
                        repo_id: suggestion.repo_id,
                        locator: suggestion.repo_locator.clone(),
                        path: suggestion.repo_path.clone(),
                    },
                    RelatedRepositoryReport {
                        repo_id: suggestion.related_repo_id,
                        locator: suggestion.related_locator.clone(),
                        path: suggestion.related_path.clone(),
                    },
                ],
                evidence: related_evidence_report(suggestion),
                resolution: suggestion.resolution.clone(),
                resolve_command: format!("repo related resolve {} <kind>", suggestion.id),
            })
            .collect(),
    }
}

fn related_evidence_report(suggestion: &RelatedSuggestion) -> RelatedEvidenceReport {
    let details = shared_root_evidence_between(&suggestion.repo_path, &suggestion.related_path)
        .inspect_err(|error| debug!("could not check shared root evidence: {error:#}"))
        .ok()
        .filter(|evidence| !evidence.is_empty())
        .or_else(|| legacy_shared_root_evidence(&suggestion.shared_refs))
        .unwrap_or_default();

    RelatedEvidenceReport {
        summary: summarize_shared_history_evidence(&details),
        details,
    }
}

fn shared_root_evidence_between(first_path: &Path, second_path: &Path) -> Result<Vec<String>> {
    let first_roots = git_root_commits(first_path)?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(git_root_commits(second_path)?
        .into_iter()
        .filter(|object| first_roots.contains(object))
        .take(3)
        .map(|object| format!("shared root commit {}", short_hash(&object)))
        .collect())
}

fn legacy_shared_root_evidence(shared_refs: &[String]) -> Option<Vec<String>> {
    let root_prefix = "shared root commit ";
    shared_refs
        .iter()
        .all(|evidence| evidence.starts_with(root_prefix))
        .then(|| shared_refs.to_vec())
}

fn summarize_shared_history_evidence(shared_refs: &[String]) -> String {
    if shared_refs.is_empty() {
        return "unknown".to_string();
    }
    shared_refs.join(", ")
}

fn output_related_resolution(output: &Output, resolution: &RelatedResolution) -> Result<()> {
    if output.json {
        return print_json(resolution);
    }
    println!(
        "resolved shared-history suggestion #{} as {}",
        resolution.id, resolution.resolution
    );
    if let Some(shared_git_dir) = &resolution.shared_git_dir {
        println!(
            "{} now reuses canonical Git directory {}",
            shared_git_dir.dependent_locator.key(),
            shared_git_dir.controlling_locator.key()
        );
        println!(
            "remote on canonical checkout: {} -> {}",
            shared_git_dir.dependent_remote, shared_git_dir.dependent_url
        );
        println!(
            "tracking branch: {} -> {}",
            shared_git_dir.local_branch, shared_git_dir.remote_branch
        );
        if shared_git_dir.converted_to_worktree {
            println!(
                "converted fork/mirror checkout to Git worktree: {}",
                shared_git_dir.dependent_path.display()
            );
        }
    }
    Ok(())
}

fn output_repo_type_change(output: &Output, result: &RepoTypeChangeResult) -> Result<()> {
    if output.json {
        return print_json(result);
    }
    println!(
        "{}: {} -> {}",
        result.id, result.previous_type, result.new_type
    );
    if let Some(canonical) = &result.repository.canonical {
        if let Some(parent) = &result.repository.parent
            && parent.id != canonical.id
        {
            println!("parent: {}", parent.id);
        }
        println!("canonical: {}", canonical.id);
    }
    if let Some(shared_git_dir) = &result.shared_git_dir {
        println!(
            "materialized {} namespace: {}",
            result.new_type, shared_git_dir.local_branch
        );
    }
    Ok(())
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn top_level_help_uses_grouped_commands_without_duplicate_command_section() {
        let mut command = Cli::command().help_template(<Commands as HelpTemplate>::help_template());
        let help = command.render_help().to_string();

        assert!(help.contains("Command groups:"));
        assert!(help.contains("Repository operations:"));
        assert!(help.contains("check"));
        assert!(!help.contains("repair"));
        assert!(help.contains("Organizational Changes:"));
        assert!(help.contains("Organizational Analysis:"));
        assert!(help.contains("Daemon:"));
        assert!(help.contains("Options:"));
        assert!(!help.contains("\nCommands:\n"));
        assert!(!help.contains("\n    audit"));
        assert!(help.find("Command groups:") < help.find("Options:"));
    }

    #[test]
    fn repo_top_level_help_advertises_daemon_ping_without_repod_controls() {
        let mut command = Cli::command().help_template(<Commands as HelpTemplate>::help_template());
        let help = command.render_help().to_string();

        assert!(help.contains("Daemon:"));
        assert!(help.contains("daemon"));
        assert!(!help.contains("--detect-related"));
        assert!(!help.contains("--clone-start-ttl-minutes"));
        assert!(!help.contains("--rpc-rate-limit-per-second"));
        assert!(help.contains("--root"));
        assert!(!help.contains("--clone-root"));
        assert!(!help.contains("--worktree-root"));
    }

    #[test]
    fn repo_daemon_help_exposes_ping_without_repod_controls() {
        let mut command = Cli::command();
        let daemon = command.find_subcommand_mut("daemon").unwrap();
        let help = daemon.render_long_help().to_string();

        assert!(help.contains("ping"));
        assert!(!help.contains("--detect-related"));
        assert!(!help.contains("--clone-start-ttl-minutes"));
        assert!(!help.contains("--rpc-rate-limit-per-second"));
    }

    #[test]
    fn dir_is_global_for_repository_context_commands() {
        let cli = Cli::try_parse_from([
            "repo",
            "--dir",
            "/tmp/example-view",
            "worktree",
            "add",
            "topic-worktree",
            "topic",
        ])
        .unwrap();

        assert_eq!(cli.dir, Some(PathBuf::from("/tmp/example-view")));
        match cli.command {
            Commands::RepositoryOperations(RepositoryOperationCommands::Worktree(command)) => {
                match command.command {
                    WorktreeSubcommand::Add(args) => {
                        assert_eq!(args.repo_or_name, "topic-worktree");
                        assert_eq!(args.name_or_start_point.as_deref(), Some("topic"));
                    }
                }
            }
            command => panic!("unexpected command: {command:?}"),
        }
    }

    #[test]
    fn manage_help_uses_canonical_prompt_options() {
        let mut command = Cli::command();
        let manage = command.find_subcommand_mut("manage").unwrap();
        let help = manage.render_long_help().to_string();

        assert!(help.contains("--assume-origin-as-canonical"));
        assert!(!help.contains("--locator"));
        assert!(!help.contains("origin or --locator"));
    }

    #[test]
    fn canonical_prompt_accepts_direct_url_or_number() {
        let remotes = vec![GitRemote {
            name: "origin".to_string(),
            url: "https://github.com/johnrichardrinehart/neovim".to_string(),
        }];

        assert_eq!(
            parse_canonical_prompt_answer("1", &remotes),
            CanonicalPromptAnswer::Remote(
                "https://github.com/johnrichardrinehart/neovim".to_string()
            )
        );
        assert_eq!(
            parse_canonical_prompt_answer("https://github.com/neovim/neovim", &remotes),
            CanonicalPromptAnswer::Remote("https://github.com/neovim/neovim".to_string())
        );
        assert_eq!(
            parse_canonical_prompt_answer("git@github.com:neovim/neovim.git", &remotes),
            CanonicalPromptAnswer::Remote("git@github.com:neovim/neovim.git".to_string())
        );
    }

    #[test]
    fn canonical_prompt_rejects_zero_and_unknown_text() {
        let remotes = vec![GitRemote {
            name: "origin".to_string(),
            url: "https://github.com/johnrichardrinehart/neovim".to_string(),
        }];

        assert_eq!(
            parse_canonical_prompt_answer("0", &remotes),
            CanonicalPromptAnswer::Invalid
        );
        assert_eq!(
            parse_canonical_prompt_answer("origin", &remotes),
            CanonicalPromptAnswer::Invalid
        );
        assert_eq!(
            parse_canonical_prompt_answer("skip", &remotes),
            CanonicalPromptAnswer::NoCanonicalRemote
        );
        assert_eq!(
            parse_canonical_prompt_answer("none", &remotes),
            CanonicalPromptAnswer::NoCanonicalRemote
        );
    }

    #[test]
    fn dependent_relationship_prompt_accepts_fork_and_mirror_aliases() {
        assert_eq!(
            parse_dependent_relationship_answer("fork"),
            Some(ManageRelationship::Fork)
        );
        assert_eq!(
            parse_dependent_relationship_answer("1"),
            Some(ManageRelationship::Fork)
        );
        assert_eq!(
            parse_dependent_relationship_answer("mirror"),
            Some(ManageRelationship::Mirror)
        );
        assert_eq!(
            parse_dependent_relationship_answer("2"),
            Some(ManageRelationship::Mirror)
        );
        assert_eq!(parse_dependent_relationship_answer("canonical"), None);
    }

    #[test]
    fn repod_help_keeps_daemon_controls() {
        let help = RepodCli::command().render_help().to_string();

        assert!(help.contains("--detect-related"));
        assert!(help.contains("--clone-start-ttl-minutes"));
        assert!(help.contains("--rpc-rate-limit-per-second"));
    }

    #[cfg(unix)]
    #[test]
    fn daemon_ping_accepts_listening_unix_socket() {
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("repo-manager.sock");
        let _listener = UnixListener::bind(&socket).unwrap();

        check_daemon_reachable(&format!("unix://{}", socket.display())).unwrap();
    }

    #[test]
    fn normalizes_common_git_urls() {
        let cases = [
            (
                "https://github.com/torvalds/linux.git",
                "github.com",
                "torvalds/linux",
            ),
            (
                "git@github.com:johnrichardrinehart/forgeproxy.git",
                "github.com",
                "johnrichardrinehart/forgeproxy",
            ),
            (
                "ssh://git@git.sr.ht/~sircmpwn/scdoc/",
                "git.sr.ht",
                "~sircmpwn/scdoc",
            ),
            (
                "ssh://git@example.com:2222/deep/path/repo.git",
                "example.com:2222",
                "deep/path/repo",
            ),
            (
                "git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
                "git.kernel.org",
                "pub/scm/linux/kernel/git/torvalds/linux",
            ),
        ];

        for (input, authority, remote_path) in cases {
            let locator = Locator::parse(input).unwrap();
            assert_eq!(locator.authority, authority);
            assert_eq!(locator.remote_path, remote_path);
        }
    }

    #[test]
    fn rejects_unsafe_remote_paths() {
        assert!(Locator::parse("github.com/../repo").is_err());
        assert!(Locator::parse("github.com/org/./repo").is_err());
    }

    #[test]
    fn create_resolves_builtin_and_configured_forges() {
        let config = test_config(Path::new("/tmp/repo-manager-test"));
        let github = resolve_create_forge(
            &config,
            &Locator::parse("https://github.com/example/project.git").unwrap(),
        )
        .unwrap();
        assert_eq!(github.backend, ForgeBackend::Github);
        assert_eq!(github.token_envs, vec!["GITHUB_TOKEN"]);
        assert_eq!(github.api_base_url, "https://api.github.com");

        let sourcehut = resolve_create_forge(
            &config,
            &Locator::parse("https://git.sr.ht/~me/tool").unwrap(),
        )
        .unwrap();
        assert_eq!(sourcehut.backend, ForgeBackend::Sourcehut);
        assert_eq!(
            sourcehut.token_envs,
            vec!["SOURCEHUT_TOKEN".to_string(), "SRHT_TOKEN".to_string()]
        );
        assert_eq!(sourcehut.api_base_url, "https://git.sr.ht/query");

        let mut configured = config;
        configured.forges.insert(
            "git.example.test".to_string(),
            ForgeConfig {
                backend: ForgeBackend::Forgejo,
                token_env: Some("EXAMPLE_FORGE_TOKEN".to_string()),
                api_base_url: Some("https://git.example.test/api-root".to_string()),
            },
        );
        let forge = resolve_create_forge(
            &configured,
            &Locator::parse("https://git.example.test/me/tool.git").unwrap(),
        )
        .unwrap();
        assert_eq!(forge.backend, ForgeBackend::Forgejo);
        assert_eq!(forge.token_envs, vec!["EXAMPLE_FORGE_TOKEN"]);
        assert_eq!(forge.api_base_url, "https://git.example.test/api-root");

        assert!(
            resolve_create_forge(
                &configured,
                &Locator::parse("https://git.unconfigured.test/me/tool.git").unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn create_visibility_prefers_cli_flags_over_config_default() {
        let mut config = test_config(Path::new("/tmp/repo-manager-test"));
        config.create_default_visibility = RepoVisibility::Public;
        assert_eq!(
            create_visibility(
                &config,
                &CreateArgs {
                    url: "github.com/me/tool".to_string(),
                    private: false,
                    public: false,
                }
            ),
            RepoVisibility::Public
        );
        assert_eq!(
            create_visibility(
                &config,
                &CreateArgs {
                    url: "github.com/me/tool".to_string(),
                    private: true,
                    public: false,
                }
            ),
            RepoVisibility::Private
        );
    }

    #[test]
    fn create_refuses_existing_local_target_before_remote_calls() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let path = config.clone_root.join("github.com/example/project");
        fs::create_dir_all(&path).unwrap();

        let error = create_repo(
            &config,
            &store,
            &Output { json: true },
            CreateArgs {
                url: "https://github.com/example/project.git".to_string(),
                private: false,
                public: false,
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("target path already exists"));
    }

    #[test]
    fn create_builds_rest_and_sourcehut_requests() {
        let github = CreateForge {
            backend: ForgeBackend::Github,
            token_envs: vec!["GITHUB_TOKEN".to_string()],
            api_base_url: "https://api.github.com".to_string(),
        };
        let parts = RemoteRepoParts {
            owner: "org".to_string(),
            name: "tool".to_string(),
        };
        let github_request = rest_repository_create_request(
            &github,
            &parts,
            RepoVisibility::Public,
            "tok",
            "me",
            true,
        );
        assert_eq!(github_request.method, "POST");
        assert_eq!(github_request.url, "https://api.github.com/orgs/org/repos");
        assert!(
            github_request
                .headers
                .contains(&("Authorization".to_string(), "Bearer tok".to_string()))
        );
        let github_body: serde_json::Value =
            serde_json::from_str(github_request.body.as_deref().unwrap()).unwrap();
        assert_eq!(github_body["name"], "tool");
        assert_eq!(github_body["private"], false);

        let forgejo = CreateForge {
            backend: ForgeBackend::Forgejo,
            token_envs: vec!["FORGEJO_TOKEN".to_string()],
            api_base_url: "https://git.example.test".to_string(),
        };
        let forgejo_request = rest_authenticated_user_request(&forgejo, "tok", false);
        assert_eq!(forgejo_request.url, "https://git.example.test/api/v1/user");
        assert!(
            forgejo_request
                .headers
                .contains(&("Authorization".to_string(), "token tok".to_string()))
        );

        let sourcehut = CreateForge {
            backend: ForgeBackend::Sourcehut,
            token_envs: vec!["SOURCEHUT_TOKEN".to_string()],
            api_base_url: "https://git.sr.ht/query".to_string(),
        };
        let sourcehut_request =
            sourcehut_create_request(&sourcehut, "tok", "tool", RepoVisibility::Private);
        assert_eq!(sourcehut_request.method, "POST");
        assert_eq!(sourcehut_request.url, "https://git.sr.ht/query");
        let sourcehut_body: serde_json::Value =
            serde_json::from_str(sourcehut_request.body.as_deref().unwrap()).unwrap();
        assert_eq!(sourcehut_body["variables"]["name"], "tool");
        assert_eq!(sourcehut_body["variables"]["visibility"], "PRIVATE");
    }

    #[test]
    fn sourcehut_create_requires_tilde_owner_paths() {
        let locator = Locator::parse("https://git.sr.ht/~john/tool").unwrap();
        assert_eq!(
            parse_sourcehut_repo(&locator).unwrap(),
            RemoteRepoParts {
                owner: "john".to_string(),
                name: "tool".to_string()
            }
        );
        assert!(
            parse_sourcehut_repo(&Locator::parse("https://git.sr.ht/john/tool").unwrap()).is_err()
        );
    }

    #[test]
    fn derives_locator_paths_from_full_remote_path() {
        let root = Path::new("/tmp/clones");
        let locator =
            Locator::parse("git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git").unwrap();
        assert_eq!(
            locator_path(root, &locator),
            PathBuf::from("/tmp/clones/git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux")
        );
    }

    #[test]
    fn move_flags_identify_authority_prefix_and_leaf_changes() {
        let old = Locator::parse("github.com/org/repo").unwrap();
        let new = Locator::parse("codeberg.org/new-org/new-repo").unwrap();
        let plan = plan_move(Path::new("/tmp/clones"), old, new, &[]);
        assert!(plan.flags.authority_changed);
        assert!(plan.flags.remote_path_changed);
        assert!(plan.flags.path_prefix_changed);
        assert!(plan.flags.leaf_name_changed);
    }

    #[test]
    fn aliases_for_repeated_churn_point_to_latest_path() {
        let first = Locator::parse("github.com/old/repo").unwrap();
        let second = Locator::parse("github.com/new/repo").unwrap();
        let third = Locator::parse("git.example.com/newer/project").unwrap();
        let plan = plan_move(
            Path::new("/tmp/clones"),
            second.clone(),
            third.clone(),
            &[first.clone(), second],
        );
        let latest = locator_path(Path::new("/tmp/clones"), &third);
        assert_eq!(plan.aliases.len(), 2);
        assert!(plan.aliases.iter().all(|alias| alias.target_path == latest));
        assert!(
            plan.aliases
                .iter()
                .any(|alias| alias.alias_path == Path::new("/tmp/clones/github.com/old/repo"))
        );
    }

    #[test]
    fn worktree_add_generates_git_like_start_point_args() {
        let locator = Locator::parse("github.com/torvalds/linux").unwrap();
        let plan = plan_worktree_add(
            Path::new("/tmp/clones"),
            Path::new("/tmp/dev-worktrees"),
            locator,
            "topic",
            WorktreeAddOptions {
                start_point: Some("origin/master"),
                branch: Some("topic-branch"),
                detach: false,
                force: true,
            },
        )
        .unwrap();
        assert_eq!(
            plan.git_args,
            vec![
                "worktree",
                "add",
                "--force",
                "-b",
                "topic-branch",
                "/tmp/dev-worktrees/github.com/torvalds/linux/topic",
                "origin/master",
            ]
        );
    }

    #[test]
    fn fork_remote_names_are_stable_and_locator_based() {
        let locator = Locator::parse("git.sr.ht/~alice/project").unwrap();
        assert_eq!(fork_remote_name(&locator), "fork-git.sr.ht-alice-project");
    }

    #[test]
    fn repair_plan_parser_accepts_pick_drop_and_deleted_lines() {
        let operations = vec![
            test_repair_operation(1, "one"),
            test_repair_operation(2, "two"),
            test_repair_operation(3, "three"),
        ];

        let selected = parse_repair_plan(
            "# comment\npick 1 one\ndrop 2 two\np 3 three\n",
            &operations,
        )
        .unwrap();

        assert!(selected.contains(&1));
        assert!(!selected.contains(&2));
        assert!(selected.contains(&3));
        assert_eq!(selected.len(), 2);
        assert!(parse_repair_plan("pick 4 unknown\n", &operations).is_err());
        assert!(parse_repair_plan("pick 1 one\npick 1 duplicate\n", &operations).is_err());
    }

    #[test]
    fn repair_plan_formatter_uses_one_line_per_operation() {
        let operations = vec![
            test_repair_operation(1, "one"),
            test_repair_operation(2, "two"),
        ];

        let text = format_repair_plan(&operations);
        let operation_lines = text
            .lines()
            .filter(|line| line.starts_with("pick "))
            .collect::<Vec<_>>();

        assert_eq!(operation_lines, vec!["pick 1 one", "pick 2 two"]);
        assert!(!text.contains("#   path:"));
        assert!(!text.contains("#   reason:"));
        assert!(!text.contains("#   locator(s):"));
    }

    #[test]
    fn repair_report_output_numbers_and_groups_redundant_format_issues() {
        let shared_path = PathBuf::from("/tmp/clones/github.com/upstream/project");
        let report = RepairReport {
            action: "check",
            check: true,
            stale_paths: vec![RepairStalePath {
                repo_id: 1,
                locator: Locator::parse("example.com/missing").unwrap(),
                path: PathBuf::from("/tmp/clones/example.com/missing"),
                status: RepairStalePathStatus::NeedsPrune,
                reasons: vec!["recorded checkout path does not exist".to_string()],
                blocking_dependents: Vec::new(),
            }],
            repository_formats: vec![
                RepairRepositoryFormat {
                    repo_id: 2,
                    locator: Locator::parse("github.com/upstream/project").unwrap(),
                    path: shared_path.clone(),
                    status: RepairRepositoryFormatStatus::NeedsBareConversion,
                    reasons: vec![
                        "clone-as-bare is enabled but this managed clone-root repository is non-bare"
                            .to_string(),
                    ],
                },
                RepairRepositoryFormat {
                    repo_id: 3,
                    locator: Locator::parse("github.com/fork/project").unwrap(),
                    path: shared_path,
                    status: RepairRepositoryFormatStatus::NeedsBareConversion,
                    reasons: vec![
                        "clone-as-bare is enabled but this managed clone-root repository is non-bare"
                            .to_string(),
                    ],
                },
            ],
            untracked_checkouts: Vec::new(),
            relationships: vec![RepairRelationship {
                relationship: "fork".to_string(),
                dependent_locator: Locator::parse("github.com/fork/project").unwrap(),
                controlling_locator: Locator::parse("github.com/upstream/project").unwrap(),
                dependent_path: PathBuf::from("/tmp/clones/github.com/fork/project"),
                controlling_path: PathBuf::from("/tmp/clones/github.com/upstream/project"),
                status: RepairStatus::NeedsRepair,
                reasons: vec![
                    "canonical checkout is non-bare but clone-as-bare is enabled: /tmp/clones/github.com/upstream/project"
                        .to_string(),
                    "fork checkout is non-bare but clone-as-bare is enabled: /tmp/clones/github.com/fork/project"
                        .to_string(),
                    "fork branch has no upstream, expected `fork-github.com-fork-project/main`"
                        .to_string(),
                ],
                shared_git_dir: None,
            }],
            skipped: Vec::new(),
        };

        let text = format_repair_report(&report);

        assert!(text.contains("[1] needs prune: example.com/missing"));
        assert!(
            text.contains("[2] needs bare conversion: /tmp/clones/github.com/upstream/project")
        );
        assert!(text.contains("locator(s): github.com/upstream/project, github.com/fork/project"));
        assert!(text.contains(
            "[3] needs repair: fork github.com/fork/project -> github.com/upstream/project"
        ));
        assert!(text.contains(
            "canonical and fork repositories are non-bare while clone-as-bare is enabled"
        ));
        assert_eq!(
            text.matches("managed clone-root repository is non-bare")
                .count(),
            0
        );
    }

    #[test]
    fn ghq_root_is_configured_with_environment() {
        let command = ghq_get_command(Path::new("/tmp/clones"), "https://github.com/owner/repo");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let envs = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(args, vec!["get", "https://github.com/owner/repo"]);
        assert!(
            envs.iter()
                .any(|(key, value)| key == "GHQ_ROOT" && value.as_deref() == Some("/tmp/clones"))
        );
    }

    fn test_repair_operation(id: usize, summary: &str) -> RepairOperation {
        RepairOperation {
            id,
            summary: summary.to_string(),
            kind: RepairOperationKind::PruneStale(RepairStalePath {
                repo_id: id as i64,
                locator: Locator::parse(&format!("example.com/{summary}")).unwrap(),
                path: PathBuf::from(format!("/tmp/{summary}")),
                status: RepairStalePathStatus::NeedsPrune,
                reasons: Vec::new(),
                blocking_dependents: Vec::new(),
            }),
        }
    }

    #[test]
    fn relocated_origin_urls_preserve_existing_style_when_possible() {
        let locator = Locator::parse("github.com/new-owner/new-name").unwrap();
        assert_eq!(
            remote_url_for_locator(Some("https://github.com/old-owner/old-name.git"), &locator),
            "https://github.com/new-owner/new-name.git"
        );
        assert_eq!(
            remote_url_for_locator(Some("git@github.com:old-owner/old-name.git"), &locator),
            "git@github.com:new-owner/new-name.git"
        );
        assert_eq!(
            remote_url_for_locator(
                Some("ssh://git@github.com/old-owner/old-name.git"),
                &locator
            ),
            "ssh://git@github.com/new-owner/new-name.git"
        );
    }

    #[test]
    fn store_records_successor_without_rename_alias() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("repos.sqlite")).unwrap();
        store
            .record_successor(
                "github.com/old/project",
                &Locator::parse("github.com/new/project").unwrap(),
            )
            .unwrap();
        assert!(store.find_repo("github.com/old/project").unwrap().is_none());
    }

    #[test]
    fn clone_repo_honors_bare_clone_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.clone_as_bare = true;
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "bare clone\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        let url = file_url_for_path(&seed);
        let locator = Locator::parse(&url).unwrap();
        let clone_path = locator_path(&config.clone_root, &locator);

        clone_repo(&config, &store, &Output { json: true }, &url).unwrap();

        assert!(is_bare_repository(&clone_path).unwrap());
        assert!(!clone_path.join(".git").exists());
        assert!(store.find_repo(&locator.key()).unwrap().is_some());
    }

    #[test]
    fn fork_repo_honors_bare_clone_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.clone_as_bare = true;
        let store = Store::open(&config.state).unwrap();
        let canonical_seed = dir.path().join("canonical-seed");
        let fork_seed = dir.path().join("fork-seed");
        fs::create_dir_all(&canonical_seed).unwrap();
        run_git_in(&canonical_seed, ["init"]).unwrap();
        run_git_in(&canonical_seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(canonical_seed.join("README.md"), "canonical\n").unwrap();
        run_git_in(&canonical_seed, ["add", "."]).unwrap();
        run_git_in(
            &canonical_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&canonical_seed, &fork_seed);
        fs::write(fork_seed.join("fork.txt"), "fork\n").unwrap();
        run_git_in(&fork_seed, ["add", "."]).unwrap();
        run_git_in(
            &fork_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "fork",
            ],
        )
        .unwrap();
        let canonical_url = file_url_for_path(&canonical_seed);
        let fork_url = file_url_for_path(&fork_seed);
        let canonical_locator = Locator::parse(&canonical_url).unwrap();
        let fork_locator = Locator::parse(&fork_url).unwrap();
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        fs::create_dir_all(canonical_path.parent().unwrap()).unwrap();
        run_git_clone(&canonical_url, &canonical_path, true).unwrap();
        store
            .upsert_repo(&canonical_locator, &canonical_path, None)
            .unwrap();

        fork_repo(
            &config,
            &store,
            &Output { json: true },
            &fork_url,
            &canonical_url,
        )
        .unwrap();

        let view = read_repo_view_metadata(&fork_path).unwrap().unwrap();
        assert_eq!(view.locator, fork_locator);
        assert_eq!(view.canonical_locator, canonical_locator);
        assert_eq!(view.canonical_path, canonical_path);
        assert!(
            git_dir_ref_exists(
                &view.canonical_path,
                &format!("{}/remotes/origin/main", view.refs_prefix)
            )
            .unwrap()
        );
        assert!(!fork_path.join("objects").exists());
        assert!(!fork_path.join(".git").exists());
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM forks", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn namespace_view_fetch_branch_and_worktree_use_canonical_storage() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.clone_as_bare = true;
        let store = Store::open(&config.state).unwrap();
        let canonical_seed = dir.path().join("canonical-seed");
        let fork_seed = dir.path().join("fork-seed");
        fs::create_dir_all(&canonical_seed).unwrap();
        run_git_in(&canonical_seed, ["init"]).unwrap();
        run_git_in(&canonical_seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(canonical_seed.join("README.md"), "canonical\n").unwrap();
        run_git_in(&canonical_seed, ["add", "."]).unwrap();
        run_git_in(
            &canonical_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&canonical_seed, &fork_seed);
        fs::write(fork_seed.join("fork.txt"), "fork\n").unwrap();
        run_git_in(&fork_seed, ["add", "."]).unwrap();
        run_git_in(
            &fork_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "fork",
            ],
        )
        .unwrap();
        let canonical_url = file_url_for_path(&canonical_seed);
        let fork_url = file_url_for_path(&fork_seed);
        let canonical_locator = Locator::parse(&canonical_url).unwrap();
        let fork_locator = Locator::parse(&fork_url).unwrap();
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        let fork_path = locator_path(&config.clone_root, &fork_locator);

        fork_repo(
            &config,
            &store,
            &Output { json: true },
            &fork_url,
            &canonical_url,
        )
        .unwrap();
        let view = read_repo_view_metadata(&fork_path).unwrap().unwrap();

        fs::write(fork_seed.join("fork2.txt"), "fork2\n").unwrap();
        run_git_in(&fork_seed, ["add", "."]).unwrap();
        run_git_in(
            &fork_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "fork2",
            ],
        )
        .unwrap();
        fetch_repo(
            &config,
            &store,
            &Output { json: true },
            Some(&fork_path),
            FetchArgs {
                remote: "origin".to_string(),
            },
        )
        .unwrap();
        let fetched = git_dir_output(
            &canonical_path,
            [
                "rev-parse",
                &format!("{}/remotes/origin/main", view.refs_prefix),
            ],
            "reading fetched fork ref",
        )
        .unwrap();
        assert_eq!(
            fetched.trim(),
            git_output(&fork_seed, ["rev-parse", "HEAD"], "reading fork head")
                .unwrap()
                .trim()
        );

        branch_repo(
            &config,
            &store,
            &Output { json: true },
            Some(&fork_path),
            BranchArgs {
                branch: Some("topic".to_string()),
                start_point: Some("origin/main".to_string()),
            },
        )
        .unwrap();
        let topic_ref = format!("{}/heads/topic", view.refs_prefix);
        assert!(git_dir_ref_exists(&canonical_path, &topic_ref).unwrap());

        add_worktree(
            &config,
            &store,
            &Output { json: true },
            Some(&fork_path),
            WorktreeAddArgs {
                repo_or_name: "topic-worktree".to_string(),
                name_or_start_point: Some("topic".to_string()),
                start_point: None,
                branch: None,
                detach: false,
                force: false,
                reset: false,
            },
        )
        .unwrap();
        let worktree_path =
            locator_path(&config.dev_worktree_root, &fork_locator).join("topic-worktree");
        let git_dir = fs::read_to_string(worktree_path.join(".git"))
            .unwrap()
            .trim()
            .strip_prefix("gitdir: ")
            .map(PathBuf::from)
            .unwrap();
        let git_dir = if git_dir.is_absolute() {
            git_dir
        } else {
            worktree_path.join(git_dir)
        };
        assert_eq!(
            fs::read_to_string(git_dir.join("HEAD")).unwrap(),
            format!("ref: {topic_ref}\n")
        );
        let dump = check_dump_report(&config, &store).unwrap();
        let worktree = dump
            .git_directories
            .iter()
            .find(|entry| entry.path == worktree_path)
            .unwrap();
        assert!(worktree.tracked);
        assert!(worktree.managed);
        assert_eq!(
            worktree.repository.as_deref(),
            Some(repository_path_id(&config, &fork_path).as_str())
        );
        assert_eq!(worktree.repository_type.as_deref(), Some("fork"));
        assert_eq!(
            worktree.namespace.as_deref(),
            Some(view.refs_prefix.as_str())
        );

        assert!(!fork_path.join("objects").exists());
    }

    #[test]
    fn manage_moves_existing_repo_from_subdirectory_and_registers_it() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let seed = dir.path().join("seed");
        let current_path = dir.path().join("imports/current");
        let managed_path = config.clone_root.join("example.com/current");
        let other_path = config.clone_root.join("example.com/other");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &current_path);
        clone_local_repo(&seed, &other_path);
        run_git_in(
            &current_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/current.git",
            ],
        )
        .unwrap();
        run_git_in(
            &current_path,
            [
                "remote",
                "add",
                "upstream",
                "https://example.com/upstream.git",
            ],
        )
        .unwrap();
        run_git_in(
            &other_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/other.git",
            ],
        )
        .unwrap();
        let nested = current_path.join("nested");
        fs::create_dir_all(&nested).unwrap();

        let store = Store::open(&config.state).unwrap();
        manage_repo(
            &config,
            &store,
            &Output { json: true },
            ManageArgs {
                path: nested,
                assume_origin_as_canonical: true,
            },
        )
        .unwrap();

        assert!(!current_path.exists());
        assert!(managed_path.exists());
        assert!(store.find_repo("example.com/current").unwrap().is_some());
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM forks", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(store.related_suggestions(true).unwrap().is_empty());
    }

    #[test]
    fn manage_moves_checkout_when_path_locator_differs_from_origin() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let repo_path = config.clone_root.join("wrong/place");
        let managed_path = config.clone_root.join("example.com/right");
        fs::create_dir_all(&repo_path).unwrap();
        run_git_in(&repo_path, ["init"]).unwrap();
        run_git_in(
            &repo_path,
            ["remote", "add", "origin", "https://example.com/right.git"],
        )
        .unwrap();
        let store = Store::open(&config.state).unwrap();

        manage_repo(
            &config,
            &store,
            &Output { json: true },
            ManageArgs {
                path: repo_path,
                assume_origin_as_canonical: true,
            },
        )
        .unwrap();

        assert!(managed_path.exists());
        assert!(store.find_repo("example.com/right").unwrap().is_some());
    }

    #[test]
    fn manage_prunes_empty_source_parent_directories_under_root() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let source_path = config.root.join("repos/codeberg.org/dnkl/fuzzel");
        let managed_path = config.clone_root.join("codeberg.org/dnkl/fuzzel");
        fs::create_dir_all(&source_path).unwrap();
        run_git_in(&source_path, ["init"]).unwrap();
        run_git_in(
            &source_path,
            [
                "remote",
                "add",
                "origin",
                "https://codeberg.org/dnkl/fuzzel.git",
            ],
        )
        .unwrap();
        let store = Store::open(&config.state).unwrap();

        manage_repo(
            &config,
            &store,
            &Output { json: true },
            ManageArgs {
                path: source_path.clone(),
                assume_origin_as_canonical: true,
            },
        )
        .unwrap();

        assert!(managed_path.exists());
        assert!(!source_path.exists());
        assert!(!config.root.join("repos/codeberg.org").exists());
        assert!(config.root.exists());
        assert!(
            store
                .find_repo("codeberg.org/dnkl/fuzzel")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn manage_records_unmaterialized_canonical_for_dependent_fork() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let fork_locator = Locator::parse("github.com/johnrichardrinehart/neovim").unwrap();
        let canonical_locator = Locator::parse("github.com/neovim/neovim").unwrap();
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        fs::create_dir_all(&fork_path).unwrap();
        run_git_in(&fork_path, ["init"]).unwrap();

        record_manage_remote_relationships(
            &config,
            &store,
            ManageRemoteRelationship {
                checkout_locator: &fork_locator,
                canonical_locator: &canonical_locator,
                checkout_url: "https://github.com/johnrichardrinehart/neovim",
                canonical_url: "https://github.com/neovim/neovim",
                repo_root: &fork_path,
                remotes: &[],
                relationship: ManageRelationship::Fork,
                materialize_canonical: false,
            },
        )
        .unwrap();

        let fork = store
            .find_repo("github.com/johnrichardrinehart/neovim")
            .unwrap()
            .unwrap();
        let canonical = store
            .find_repo("github.com/neovim/neovim")
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT current_path FROM repos WHERE id = ?1",
                    params![fork.id],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            fork_path.display().to_string()
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT current_path FROM repos WHERE id = ?1",
                    params![canonical.id],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            canonical_path.display().to_string()
        );
        assert!(!canonical_path.exists());
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM forks", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn manage_records_unmaterialized_canonical_for_dependent_mirror() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let mirror_locator = Locator::parse("github.com/example/mirror").unwrap();
        let canonical_locator = Locator::parse("github.com/example/canonical").unwrap();
        let mirror_path = locator_path(&config.clone_root, &mirror_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        fs::create_dir_all(&mirror_path).unwrap();
        run_git_in(&mirror_path, ["init"]).unwrap();

        record_manage_remote_relationships(
            &config,
            &store,
            ManageRemoteRelationship {
                checkout_locator: &mirror_locator,
                canonical_locator: &canonical_locator,
                checkout_url: "https://github.com/example/mirror",
                canonical_url: "https://github.com/example/canonical",
                repo_root: &mirror_path,
                remotes: &[],
                relationship: ManageRelationship::Mirror,
                materialize_canonical: false,
            },
        )
        .unwrap();

        let relationships = store.shared_git_dir_relationships().unwrap();
        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].relationship, "mirror");
        assert_eq!(relationships[0].dependent_locator, mirror_locator);
        assert_eq!(relationships[0].controlling_locator, canonical_locator);
        assert_eq!(relationships[0].dependent_path, mirror_path);
        assert_eq!(relationships[0].controlling_path, canonical_path);
        assert!(!canonical_path.exists());
    }

    #[test]
    fn manage_seeds_missing_canonical_from_dirty_dependent_checkout() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let canonical_seed = dir.path().join("canonical-seed");
        let fork_seed = dir.path().join("fork-seed");
        fs::create_dir_all(&canonical_seed).unwrap();
        run_git_in(&canonical_seed, ["init"]).unwrap();
        run_git_in(&canonical_seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(canonical_seed.join("README.md"), "canonical\n").unwrap();
        run_git_in(&canonical_seed, ["add", "."]).unwrap();
        run_git_in(
            &canonical_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&canonical_seed, &fork_seed);
        fs::write(fork_seed.join("fork.txt"), "fork\n").unwrap();
        run_git_in(&fork_seed, ["add", "."]).unwrap();
        run_git_in(
            &fork_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "fork",
            ],
        )
        .unwrap();

        let fork_locator = Locator::parse("localhost/tmp/fork-seed").unwrap();
        let canonical_locator = Locator::parse("localhost/tmp/canonical-seed").unwrap();
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        clone_local_repo(&fork_seed, &fork_path);
        fs::write(fork_path.join("README.md"), "dirty worktree\n").unwrap();
        fs::write(fork_path.join("staged.txt"), "staged\n").unwrap();
        run_git_in(&fork_path, ["add", "staged.txt"]).unwrap();
        fs::write(fork_path.join("untracked.txt"), "untracked\n").unwrap();
        fs::write(fork_path.join(".gitignore"), "ignored.txt\n").unwrap();
        fs::write(fork_path.join("ignored.txt"), "ignored\n").unwrap();
        run_git_in(&fork_path, ["tag", "local-only"]).unwrap();

        let fork_url = format!("file://localhost{}", fork_seed.display());
        let canonical_url = format!("file://localhost{}", canonical_seed.display());
        let resolution = seed_canonical_from_dependent_checkout(
            &store,
            SeedCanonicalPlan {
                dependent_locator: &fork_locator,
                dependent_path: &fork_path,
                dependent_url: &fork_url,
                controlling_locator: &canonical_locator,
                controlling_path: &canonical_path,
                controlling_url: &canonical_url,
                relationship: "fork",
            },
        )
        .unwrap();

        assert!(resolution.converted_to_worktree);
        assert_eq!(
            git_common_dir(&fork_path).unwrap(),
            git_common_dir(&canonical_path).unwrap()
        );
        assert_eq!(
            git_remote_url(&fork_path, "origin").unwrap(),
            Some(canonical_url)
        );
        assert_eq!(
            git_remote_url(&fork_path, "fork-localhost-tmp-fork-seed").unwrap(),
            Some(fork_url)
        );
        assert_eq!(
            fs::read_to_string(fork_path.join("README.md")).unwrap(),
            "dirty worktree\n"
        );
        assert_eq!(
            fs::read_to_string(fork_path.join("untracked.txt")).unwrap(),
            "untracked\n"
        );
        assert_eq!(
            fs::read_to_string(fork_path.join("ignored.txt")).unwrap(),
            "ignored\n"
        );
        let status = git_output(
            &fork_path,
            ["status", "--porcelain=v1", "--ignored"],
            "reading fork status",
        )
        .unwrap();
        assert!(status.lines().any(|line| line == " M README.md"));
        assert!(status.lines().any(|line| line == "A  staged.txt"));
        assert!(status.lines().any(|line| line == "?? untracked.txt"));
        assert!(status.lines().any(|line| line == "!! ignored.txt"));
        assert!(
            git_output(
                &canonical_path,
                ["show-ref", "--tags", "local-only"],
                "reading local tag",
            )
            .unwrap()
            .contains("refs/tags/local-only")
        );
        assert!(
            git_output(
                &canonical_path,
                ["status", "--porcelain=v1"],
                "reading canonical status",
            )
            .unwrap()
            .trim()
            .is_empty()
        );
        assert_eq!(
            fs::read_to_string(canonical_path.join("README.md")).unwrap(),
            "canonical\n"
        );
    }

    #[test]
    fn repair_fetches_dependent_remote_before_setting_shared_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let canonical_seed = dir.path().join("canonical-seed");
        let fork_seed = dir.path().join("fork-seed");
        fs::create_dir_all(&canonical_seed).unwrap();
        run_git_in(&canonical_seed, ["init"]).unwrap();
        run_git_in(&canonical_seed, ["checkout", "-b", "master"]).unwrap();
        fs::write(canonical_seed.join("README.md"), "canonical\n").unwrap();
        run_git_in(&canonical_seed, ["add", "."]).unwrap();
        run_git_in(
            &canonical_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&canonical_seed, &fork_seed);
        fs::write(fork_seed.join("fork.txt"), "fork\n").unwrap();
        run_git_in(&fork_seed, ["add", "."]).unwrap();
        run_git_in(
            &fork_seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "fork",
            ],
        )
        .unwrap();

        let fork_url = format!("file://localhost{}", fork_seed.display());
        let fork_locator = Locator::parse(&format!("localhost{}", fork_seed.display())).unwrap();
        let canonical_locator =
            Locator::parse(&format!("localhost{}", canonical_seed.display())).unwrap();
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        clone_local_repo(&fork_seed, &canonical_path);
        run_git_in(&canonical_path, ["remote", "set-url", "origin", &fork_url]).unwrap();
        let local_branch = dependent_local_branch("fork", &fork_locator, "master");
        run_git_in(&canonical_path, ["branch", &local_branch, "master"]).unwrap();
        run_git_in(
            &canonical_path,
            [
                "worktree",
                "add",
                &fork_path.display().to_string(),
                &local_branch,
            ],
        )
        .unwrap();

        materialize_related_shared_git_dir(
            &store,
            &fork_locator,
            &fork_path,
            &canonical_locator,
            &canonical_path,
            "fork",
            false,
        )
        .unwrap();

        assert_eq!(
            git_remote_url(&canonical_path, "origin").unwrap(),
            Some(remote_url_for_locator(Some(&fork_url), &canonical_locator))
        );
        assert_eq!(
            git_output(
                &fork_path,
                [
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}"
                ],
                "reading fork upstream"
            )
            .unwrap()
            .trim(),
            &format!("{}/master", related_remote_name("fork", &fork_locator))
        );
    }

    #[test]
    fn manage_rejects_unlocatable_checkout() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let repo_path = dir.path().join("outside");
        fs::create_dir_all(&repo_path).unwrap();
        run_git_in(&repo_path, ["init"]).unwrap();
        let store = Store::open(&config.state).unwrap();

        let error = manage_repo(
            &config,
            &store,
            &Output { json: true },
            ManageArgs {
                path: repo_path,
                assume_origin_as_canonical: true,
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("prompt response ended"));
    }

    #[test]
    fn reconcile_applies_origin_locator_drift() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("code");
        let clone_root = clone_root_for(&root);
        let dev_worktree_root = dev_worktree_root_for(&root);
        let old_locator = Locator::parse("example.com/old/project").unwrap();
        let repo_path = locator_path(&clone_root, &old_locator);
        fs::create_dir_all(&repo_path).unwrap();
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&repo_path)
                .arg("init")
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&repo_path)
                .args([
                    "remote",
                    "add",
                    "origin",
                    "https://github.com/new/project.git"
                ])
                .status()
                .unwrap()
                .success()
        );

        let store = Store::open(&dir.path().join("repos.sqlite")).unwrap();
        store.upsert_repo(&old_locator, &repo_path, None).unwrap();
        let config = Config {
            config_path: dir.path().join("config.json"),
            state: dir.path().join("repos.sqlite"),
            cache_root: dir.path().join("cache"),
            root,
            clone_root,
            dev_worktree_root,
            rpc_url: test_rpc_url(dir.path()),
            client_id: generate_client_id().unwrap(),
            assume_origin_as_canonical: false,
            clone_as_bare: false,
            create_default_visibility: RepoVisibility::Private,
            forges: HashMap::new(),
        };

        let report = reconcile_repos(&config, &store).unwrap();
        assert_eq!(report.planned_moves.len(), 1);
        assert_eq!(
            report.planned_moves[0].plan.new_locator,
            Locator::parse("github.com/new/project").unwrap()
        );
        assert!(report.skipped.is_empty());
    }

    #[test]
    fn reconcile_updates_origin_for_forge_redirect() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("code");
        let clone_root = clone_root_for(&root);
        let dev_worktree_root = dev_worktree_root_for(&root);
        let old_locator = Locator::parse("github.com/old-owner/old-name").unwrap();
        let new_locator = Locator::parse("github.com/new-owner/new-name").unwrap();
        let old_path = locator_path(&clone_root, &old_locator);
        let new_path = locator_path(&clone_root, &new_locator);
        fs::create_dir_all(&old_path).unwrap();
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&old_path)
                .arg("init")
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&old_path)
                .args([
                    "remote",
                    "add",
                    "origin",
                    "https://github.com/old-owner/old-name.git"
                ])
                .status()
                .unwrap()
                .success()
        );

        let cache_root = dir.path().join("cache");
        write_cached_github_response(
            &cache_root,
            &old_locator,
            r#"{"full_name":"new-owner/new-name"}"#,
        )
        .unwrap();
        let store = Store::open(&dir.path().join("repos.sqlite")).unwrap();
        store.upsert_repo(&old_locator, &old_path, None).unwrap();
        let config = Config {
            config_path: dir.path().join("config.json"),
            state: dir.path().join("repos.sqlite"),
            cache_root,
            root,
            clone_root,
            dev_worktree_root,
            rpc_url: test_rpc_url(dir.path()),
            client_id: generate_client_id().unwrap(),
            assume_origin_as_canonical: false,
            clone_as_bare: false,
            create_default_visibility: RepoVisibility::Private,
            forges: HashMap::new(),
        };

        let report = reconcile_repos(&config, &store).unwrap();
        assert_eq!(report.planned_moves.len(), 1);
        assert!(new_path.exists());
        assert_eq!(
            git_origin_url(&new_path).unwrap().unwrap(),
            "https://github.com/new-owner/new-name.git"
        );
    }

    #[test]
    fn parses_github_api_full_name_as_locator() {
        let locator =
            github_locator_from_api_json(r#"{"id":123,"full_name":"new-owner/new-name"}"#)
                .unwrap()
                .unwrap();
        assert_eq!(
            locator,
            Locator::parse("github.com/new-owner/new-name").unwrap()
        );
    }

    #[test]
    fn file_config_loads_and_cli_values_override_it() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config/config.json");
        let content = serde_json::json!({
            "state": dir.path().join("state/from-file.sqlite"),
            "cache_root": dir.path().join("cache/from-file"),
            "root": dir.path().join("code/from-file"),
            "rpc_url": "unix:///tmp/repo-manager-from-file.sock",
            "client_id": "00000000-0000-4000-8000-000000000001",
            "assume_origin_as_canonical": false,
            "clone-as-bare": true,
            "detect_related": true,
            "clone_start_ttl_minutes": 45,
            "rpc_rate_limit_per_second": 7,
            "background-fetch-minimum-interval-seconds": 3600,
            "create_default_visibility": "public",
            "forges": {
                "git.example.test": {
                    "backend": "forgejo",
                    "token-env": "EXAMPLE_FORGE_TOKEN",
                    "api-base-url": "https://git.example.test"
                }
            }
        });
        fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        fs::write(&config_path, format!("{content}\n")).unwrap();
        let _expected = FileConfig {
            config_version: None,
            state: Some(dir.path().join("state/from-file.sqlite")),
            cache_root: Some(dir.path().join("cache/from-file")),
            root: Some(dir.path().join("code/from-file")),
            rpc_url: Some("unix:///tmp/repo-manager-from-file.sock".to_string()),
            client_id: Some("00000000-0000-4000-8000-000000000001".to_string()),
            assume_origin_as_canonical: Some(false),
            clone_as_bare: None,
            detect_related: Some(true),
            clone_start_ttl_minutes: Some(45),
            rpc_rate_limit_per_second: Some(7),
            background_fetch_minimum_interval_seconds: None,
            create_default_visibility: Some(RepoVisibility::Public),
            forges: Some(HashMap::from([(
                "git.example.test".to_string(),
                ForgeConfig {
                    backend: ForgeBackend::Forgejo,
                    token_env: Some("EXAMPLE_FORGE_TOKEN".to_string()),
                    api_base_url: Some("https://git.example.test".to_string()),
                },
            )])),
        };

        let cli = Cli {
            config: ConfigArgs {
                config: Some(config_path.clone()),
                state: None,
                cache_root: Some(dir.path().join("cache/from-cli")),
                root: Some(dir.path().join("code/from-cli")),
                rpc_url: Some("unix:///tmp/repo-manager-from-cli.sock".to_string()),
                client_id: Some("00000000-0000-4000-8000-000000000002".to_string()),
                assume_origin_as_canonical: Some(true),
            },
            dir: None,
            json: false,
            command: Commands::Setup(SetupCommands::Setup(SetupArgs {
                file: None,
                state: None,
                cache_root: None,
                root: None,
                rpc_url: None,
                client_id: None,
                assume_origin_as_canonical: None,
            })),
        };
        let config = Config::from_cli(&cli).unwrap();

        assert_eq!(config.config_path, config_path);
        assert_eq!(config.state, dir.path().join("state/from-file.sqlite"));
        assert_eq!(config.cache_root, dir.path().join("cache/from-cli"));
        assert_eq!(config.root, dir.path().join("code/from-cli"));
        assert_eq!(config.clone_root, dir.path().join("code/from-cli/clones"));
        assert_eq!(
            config.dev_worktree_root,
            dir.path().join("code/from-cli/dev-worktrees")
        );
        assert_eq!(config.rpc_url, "unix:///tmp/repo-manager-from-cli.sock");
        assert_eq!(config.client_id, "00000000-0000-4000-8000-000000000002");
        assert!(config.assume_origin_as_canonical);
        assert!(config.clone_as_bare);
        assert_eq!(config.create_default_visibility, RepoVisibility::Public);
        assert_eq!(
            config
                .forges
                .get("git.example.test")
                .and_then(|forge| forge.token_env.as_deref()),
            Some("EXAMPLE_FORGE_TOKEN")
        );
        let (_daemon_config, _rpc_url) = DaemonConfig::from_args(&DaemonConfigArgs {
            config: Some(config_path),
            state: None,
            rpc_url: None,
            detect_related: None,
            clone_start_ttl_minutes: None,
            rpc_rate_limit_per_second: None,
            background_fetch_minimum_interval_seconds: None,
        })
        .unwrap();
        assert_eq!(
            _daemon_config.background_fetch_minimum_interval_seconds,
            Some(3600)
        );
    }

    #[test]
    fn config_schema_v1_accepts_compatibility_fixtures() {
        let schema: serde_json::Value = serde_json::from_str(CONFIG_SCHEMA_V1).unwrap();
        jsonschema::validator_for(&schema).unwrap();

        let fixtures = [
            serde_json::json!({}),
            serde_json::json!({
                "root": "/home/test/code",
                "state": "/home/test/.local/state/repo-manager/repos.sqlite",
                "detect_related": true
            }),
            serde_json::json!({
                "config_version": 1,
                "clone_as_bare": true,
                "background_fetch_minimum_interval_seconds": 3600
            }),
            serde_json::json!({
                "config-version": 1,
                "clone-as-bare": true,
                "background-fetch-minimum-interval-seconds": 3600
            }),
            serde_json::json!({
                "config_version": null,
                "clone_as_bare": null,
                "background_fetch_minimum_interval_seconds": null
            }),
            serde_json::json!({
                "config_version": 1,
                "create_default_visibility": "public",
                "forges": {
                    "git.example.test": {
                        "backend": "forgejo",
                        "token_env": "EXAMPLE_FORGE_TOKEN",
                        "api_base_url": "https://git.example.test"
                    }
                }
            }),
        ];

        for fixture in fixtures {
            validate_config_json(&fixture).unwrap();
        }
    }

    #[test]
    fn config_schema_rejects_breaking_or_unknown_config_shapes() {
        assert!(validate_config_json(&serde_json::json!({ "config_version": 2 })).is_err());
        assert!(validate_config_json(&serde_json::json!({ "unknown": true })).is_err());
        assert!(
            validate_config_json(
                &serde_json::json!({ "background_fetch_minimum_interval_seconds": -1 })
            )
            .is_err()
        );
        assert!(
            validate_config_json(&serde_json::json!({ "create_default_visibility": "protected" }))
                .is_err()
        );
        assert!(
            validate_config_json(
                &serde_json::json!({ "forges": { "git.example.test": { "backend": "unknown" } } })
            )
            .is_err()
        );
    }

    #[test]
    fn repod_rejects_invalid_versioned_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.json");
        fs::write(&config_path, r#"{"config_version":2}"#).unwrap();

        let error = DaemonConfig::from_args(&DaemonConfigArgs {
            config: Some(config_path),
            state: None,
            rpc_url: None,
            detect_related: None,
            clone_start_ttl_minutes: None,
            rpc_rate_limit_per_second: None,
            background_fetch_minimum_interval_seconds: None,
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unsupported repo-manager config version 2")
        );
    }

    #[test]
    fn daemon_shared_history_detection_defaults_to_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("missing/config.json");
        let (daemon_config, _rpc_url) = DaemonConfig::from_args(&DaemonConfigArgs {
            config: Some(config_path),
            state: None,
            rpc_url: None,
            detect_related: None,
            clone_start_ttl_minutes: None,
            rpc_rate_limit_per_second: None,
            background_fetch_minimum_interval_seconds: None,
        })
        .unwrap();

        assert!(daemon_config.detect_related);
    }

    #[test]
    fn setup_can_write_an_explicit_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            config_path: dir.path().join("default/config.json"),
            state: dir.path().join("state/repos.sqlite"),
            cache_root: dir.path().join("cache"),
            root: dir.path().join("code"),
            clone_root: dir.path().join("code/clones"),
            dev_worktree_root: dir.path().join("code/dev-worktrees"),
            rpc_url: test_rpc_url(dir.path()),
            client_id: "00000000-0000-4000-8000-000000000003".to_string(),
            assume_origin_as_canonical: false,
            clone_as_bare: false,
            create_default_visibility: RepoVisibility::Private,
            forges: HashMap::new(),
        };
        let explicit_file = dir.path().join("custom/repo-config.json");

        setup_config(
            &config,
            &Output { json: true },
            SetupArgs {
                file: Some(explicit_file.clone()),
                state: None,
                cache_root: None,
                root: Some(dir.path().join("custom-root")),
                rpc_url: Some("unix:///tmp/repo-manager-explicit.sock".to_string()),
                client_id: Some("00000000-0000-4000-8000-000000000004".to_string()),
                assume_origin_as_canonical: Some(true),
            },
        )
        .unwrap();

        assert!(!config.config_path.exists());
        let saved = FileConfig::load(&explicit_file).unwrap();
        assert_eq!(saved.config_version, Some(CURRENT_CONFIG_VERSION));
        assert_eq!(saved.state, Some(config.state));
        assert_eq!(saved.cache_root, Some(config.cache_root));
        assert_eq!(saved.root, Some(dir.path().join("custom-root")));
        assert_eq!(
            saved.rpc_url,
            Some("unix:///tmp/repo-manager-explicit.sock".to_string())
        );
        assert_eq!(
            saved.client_id,
            Some("00000000-0000-4000-8000-000000000004".to_string())
        );
        assert_eq!(saved.assume_origin_as_canonical, Some(true));
        assert_eq!(saved.detect_related, None);
        assert_eq!(saved.clone_start_ttl_minutes, None);
        assert_eq!(saved.rpc_rate_limit_per_second, None);
        assert_eq!(
            saved.create_default_visibility,
            Some(RepoVisibility::Private)
        );
        assert_eq!(saved.forges, None);
    }

    #[test]
    fn file_config_merge_lets_later_layers_override_earlier_ones() {
        let dir = tempfile::tempdir().unwrap();
        let mut base = FileConfig {
            config_version: None,
            state: Some(dir.path().join("state/base.sqlite")),
            cache_root: Some(dir.path().join("cache/base")),
            root: None,
            rpc_url: Some("unix:///run/base.sock".to_string()),
            client_id: None,
            assume_origin_as_canonical: Some(false),
            clone_as_bare: None,
            detect_related: Some(false),
            clone_start_ttl_minutes: Some(60),
            rpc_rate_limit_per_second: Some(1),
            background_fetch_minimum_interval_seconds: None,
            create_default_visibility: None,
            forges: None,
        };

        base.merge(FileConfig {
            config_version: Some(CURRENT_CONFIG_VERSION),
            state: None,
            cache_root: Some(dir.path().join("cache/user")),
            root: Some(dir.path().join("code/user")),
            rpc_url: None,
            client_id: Some("00000000-0000-4000-8000-000000000005".to_string()),
            assume_origin_as_canonical: Some(true),
            clone_as_bare: None,
            detect_related: Some(true),
            clone_start_ttl_minutes: Some(10),
            rpc_rate_limit_per_second: Some(9),
            background_fetch_minimum_interval_seconds: None,
            create_default_visibility: None,
            forges: None,
        });

        assert_eq!(base.state, Some(dir.path().join("state/base.sqlite")));
        assert_eq!(base.cache_root, Some(dir.path().join("cache/user")));
        assert_eq!(base.root, Some(dir.path().join("code/user")));
        assert_eq!(base.rpc_url, Some("unix:///run/base.sock".to_string()));
        assert_eq!(base.clone_start_ttl_minutes, Some(10));
        assert_eq!(
            base.client_id,
            Some("00000000-0000-4000-8000-000000000005".to_string())
        );
        assert_eq!(base.assume_origin_as_canonical, Some(true));
        assert_eq!(base.detect_related, Some(true));
        assert_eq!(base.rpc_rate_limit_per_second, Some(9));
    }

    #[test]
    fn rate_limiter_defaults_to_one_request_per_second_per_client() {
        let mut limiter = RateLimiter::new(1);

        assert!(limiter.allow("client-a"));
        assert!(!limiter.allow("client-a"));
        assert!(limiter.allow("client-b"));
    }

    #[test]
    fn rate_limiter_can_be_disabled() {
        let mut limiter = RateLimiter::new(0);

        assert!(limiter.allow("client-a"));
        assert!(limiter.allow("client-a"));
    }

    #[test]
    fn rpc_endpoints_are_unix_only() {
        assert_eq!(
            parse_rpc_endpoint("unix:///tmp/repo-manager.sock").unwrap(),
            PathBuf::from("/tmp/repo-manager.sock")
        );
        assert!(parse_rpc_endpoint("tcp://127.0.0.1:47321").is_err());
        assert!(parse_rpc_endpoint("udp://127.0.0.1:47321").is_err());
    }

    #[test]
    fn daemon_cancellation_removes_matching_clone_start() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon_config = test_daemon_config(dir.path());
        let daemon_state = DaemonState::new(0, 60);
        let locator = Locator::parse("example.com/current").unwrap();
        let path = dir.path().join("code/clones/example.com/current");

        handle_rpc_event(
            &daemon_config,
            &daemon_state,
            RpcEvent::Started(CloneStartedEvent {
                client_id: config.client_id.clone(),
                url: "https://example.com/current.git".to_string(),
                locator: locator.clone(),
                path: path.clone(),
                scan_root: config.clone_root.clone(),
            }),
        )
        .unwrap();
        assert_eq!(daemon_state.clone_starts.lock().unwrap().len(), 1);

        handle_rpc_event(
            &daemon_config,
            &daemon_state,
            RpcEvent::Cancelled(CloneCancelledEvent {
                client_id: config.client_id.clone(),
                url: "https://example.com/current.git".to_string(),
                locator,
                path,
                reason: "test cancellation".to_string(),
                scan_root: config.clone_root.clone(),
            }),
        )
        .unwrap();
        assert!(daemon_state.clone_starts.lock().unwrap().is_empty());
    }

    #[test]
    fn daemon_ttl_prunes_stale_clone_starts() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let daemon_config = test_daemon_config(dir.path());
        let daemon_state = DaemonState::new(0, 0);
        let locator = Locator::parse("example.com/current").unwrap();
        let path = dir.path().join("code/clones/example.com/current");

        handle_rpc_event(
            &daemon_config,
            &daemon_state,
            RpcEvent::Started(CloneStartedEvent {
                client_id: config.client_id.clone(),
                url: "https://example.com/current.git".to_string(),
                locator,
                path,
                scan_root: config.clone_root.clone(),
            }),
        )
        .unwrap();

        let pruned = prune_expired_clone_starts(&daemon_state).unwrap();
        assert_eq!(pruned, 1);
        assert!(daemon_state.clone_starts.lock().unwrap().is_empty());
    }

    #[test]
    fn daemon_reviews_client_scan_root_after_matching_clone_start_and_finish() {
        let dir = tempfile::tempdir().unwrap();
        let code_root = dir.path().join("code");
        let seed = dir.path().join("seed");
        let current_path = code_root.join("clones/example.com/current");
        let other_path = code_root.join("repos/example.com/other");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &current_path);
        clone_local_repo(&seed, &other_path);
        run_git_in(
            &current_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/current.git",
            ],
        )
        .unwrap();
        run_git_in(
            &other_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/other.git",
            ],
        )
        .unwrap();

        let state_path = dir.path().join("repos.sqlite");
        let client_id = "00000000-0000-4000-8000-000000000006".to_string();
        let daemon_config = DaemonConfig {
            state: state_path.clone(),
            detect_related: true,
            clone_start_ttl_minutes: 60,
            rpc_rate_limit_per_second: 0,
            background_fetch_minimum_interval_seconds: None,
        };
        let daemon_state = DaemonState::new(0, 60);
        let locator = Locator::parse("example.com/current").unwrap();
        let start = CloneStartedEvent {
            client_id: client_id.clone(),
            url: "https://example.com/current.git".to_string(),
            locator: locator.clone(),
            path: current_path.clone(),
            scan_root: code_root.clone(),
        };
        handle_rpc_event(&daemon_config, &daemon_state, RpcEvent::Started(start)).unwrap();
        handle_rpc_event(
            &daemon_config,
            &daemon_state,
            RpcEvent::Finished(CloneFinishedEvent {
                client_id: client_id.clone(),
                url: "https://example.com/current.git".to_string(),
                locator,
                path: current_path,
                success: true,
                scan_root: code_root,
            }),
        )
        .unwrap();

        let store = Store::open(&state_path).unwrap();
        let suggestions = store.related_suggestions(true).unwrap();
        assert_eq!(suggestions.len(), 1);
        assert!(
            suggestions[0].repo_locator.key() == "example.com/current"
                || suggestions[0].related_locator.key() == "example.com/current"
        );
        assert!(
            suggestions[0].repo_locator.key() == "example.com/other"
                || suggestions[0].related_locator.key() == "example.com/other"
        );
        assert!(
            suggestions[0]
                .shared_refs
                .iter()
                .any(|evidence| evidence.starts_with("shared root commit "))
        );
    }

    #[test]
    fn daemon_reviews_manage_request_without_clone_start() {
        let dir = tempfile::tempdir().unwrap();
        let clone_root = dir.path().join("clones");
        let seed = dir.path().join("seed");
        let current_path = clone_root.join("example.com/current");
        let other_path = clone_root.join("example.com/other");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &current_path);
        clone_local_repo(&seed, &other_path);
        run_git_in(
            &current_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/current.git",
            ],
        )
        .unwrap();
        run_git_in(
            &other_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/other.git",
            ],
        )
        .unwrap();

        let state_path = dir.path().join("repos.sqlite");
        let daemon_config = DaemonConfig {
            state: state_path.clone(),
            detect_related: true,
            clone_start_ttl_minutes: 60,
            rpc_rate_limit_per_second: 0,
            background_fetch_minimum_interval_seconds: None,
        };
        let daemon_state = DaemonState::new(0, 60);
        handle_rpc_event(
            &daemon_config,
            &daemon_state,
            RpcEvent::ManageRequested(ManageRequestedEvent {
                client_id: "00000000-0000-4000-8000-000000000088".to_string(),
                url: "https://example.com/current.git".to_string(),
                locator: Locator::parse("example.com/current").unwrap(),
                path: current_path,
                scan_root: clone_root,
            }),
        )
        .unwrap();

        let store = Store::open(&state_path).unwrap();
        let suggestions = store.related_suggestions(true).unwrap();
        assert_eq!(suggestions.len(), 1);
    }

    #[test]
    fn background_fetch_records_changes_and_respects_learned_interval() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_daemon_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let locator = Locator::parse("example.com/background/repo").unwrap();
        let clone_path = dir.path().join("clones/example.com/background/repo");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "initial\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        fs::create_dir_all(clone_path.parent().unwrap()).unwrap();
        run_git_clone(&file_url_for_path(&seed), &clone_path, true).unwrap();
        let repo_id = store.upsert_repo(&locator, &clone_path, None).unwrap();

        fs::write(seed.join("README.md"), "updated\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "update",
            ],
        )
        .unwrap();

        assert_eq!(background_fetch_once(&config, 60).unwrap(), 1);
        let (status, interval): (String, i64) = store
            .conn
            .query_row(
                "SELECT last_status, learned_interval_seconds FROM background_fetch WHERE repo_id = ?1",
                params![repo_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "changed");
        assert_eq!(interval, 60);
        assert_eq!(background_fetch_once(&config, 60).unwrap(), 0);
    }

    #[test]
    fn check_dump_reports_tracked_untracked_bare_and_fetch_state() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "dump\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();

        let tracked_locator = Locator::parse("example.com/tracked/repo").unwrap();
        let tracked_path = locator_path(&config.clone_root, &tracked_locator);
        fs::create_dir_all(tracked_path.parent().unwrap()).unwrap();
        run_git_clone(&file_url_for_path(&seed), &tracked_path, true).unwrap();
        let tracked_id = store
            .upsert_repo(&tracked_locator, &tracked_path, None)
            .unwrap();
        store
            .record_background_fetch(tracked_id, 1234, 60, true, None)
            .unwrap();

        let untracked_path = config.clone_root.join("example.com/untracked/repo");
        clone_local_repo(&seed, &untracked_path);
        run_git_in(
            &untracked_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/untracked/repo.git",
            ],
        )
        .unwrap();

        let dump = check_dump_report(&config, &store).unwrap();

        let tracked = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.id == "clones/example.com/tracked/repo")
            .unwrap();
        assert_eq!(tracked.locator, tracked_locator);
        assert_eq!(tracked.repo_type, "canonical");
        assert_eq!(tracked.checkout_kind, "bare");
        assert_eq!(
            tracked
                .background_fetch
                .as_ref()
                .unwrap()
                .learned_interval_seconds,
            60
        );

        assert!(dump.git_directories.iter().any(|entry| {
            entry.path == tracked_path
                && entry.tracked
                && entry.managed
                && entry.repository.as_deref() == Some("clones/example.com/tracked/repo")
                && entry.repository_type.as_deref() == Some("canonical")
                && entry.kind.as_deref() == Some("bare")
        }));
        assert!(dump.git_directories.iter().any(|entry| {
            entry.path == untracked_path
                && !entry.tracked
                && !entry.managed
                && entry.repository.is_none()
                && entry.repository_type.is_none()
                && entry.kind.as_deref() == Some("non-bare")
                && entry.locator == Some(Locator::parse("example.com/untracked/repo").unwrap())
        }));
    }

    #[test]
    fn repo_worktree_add_records_tracked_worktree() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "worktree\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        let locator = Locator::parse("example.com/worktree/repo").unwrap();
        let repo_path = locator_path(&config.clone_root, &locator);
        clone_local_repo(&seed, &repo_path);
        run_git_in(
            &repo_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/worktree/repo.git",
            ],
        )
        .unwrap();
        store.upsert_repo(&locator, &repo_path, None).unwrap();

        add_worktree(
            &config,
            &store,
            &Output { json: true },
            Some(&repo_path),
            WorktreeAddArgs {
                repo_or_name: "managed-topic".to_string(),
                name_or_start_point: None,
                start_point: None,
                branch: Some("managed-topic".to_string()),
                detach: false,
                force: false,
                reset: false,
            },
        )
        .unwrap();

        let worktree_path = locator_path(&config.dev_worktree_root, &locator).join("managed-topic");
        let dump = check_dump_report(&config, &store).unwrap();
        let worktree = dump
            .git_directories
            .iter()
            .find(|entry| entry.path == worktree_path)
            .unwrap();
        assert_eq!(
            worktree.id.as_deref(),
            Some("dev-worktrees/example.com/worktree/repo/managed-topic")
        );
        assert_eq!(worktree.kind.as_deref(), Some("worktree"));
        assert!(worktree.tracked);
        assert!(worktree.managed);
        assert_eq!(worktree.worktree_name.as_deref(), Some("managed-topic"));
        assert_eq!(
            worktree.repository.as_deref(),
            Some("clones/example.com/worktree/repo")
        );
        assert_eq!(worktree.repository_type.as_deref(), Some("canonical"));
    }

    #[test]
    fn check_dump_reports_raw_git_worktrees_as_untracked_even_outside_roots() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "raw worktree\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        let locator = Locator::parse("example.com/raw/repo").unwrap();
        let repo_path = locator_path(&config.clone_root, &locator);
        clone_local_repo(&seed, &repo_path);
        run_git_in(
            &repo_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/raw/repo.git",
            ],
        )
        .unwrap();
        store.upsert_repo(&locator, &repo_path, None).unwrap();
        let raw_worktree_path = dir.path().join("outside-raw-worktree");
        run_git_in(
            &repo_path,
            [
                "worktree",
                "add",
                "--detach",
                raw_worktree_path.to_str().unwrap(),
                "HEAD",
            ],
        )
        .unwrap();

        let dump = check_dump_report(&config, &store).unwrap();
        let raw_worktree = dump
            .git_directories
            .iter()
            .find(|entry| entry.path == raw_worktree_path)
            .unwrap();
        assert_eq!(raw_worktree.id, None);
        assert_eq!(raw_worktree.kind.as_deref(), Some("worktree"));
        assert!(!raw_worktree.tracked);
        assert!(!raw_worktree.managed);
        assert_eq!(
            raw_worktree.repository.as_deref(),
            Some("clones/example.com/raw/repo")
        );
        assert_eq!(raw_worktree.repository_type.as_deref(), Some("canonical"));
    }

    #[test]
    fn related_report_prefers_shared_root_evidence_for_legacy_rows() {
        let dir = tempfile::tempdir().unwrap();
        let seed = dir.path().join("seed");
        let first_path = dir.path().join("clones/example.com/first");
        let second_path = dir.path().join("clones/example.com/second");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &first_path);
        clone_local_repo(&seed, &second_path);

        let first_locator = Locator::parse("example.com/first").unwrap();
        let second_locator = Locator::parse("example.com/second").unwrap();
        let legacy = RelatedSuggestion {
            id: 7,
            repo_id: 1,
            repo_locator: first_locator.clone(),
            repo_path: first_path,
            related_repo_id: 2,
            related_locator: second_locator,
            related_path: second_path,
            shared_refs: vec!["shared commit aaaaaaaaaaaa".to_string()],
            resolution: None,
        };

        let report = related_list_report(&[legacy]);

        assert!(
            report.suggestions[0]
                .evidence
                .summary
                .starts_with("shared root commit ")
        );
        assert!(
            report.suggestions[0]
                .evidence
                .details
                .iter()
                .all(|evidence| evidence.starts_with("shared root commit "))
        );
    }

    #[test]
    fn related_report_does_not_use_legacy_non_root_evidence() {
        let legacy = RelatedSuggestion {
            id: 7,
            repo_id: 1,
            repo_locator: Locator::parse("example.com/first").unwrap(),
            repo_path: PathBuf::from("/missing/first"),
            related_repo_id: 2,
            related_locator: Locator::parse("example.com/second").unwrap(),
            related_path: PathBuf::from("/missing/second"),
            shared_refs: vec!["shared commit aaaaaaaaaaaa".to_string()],
            resolution: None,
        };

        let report = related_list_report(&[legacy]);

        assert_eq!(report.suggestions[0].evidence.summary, "unknown");
        assert!(report.suggestions[0].evidence.details.is_empty());
    }

    #[test]
    fn rpc_clone_event_round_trips_through_protobuf() {
        let events = [
            RpcEvent::Finished(CloneFinishedEvent {
                client_id: "00000000-0000-4000-8000-000000000007".to_string(),
                url: "https://example.com/current.git".to_string(),
                locator: Locator::parse("example.com/current").unwrap(),
                path: PathBuf::from("/tmp/client/clones/example.com/current"),
                success: true,
                scan_root: PathBuf::from("/tmp/client/clones"),
            }),
            RpcEvent::ManageRequested(ManageRequestedEvent {
                client_id: "00000000-0000-4000-8000-000000000008".to_string(),
                url: "https://example.com/managed.git".to_string(),
                locator: Locator::parse("example.com/managed").unwrap(),
                path: PathBuf::from("/tmp/client/clones/example.com/managed"),
                scan_root: PathBuf::from("/tmp/client/clones"),
            }),
        ];

        for event in events {
            let mut message = Vec::new();
            event
                .to_proto()
                .encode_length_delimited(&mut message)
                .unwrap();
            assert_eq!(event.to_proto().protocol_version, RPC_PROTOCOL_VERSION);

            let decoded = decode_rpc_event(&message).unwrap();

            match decoded {
                RpcEvent::Finished(decoded) => {
                    assert_eq!(decoded.client_id, "00000000-0000-4000-8000-000000000007");
                    assert_eq!(
                        decoded.locator,
                        Locator::parse("example.com/current").unwrap()
                    );
                    assert_eq!(
                        decoded.path,
                        PathBuf::from("/tmp/client/clones/example.com/current")
                    );
                    assert!(decoded.success);
                    assert_eq!(decoded.scan_root, PathBuf::from("/tmp/client/clones"));
                }
                RpcEvent::ManageRequested(decoded) => {
                    assert_eq!(decoded.client_id, "00000000-0000-4000-8000-000000000008");
                    assert_eq!(
                        decoded.locator,
                        Locator::parse("example.com/managed").unwrap()
                    );
                    assert_eq!(
                        decoded.path,
                        PathBuf::from("/tmp/client/clones/example.com/managed")
                    );
                    assert_eq!(decoded.scan_root, PathBuf::from("/tmp/client/clones"));
                }
                other => panic!("unexpected decoded event: {other:?}"),
            }
        }
    }

    #[test]
    fn rpc_clone_event_rejects_protocol_version_mismatch() {
        let event = RpcEvent::Finished(CloneFinishedEvent {
            client_id: "00000000-0000-4000-8000-000000000007".to_string(),
            url: "https://example.com/current.git".to_string(),
            locator: Locator::parse("example.com/current").unwrap(),
            path: PathBuf::from("/tmp/client/clones/example.com/current"),
            success: true,
            scan_root: PathBuf::from("/tmp/client/clones"),
        });
        for unsupported_version in [0, RPC_PROTOCOL_VERSION + 1] {
            let mut proto = event.to_proto();
            proto.protocol_version = unsupported_version;
            let mut message = Vec::new();
            proto.encode_length_delimited(&mut message).unwrap();

            let error = decode_rpc_event(&message).unwrap_err();

            assert!(error.to_string().contains("RPC protocol version mismatch"));
        }
    }

    fn clone_local_repo(seed: &Path, destination: &Path) {
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        assert!(
            Command::new("git")
                .arg("clone")
                .arg(seed)
                .arg(destination)
                .status()
                .unwrap()
                .success()
        );
    }

    fn file_url_for_path(path: &Path) -> String {
        format!("file://localhost{}", path.display())
    }

    fn test_rpc_url(root: &Path) -> String {
        format!("unix://{}", root.join("repo-manager-test.sock").display())
    }

    fn test_config(root: &Path) -> Config {
        Config {
            config_path: root.join("config.json"),
            state: root.join("repos.sqlite"),
            cache_root: root.join("cache"),
            root: root.join("code"),
            clone_root: root.join("code/clones"),
            dev_worktree_root: root.join("code/dev-worktrees"),
            rpc_url: test_rpc_url(root),
            client_id: "00000000-0000-4000-8000-000000000099".to_string(),
            assume_origin_as_canonical: false,
            clone_as_bare: false,
            create_default_visibility: RepoVisibility::Private,
            forges: HashMap::new(),
        }
    }

    fn test_daemon_config(root: &Path) -> DaemonConfig {
        DaemonConfig {
            state: root.join("repos.sqlite"),
            detect_related: true,
            clone_start_ttl_minutes: 60,
            rpc_rate_limit_per_second: 0,
            background_fetch_minimum_interval_seconds: None,
        }
    }

    #[test]
    fn test_config_uses_isolated_state_and_rpc_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        assert_eq!(config.state, dir.path().join("repos.sqlite"));
        assert_eq!(config.rpc_url, test_rpc_url(dir.path()));
        assert_ne!(config.rpc_url, default_rpc_url());
    }

    #[test]
    fn related_history_suggestions_are_persisted_until_resolved() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("repos.sqlite")).unwrap();
        let first_locator = Locator::parse("github.com/example/first").unwrap();
        let second_locator = Locator::parse("github.com/example/second").unwrap();
        let first_path = dir.path().join("clones/github.com/example/first");
        let second_path = dir.path().join("clones/github.com/example/second");
        let first_id = store
            .upsert_repo(&first_locator, &first_path, None)
            .unwrap();
        let second_id = store
            .upsert_repo(&second_locator, &second_path, None)
            .unwrap();

        store
            .record_related_history(first_id, second_id, &["abcdef123456 main".to_string()])
            .unwrap();

        let suggestions = store.related_suggestions(true).unwrap();
        assert_eq!(suggestions.len(), 1);
        assert_eq!(store.pending_related_count().unwrap(), 1);

        store.resolve_related(suggestions[0].id, "mirror").unwrap();

        assert_eq!(store.pending_related_count().unwrap(), 0);
        assert!(store.related_suggestions(true).unwrap().is_empty());
    }

    #[test]
    fn resolving_related_fork_converts_first_repo_to_worktree_of_second() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let fork_locator = Locator::parse("github.com/johnrichardrinehart/niri").unwrap();
        let canonical_locator = Locator::parse("github.com/yalter/niri").unwrap();
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &fork_path);
        clone_local_repo(&seed, &canonical_path);
        run_git_in(
            &fork_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://github.com/johnrichardrinehart/niri.git",
            ],
        )
        .unwrap();
        run_git_in(
            &canonical_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://github.com/yalter/niri.git",
            ],
        )
        .unwrap();
        let fork_head = git_output(&fork_path, ["rev-parse", "HEAD"], "reading fork HEAD")
            .unwrap()
            .trim()
            .to_string();
        let fork_id = store.upsert_repo(&fork_locator, &fork_path, None).unwrap();
        let canonical_id = store
            .upsert_repo(&canonical_locator, &canonical_path, None)
            .unwrap();
        store
            .record_related_history(
                fork_id,
                canonical_id,
                &["shared root commit abc".to_string()],
            )
            .unwrap();
        let suggestion = store.related_suggestions(true).unwrap().remove(0);
        assert_eq!(suggestion.repo_locator, fork_locator);
        assert_eq!(suggestion.related_locator, canonical_locator);

        related_resolve(
            &config,
            &store,
            &Output { json: true },
            suggestion.id,
            "fork",
        )
        .unwrap();

        assert_eq!(store.pending_related_count().unwrap(), 0);
        assert_eq!(
            git_output(&fork_path, ["rev-parse", "HEAD"], "reading fork HEAD")
                .unwrap()
                .trim(),
            fork_head
        );
        assert_eq!(
            git_common_dir(&fork_path).unwrap(),
            git_common_dir(&canonical_path).unwrap()
        );
        assert_eq!(
            git_output(
                &fork_path,
                ["branch", "--show-current"],
                "reading fork branch"
            )
            .unwrap()
            .trim(),
            "repo-manager/forks/github.com-johnrichardrinehart-niri/main"
        );
        assert_eq!(
            git_output(
                &fork_path,
                [
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}"
                ],
                "reading fork upstream"
            )
            .unwrap()
            .trim(),
            "fork-github.com-johnrichardrinehart-niri/main"
        );
        assert_eq!(
            git_remote_url(&canonical_path, "fork-github.com-johnrichardrinehart-niri").unwrap(),
            Some("https://github.com/johnrichardrinehart/niri.git".to_string())
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM forks", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn repair_check_reports_and_repair_materializes_resolved_fork() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let fork_locator = Locator::parse("github.com/example/fork").unwrap();
        let canonical_locator = Locator::parse("github.com/example/canonical").unwrap();
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &fork_path);
        clone_local_repo(&seed, &canonical_path);
        run_git_in(
            &fork_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://github.com/example/fork.git",
            ],
        )
        .unwrap();
        run_git_in(
            &canonical_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://github.com/example/canonical.git",
            ],
        )
        .unwrap();
        let fork_id = store.upsert_repo(&fork_locator, &fork_path, None).unwrap();
        let canonical_id = store
            .upsert_repo(&canonical_locator, &canonical_path, None)
            .unwrap();
        store
            .record_related_history(
                fork_id,
                canonical_id,
                &["shared root commit abc".to_string()],
            )
            .unwrap();
        let suggestion_id = store.related_suggestions(true).unwrap()[0].id;
        store.resolve_related(suggestion_id, "fork").unwrap();
        assert_ne!(
            git_common_dir(&fork_path).unwrap(),
            git_common_dir(&canonical_path).unwrap()
        );
        let relationship = store.shared_git_dir_relationships().unwrap().remove(0);
        let reasons = shared_git_dir_relationship_repair_reasons(&relationship, false).unwrap();
        assert!(reasons.iter().any(|reason| {
            reason.starts_with("fork does not use canonical Git directory; fork uses ")
        }));

        assert!(repair_repos(&config, &store, &Output { json: true }, true).is_err());
        assert_ne!(
            git_common_dir(&fork_path).unwrap(),
            git_common_dir(&canonical_path).unwrap()
        );

        repair_repos(&config, &store, &Output { json: true }, false).unwrap();
        assert_eq!(
            git_common_dir(&fork_path).unwrap(),
            git_common_dir(&canonical_path).unwrap()
        );
        assert_eq!(
            git_output(
                &fork_path,
                [
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}"
                ],
                "reading fork upstream"
            )
            .unwrap()
            .trim(),
            "fork-github.com-example-fork/main"
        );
    }

    #[test]
    fn repair_converts_existing_related_worktree_to_bare_when_configured() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let fork_remote = dir.path().join("fork-remote");
        let canonical_remote = dir.path().join("canonical-remote");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &fork_remote);
        clone_local_repo(&seed, &canonical_remote);
        let fork_url = file_url_for_path(&fork_remote);
        let canonical_url = file_url_for_path(&canonical_remote);
        let fork_locator = Locator::parse(&fork_url).unwrap();
        let canonical_locator = Locator::parse(&canonical_url).unwrap();
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        clone_local_repo(&fork_remote, &fork_path);
        clone_local_repo(&canonical_remote, &canonical_path);
        run_git_in(
            &fork_path,
            ["remote", "set-url", "origin", fork_url.as_str()],
        )
        .unwrap();
        run_git_in(
            &canonical_path,
            ["remote", "set-url", "origin", canonical_url.as_str()],
        )
        .unwrap();
        let fork_id = store.upsert_repo(&fork_locator, &fork_path, None).unwrap();
        let canonical_id = store
            .upsert_repo(&canonical_locator, &canonical_path, None)
            .unwrap();
        store
            .record_related_history(
                fork_id,
                canonical_id,
                &["shared root commit abc".to_string()],
            )
            .unwrap();
        let suggestion_id = store.related_suggestions(true).unwrap()[0].id;
        store.resolve_related(suggestion_id, "fork").unwrap();

        repair_repos(&config, &store, &Output { json: true }, false).unwrap();
        assert!(!is_bare_repository(&canonical_path).unwrap());
        assert!(!is_bare_repository(&fork_path).unwrap());
        assert_eq!(
            git_common_dir(&fork_path).unwrap(),
            git_common_dir(&canonical_path).unwrap()
        );

        config.clone_as_bare = true;
        assert!(repair_repos(&config, &store, &Output { json: true }, true).is_err());
        repair_repos(&config, &store, &Output { json: true }, false).unwrap();

        assert!(is_bare_repository(&canonical_path).unwrap());
        let view = read_repo_view_metadata(&fork_path).unwrap().unwrap();
        assert_eq!(view.locator, fork_locator);
        assert_eq!(view.canonical_locator, canonical_locator);
        assert_eq!(
            view.origin_url,
            remote_url_for_locator(Some(&canonical_url), &view.locator)
        );
        assert_eq!(
            comparable_path(&view.canonical_path),
            comparable_path(&canonical_path)
        );
        assert!(
            git_dir_ref_exists(
                &canonical_path,
                &format!("{}/remotes/origin/main", view.refs_prefix)
            )
            .unwrap()
        );
        assert!(!fork_path.join("objects").exists());
        assert!(!fork_path.join(".git").exists());
    }

    #[test]
    fn repair_prunes_stale_repo_rows_without_existing_dependents() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let stale_locator = Locator::parse("github.com/example/stale").unwrap();
        let stale_path = locator_path(&config.clone_root, &stale_locator);
        let repo_id = store
            .upsert_repo(&stale_locator, &stale_path, None)
            .unwrap();

        assert!(repair_repos(&config, &store, &Output { json: true }, true).is_err());
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM repos WHERE id = ?1",
                    params![repo_id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );

        repair_repos(&config, &store, &Output { json: true }, false).unwrap();
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM repos WHERE id = ?1",
                    params![repo_id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM locators WHERE repo_id = ?1",
                    params![repo_id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn repair_tracks_unmanaged_clone_root_checkout_with_parseable_origin() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let locator = Locator::parse("example.com/manual/repo").unwrap();
        let checkout_path = locator_path(&config.clone_root, &locator);
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "manual checkout\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &checkout_path);
        run_git_in(
            &checkout_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/manual/repo.git",
            ],
        )
        .unwrap();

        assert!(repair_repos(&config, &store, &Output { json: true }, true).is_err());
        assert!(
            store
                .find_repo("example.com/manual/repo")
                .unwrap()
                .is_none()
        );

        repair_repos(&config, &store, &Output { json: true }, false).unwrap();
        let record = store.find_repo("example.com/manual/repo").unwrap().unwrap();
        assert_eq!(record.current, locator);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT current_path FROM repos WHERE id = ?1",
                    params![record.id],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            checkout_path.display().to_string()
        );
    }

    #[test]
    fn repair_converts_tracked_clone_root_checkout_to_bare_when_configured() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.clone_as_bare = true;
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let locator = Locator::parse("example.com/plain/repo").unwrap();
        let checkout_path = locator_path(&config.clone_root, &locator);
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "plain checkout\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &checkout_path);
        run_git_in(
            &checkout_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/plain/repo.git",
            ],
        )
        .unwrap();
        store.upsert_repo(&locator, &checkout_path, None).unwrap();

        assert!(!is_bare_repository(&checkout_path).unwrap());
        assert!(repair_repos(&config, &store, &Output { json: true }, true).is_err());

        repair_repos(&config, &store, &Output { json: true }, false).unwrap();

        assert!(is_bare_repository(&checkout_path).unwrap());
        assert!(!checkout_path.join("README.md").exists());
        assert_eq!(
            git_remote_url(&checkout_path, "origin").unwrap(),
            Some("https://example.com/plain/repo.git".to_string())
        );
    }

    #[test]
    fn repair_updates_metadata_for_out_of_band_moved_checkout() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let locator = Locator::parse("example.com/moved/repo").unwrap();
        let old_path = locator_path(&config.clone_root, &locator);
        let moved_path = config.clone_root.join("example.com/moved-out-of-band/repo");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "moved checkout\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &old_path);
        run_git_in(
            &old_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/moved/repo.git",
            ],
        )
        .unwrap();
        let repo_id = store.upsert_repo(&locator, &old_path, None).unwrap();
        fs::create_dir_all(moved_path.parent().unwrap()).unwrap();
        fs::rename(&old_path, &moved_path).unwrap();

        repair_repos(&config, &store, &Output { json: true }, false).unwrap();

        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT current_path FROM repos WHERE id = ?1",
                    params![repo_id],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
            moved_path.display().to_string()
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM repos", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn repair_blocks_pruning_stale_controlling_repo_with_existing_dependent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let fork_locator = Locator::parse("github.com/example/fork").unwrap();
        let canonical_locator = Locator::parse("github.com/example/canonical").unwrap();
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        fs::create_dir_all(&fork_path).unwrap();
        let fork_id = store.upsert_repo(&fork_locator, &fork_path, None).unwrap();
        let canonical_id = store
            .upsert_repo(&canonical_locator, &canonical_path, None)
            .unwrap();
        store.record_fork(fork_id, canonical_id).unwrap();

        repair_repos(&config, &store, &Output { json: true }, false).unwrap();

        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM repos WHERE id = ?1",
                    params![canonical_id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM forks", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn check_dump_reports_root_relative_ids_and_relationship_types() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let canonical_locator = Locator::parse("example.com/project/canonical").unwrap();
        let fork_locator = Locator::parse("example.com/project/fork").unwrap();
        let mirror_locator = Locator::parse("example.com/project/mirror").unwrap();
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let mirror_path = locator_path(&config.clone_root, &mirror_locator);
        let canonical_id = store
            .upsert_repo(&canonical_locator, &canonical_path, None)
            .unwrap();
        let fork_id = store.upsert_repo(&fork_locator, &fork_path, None).unwrap();
        let mirror_id = store
            .upsert_repo(&mirror_locator, &mirror_path, None)
            .unwrap();
        store.record_fork(fork_id, canonical_id).unwrap();
        store
            .record_resolved_related(mirror_id, canonical_id, "mirror")
            .unwrap();

        let dump = check_dump_report(&config, &store).unwrap();
        let canonical_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == canonical_locator)
            .unwrap();
        let fork_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == fork_locator)
            .unwrap();
        let mirror_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == mirror_locator)
            .unwrap();

        assert_eq!(canonical_entry.id, "clones/example.com/project/canonical");
        assert_eq!(canonical_entry.repo_type, "canonical");
        assert_eq!(canonical_entry.fork_depth, 0);
        assert_eq!(fork_entry.id, "clones/example.com/project/fork");
        assert_eq!(fork_entry.repo_type, "fork");
        assert_eq!(fork_entry.fork_depth, 1);
        assert_eq!(fork_entry.parent.as_ref().unwrap().id, canonical_entry.id);
        assert_eq!(
            fork_entry.canonical.as_ref().unwrap().id,
            canonical_entry.id
        );
        assert_eq!(mirror_entry.id, "clones/example.com/project/mirror");
        assert_eq!(mirror_entry.repo_type, "mirror");
        assert_eq!(mirror_entry.fork_depth, 1);
        assert_eq!(mirror_entry.parent.as_ref().unwrap().id, canonical_entry.id);
        assert_eq!(
            mirror_entry.canonical.as_ref().unwrap().id,
            canonical_entry.id
        );
        assert_eq!(canonical_entry.dependents.len(), 2);
        assert!(
            canonical_entry
                .dependents
                .iter()
                .any(|dependent| dependent.id == fork_entry.id && dependent.relationship == "fork")
        );
        assert!(
            canonical_entry
                .dependents
                .iter()
                .any(|dependent| dependent.id == mirror_entry.id
                    && dependent.relationship == "mirror")
        );
    }

    #[test]
    fn check_dump_resolves_transitive_fork_parent_and_canonical() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let upstream_locator = Locator::parse("example.com/project/upstream").unwrap();
        let fork_locator = Locator::parse("example.com/project/fork").unwrap();
        let fork_of_fork_locator = Locator::parse("example.com/project/fork-of-fork").unwrap();
        let upstream_path = locator_path(&config.clone_root, &upstream_locator);
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let fork_of_fork_path = locator_path(&config.clone_root, &fork_of_fork_locator);
        let upstream_id = store
            .upsert_repo(&upstream_locator, &upstream_path, None)
            .unwrap();
        let fork_id = store
            .upsert_repo(&fork_locator, &fork_path, Some(&upstream_locator.key()))
            .unwrap();
        let fork_of_fork_id = store
            .upsert_repo(
                &fork_of_fork_locator,
                &fork_of_fork_path,
                Some(&upstream_locator.key()),
            )
            .unwrap();
        store.record_fork(fork_id, upstream_id).unwrap();
        store.record_fork(fork_of_fork_id, fork_id).unwrap();

        let dump = check_dump_report(&config, &store).unwrap();
        let upstream_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == upstream_locator)
            .unwrap();
        let fork_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == fork_locator)
            .unwrap();
        let fork_of_fork_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == fork_of_fork_locator)
            .unwrap();

        assert_eq!(upstream_entry.repo_type, "canonical");
        assert_eq!(upstream_entry.fork_depth, 0);
        assert_eq!(upstream_entry.dependents.len(), 1);
        assert_eq!(upstream_entry.dependents[0].id, fork_entry.id);

        assert_eq!(fork_entry.repo_type, "fork");
        assert_eq!(fork_entry.fork_depth, 1);
        assert_eq!(fork_entry.parent.as_ref().unwrap().id, upstream_entry.id);
        assert_eq!(fork_entry.canonical.as_ref().unwrap().id, upstream_entry.id);
        assert_eq!(fork_entry.dependents.len(), 1);
        assert_eq!(fork_entry.dependents[0].id, fork_of_fork_entry.id);

        assert_eq!(fork_of_fork_entry.repo_type, "fork");
        assert_eq!(fork_of_fork_entry.fork_depth, 2);
        assert_eq!(
            fork_of_fork_entry.parent.as_ref().unwrap().id,
            fork_entry.id
        );
        assert_eq!(
            fork_of_fork_entry.canonical.as_ref().unwrap().id,
            upstream_entry.id
        );
        assert!(fork_of_fork_entry.dependents.is_empty());
    }

    #[test]
    fn repos_set_type_materializes_mirror_namespace_from_path_id() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let mirror_remote = dir.path().join("mirror-remote");
        let canonical_remote = dir.path().join("canonical-remote");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &mirror_remote);
        clone_local_repo(&seed, &canonical_remote);
        let mirror_url = file_url_for_path(&mirror_remote);
        let canonical_url = file_url_for_path(&canonical_remote);
        let mirror_locator = Locator::parse(&mirror_url).unwrap();
        let canonical_locator = Locator::parse(&canonical_url).unwrap();
        let mirror_path = locator_path(&config.clone_root, &mirror_locator);
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        clone_local_repo(&mirror_remote, &mirror_path);
        clone_local_repo(&canonical_remote, &canonical_path);
        run_git_in(&mirror_path, ["remote", "set-url", "origin", &mirror_url]).unwrap();
        run_git_in(
            &canonical_path,
            ["remote", "set-url", "origin", &canonical_url],
        )
        .unwrap();
        store
            .upsert_repo(&mirror_locator, &mirror_path, None)
            .unwrap();

        repos_set_type(
            &config,
            &store,
            &Output { json: true },
            RepoSetTypeArgs {
                repo_path: PathBuf::from(repository_path_id(&config, &mirror_path)),
                repo_type: "mirror".to_string(),
                canonical: Some(repository_path_id(&config, &canonical_path)),
            },
        )
        .unwrap();

        assert!(is_bare_repository(&canonical_path).unwrap());
        let view = read_repo_view_metadata(&mirror_path).unwrap().unwrap();
        assert_eq!(view.relationship, "mirror");
        assert_eq!(view.locator, mirror_locator);
        assert_eq!(view.canonical_locator, canonical_locator);
        assert_eq!(
            comparable_path(&view.canonical_path),
            comparable_path(&canonical_path)
        );

        let dump = check_dump_report(&config, &store).unwrap();
        let mirror_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == mirror_locator)
            .unwrap();
        assert_eq!(mirror_entry.repo_type, "mirror");
        assert_eq!(
            mirror_entry.canonical.as_ref().unwrap().id,
            repository_path_id(&config, &canonical_path)
        );
        assert!(!mirror_path.join("objects").exists());
        assert!(!mirror_path.join(".git").exists());
    }

    #[test]
    fn repos_set_type_accepts_fork_parent_and_flattens_storage_to_canonical() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let canonical_remote = dir.path().join("canonical-remote");
        let fork_remote = dir.path().join("fork-remote");
        let child_remote = dir.path().join("child-remote");
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &canonical_remote);
        clone_local_repo(&seed, &fork_remote);
        clone_local_repo(&seed, &child_remote);
        let canonical_url = file_url_for_path(&canonical_remote);
        let fork_url = file_url_for_path(&fork_remote);
        let child_url = file_url_for_path(&child_remote);
        let canonical_locator = Locator::parse(&canonical_url).unwrap();
        let fork_locator = Locator::parse(&fork_url).unwrap();
        let child_locator = Locator::parse(&child_url).unwrap();
        let canonical_path = locator_path(&config.clone_root, &canonical_locator);
        let fork_path = locator_path(&config.clone_root, &fork_locator);
        let child_path = locator_path(&config.clone_root, &child_locator);
        clone_local_repo(&canonical_remote, &canonical_path);
        clone_local_repo(&fork_remote, &fork_path);
        clone_local_repo(&child_remote, &child_path);
        run_git_in(
            &canonical_path,
            ["remote", "set-url", "origin", &canonical_url],
        )
        .unwrap();
        run_git_in(&fork_path, ["remote", "set-url", "origin", &fork_url]).unwrap();
        run_git_in(&child_path, ["remote", "set-url", "origin", &child_url]).unwrap();
        store.upsert_repo(&fork_locator, &fork_path, None).unwrap();
        store
            .upsert_repo(&child_locator, &child_path, None)
            .unwrap();

        repos_set_type(
            &config,
            &store,
            &Output { json: true },
            RepoSetTypeArgs {
                repo_path: PathBuf::from(repository_path_id(&config, &fork_path)),
                repo_type: "fork".to_string(),
                canonical: Some(repository_path_id(&config, &canonical_path)),
            },
        )
        .unwrap();
        repos_set_type(
            &config,
            &store,
            &Output { json: true },
            RepoSetTypeArgs {
                repo_path: PathBuf::from(repository_path_id(&config, &child_path)),
                repo_type: "fork".to_string(),
                canonical: Some(repository_path_id(&config, &fork_path)),
            },
        )
        .unwrap();

        let fork_view = read_repo_view_metadata(&fork_path).unwrap().unwrap();
        let child_view = read_repo_view_metadata(&child_path).unwrap().unwrap();
        assert_eq!(fork_view.canonical_locator, canonical_locator);
        assert_eq!(child_view.canonical_locator, canonical_locator);
        assert_eq!(
            comparable_path(&child_view.canonical_path),
            comparable_path(&canonical_path)
        );

        let dump = check_dump_report(&config, &store).unwrap();
        let canonical_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == canonical_locator)
            .unwrap();
        let fork_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == fork_locator)
            .unwrap();
        let child_entry = dump
            .tracked_repositories
            .iter()
            .find(|repo| repo.locator == child_locator)
            .unwrap();
        assert_eq!(fork_entry.parent.as_ref().unwrap().id, canonical_entry.id);
        assert_eq!(
            fork_entry.canonical.as_ref().unwrap().id,
            canonical_entry.id
        );
        assert_eq!(child_entry.parent.as_ref().unwrap().id, fork_entry.id);
        assert_eq!(
            child_entry.canonical.as_ref().unwrap().id,
            canonical_entry.id
        );
        assert_eq!(child_entry.fork_depth, 2);
    }

    #[test]
    fn resolving_related_mirror_reuses_second_repo_git_directory_without_fork_row() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config.state).unwrap();
        let seed = dir.path().join("seed");
        let mirror_locator = Locator::parse("example.com/mirror/project").unwrap();
        let controlling_locator = Locator::parse("example.com/canonical/project").unwrap();
        let mirror_path = locator_path(&config.clone_root, &mirror_locator);
        let controlling_path = locator_path(&config.clone_root, &controlling_locator);
        fs::create_dir_all(&seed).unwrap();
        run_git_in(&seed, ["init"]).unwrap();
        run_git_in(&seed, ["checkout", "-b", "main"]).unwrap();
        fs::write(seed.join("README.md"), "shared history\n").unwrap();
        run_git_in(&seed, ["add", "."]).unwrap();
        run_git_in(
            &seed,
            [
                "-c",
                "user.name=repo-manager",
                "-c",
                "user.email=repo-manager@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();
        clone_local_repo(&seed, &mirror_path);
        clone_local_repo(&seed, &controlling_path);
        run_git_in(
            &mirror_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/mirror/project.git",
            ],
        )
        .unwrap();
        run_git_in(
            &controlling_path,
            [
                "remote",
                "set-url",
                "origin",
                "https://example.com/canonical/project.git",
            ],
        )
        .unwrap();
        let mirror_id = store
            .upsert_repo(&mirror_locator, &mirror_path, None)
            .unwrap();
        let controlling_id = store
            .upsert_repo(&controlling_locator, &controlling_path, None)
            .unwrap();
        store
            .record_related_history(
                mirror_id,
                controlling_id,
                &["shared root commit abc".to_string()],
            )
            .unwrap();
        let suggestion = store.related_suggestions(true).unwrap().remove(0);

        related_resolve(
            &config,
            &store,
            &Output { json: true },
            suggestion.id,
            "mirror",
        )
        .unwrap();

        assert_eq!(store.pending_related_count().unwrap(), 0);
        assert_eq!(
            git_common_dir(&mirror_path).unwrap(),
            git_common_dir(&controlling_path).unwrap()
        );
        assert_eq!(
            git_output(
                &mirror_path,
                ["branch", "--show-current"],
                "reading mirror branch"
            )
            .unwrap()
            .trim(),
            "repo-manager/mirrors/example.com-mirror-project/main"
        );
        assert_eq!(
            git_output(
                &mirror_path,
                [
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}"
                ],
                "reading mirror upstream"
            )
            .unwrap()
            .trim(),
            "mirror-example.com-mirror-project/main"
        );
        assert_eq!(
            git_remote_url(&controlling_path, "mirror-example.com-mirror-project").unwrap(),
            Some("https://example.com/mirror/project.git".to_string())
        );
        assert_eq!(
            store
                .conn
                .query_row("SELECT COUNT(*) FROM forks", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn github_api_responses_are_cached_under_cache_root() {
        let dir = tempfile::tempdir().unwrap();
        let cache_root = dir.path().join("cache");
        let locator = Locator::parse("github.com/old-owner/old-name").unwrap();
        write_cached_github_response(
            &cache_root,
            &locator,
            r#"{"full_name":"new-owner/new-name"}"#,
        )
        .unwrap();

        assert_eq!(
            github_cache_path(&cache_root, &locator),
            cache_root.join("github.com/old-owner/old-name.json")
        );
        assert_eq!(
            read_cached_github_locator(&cache_root, &locator)
                .unwrap()
                .unwrap(),
            Locator::parse("github.com/new-owner/new-name").unwrap()
        );
    }
}
