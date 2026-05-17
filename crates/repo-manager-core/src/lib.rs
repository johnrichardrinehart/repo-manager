use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
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
use std::time::{Duration, Instant};

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
const RPC_PROTOCOL_VERSION: u32 = 1;

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
        about = "Register an existing checkout under the managed clone root",
        long_about = "Register an existing Git checkout under the managed clone root without cloning it.\n\nUse this for a repository that already exists on disk. The command resolves the Git worktree root, chooses a canonical URL from its remotes or an interactive prompt, moves the checkout into its managed locator path when needed, records it in repo-manager metadata, and asks repod to review repositories under the clone root for shared Git history."
    )]
    Manage(ManageArgs),
    #[command(about = "Create or register a fork worktree for a canonical repository")]
    Fork(ForkArgs),
    #[command(about = "Manage development worktrees under the managed dev-worktree root")]
    Worktree(WorktreeCommand),
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
        help = "Canonical upstream Git URL or locator for this fork"
    )]
    canonical: String,
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

#[derive(Debug, Args)]
struct WorktreeAddArgs {
    #[arg(
        value_name = "CANONICAL_URL",
        help = "Canonical repository URL or locator that owns the worktree"
    )]
    canonical_url: String,
    #[arg(
        value_name = "NAME",
        help = "Local worktree name appended under the canonical worktree directory"
    )]
    name: String,
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
        long_about = "Resolve a shared-history suggestion with an explicit relationship.\n\nKinds: mirror, fork, canonical, moved, successor, unrelated.\n\nFor fork and mirror, the first repository shown by `repo related list` is treated as the dependent checkout and the second repository is treated as the checkout that controls the shared Git directory. The dependent checkout is converted into a Git worktree of the controlling checkout when possible."
    )]
    Resolve(RelatedResolveArgs),
}

#[derive(Debug, Args)]
struct RelatedCommand {
    #[command(subcommand)]
    command: RelatedSubcommand,
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
}

#[derive(Debug, Clone)]
struct DaemonConfig {
    state: PathBuf,
    detect_related: bool,
    clone_start_ttl_minutes: u64,
    rpc_rate_limit_per_second: u32,
}

#[derive(Debug, Clone, Copy)]
struct Output {
    json: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct FileConfig {
    state: Option<PathBuf>,
    cache_root: Option<PathBuf>,
    root: Option<PathBuf>,
    rpc_url: Option<String>,
    client_id: Option<String>,
    assume_origin_as_canonical: Option<bool>,
    detect_related: Option<bool>,
    clone_start_ttl_minutes: Option<u64>,
    rpc_rate_limit_per_second: Option<u32>,
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
struct ManageResult {
    action: &'static str,
    locator: Locator,
    canonical_url: String,
    path: PathBuf,
    moved_from: Option<PathBuf>,
    history_review_requested: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ForkResult {
    action: &'static str,
    fork_locator: Locator,
    canonical_locator: Locator,
    fork_path: PathBuf,
    canonical_path: PathBuf,
    fork_remote: String,
}

#[derive(Debug, Clone, Serialize)]
struct SetupResult {
    action: &'static str,
    config_path: PathBuf,
    config: FileConfig,
    note: &'static str,
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
    dependent_locator: Locator,
    controlling_locator: Locator,
    dependent_path: PathBuf,
    controlling_path: PathBuf,
    dependent_remote: String,
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
            RepositoryOperationCommands::Manage(args) => {
                let db = Store::open(&config.state)?;
                manage_repo(&config, &db, &output, args)
            }
            RepositoryOperationCommands::Fork(args) => {
                let db = Store::open(&config.state)?;
                fork_repo(&config, &db, &output, &args.fork_url, &args.canonical)
            }
            RepositoryOperationCommands::Worktree(command) => match command.command {
                WorktreeSubcommand::Add(args) => {
                    let db = Store::open(&config.state)?;
                    add_worktree(&config, &db, &output, args)
                }
            },
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
                        related_resolve(&db, &output, args.id, &args.kind)
                    }
                }
            }
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
        serde_json::from_str(&content).with_context(|| format!("parsing {}", path.display()))
    }

    fn merge(&mut self, other: Self) {
        self.state = other.state.or_else(|| self.state.take());
        self.cache_root = other.cache_root.or_else(|| self.cache_root.take());
        self.root = other.root.or_else(|| self.root.take());
        self.rpc_url = other.rpc_url.or_else(|| self.rpc_url.take());
        self.client_id = other.client_id.or_else(|| self.client_id.take());
        self.assume_origin_as_canonical = other
            .assume_origin_as_canonical
            .or(self.assume_origin_as_canonical);
        self.detect_related = other.detect_related.or(self.detect_related);
        self.clone_start_ttl_minutes = other
            .clone_start_ttl_minutes
            .or(self.clone_start_ttl_minutes);
        self.rpc_rate_limit_per_second = other
            .rpc_rate_limit_per_second
            .or(self.rpc_rate_limit_per_second);
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

        Ok((
            Self {
                state,
                detect_related,
                clone_start_ttl_minutes,
                rpc_rate_limit_per_second,
            },
            rpc_url,
        ))
    }
}

fn setup_config(config: &Config, output: &Output, args: SetupArgs) -> Result<()> {
    let config_path = args.file.unwrap_or_else(|| config.config_path.clone());
    let file_config = FileConfig {
        state: Some(args.state.unwrap_or_else(|| config.state.clone())),
        cache_root: Some(args.cache_root.unwrap_or_else(|| config.cache_root.clone())),
        root: Some(args.root.unwrap_or_else(|| config.root.clone())),
        rpc_url: Some(args.rpc_url.unwrap_or_else(|| config.rpc_url.clone())),
        client_id: Some(args.client_id.unwrap_or_else(|| config.client_id.clone())),
        assume_origin_as_canonical: args
            .assume_origin_as_canonical
            .or(Some(config.assume_origin_as_canonical)),
        detect_related: None,
        clone_start_ttl_minutes: None,
        rpc_rate_limit_per_second: None,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct GitRemote {
    name: String,
    url: String,
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
    let clone_result = if which::which("ghq").is_ok() {
        match run_clone_command_with_cancellation(
            ghq_get_command(&config.clone_root, url),
            "ghq get",
            &lifecycle,
        )? {
            CloneCommandOutcome::Success => Ok(()),
            CloneCommandOutcome::Failed => run_clone_command_with_cancellation(
                git_clone_command(url, &path),
                "git clone",
                &lifecycle,
            )
            .and_then(CloneCommandOutcome::into_result),
        }
    } else {
        run_clone_command_with_cancellation(git_clone_command(url, &path), "git clone", &lifecycle)
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
    output_clone(
        output,
        &CloneResult {
            action: "clone",
            locator,
            path,
        },
    )
}

fn manage_repo(config: &Config, db: &Store, output: &Output, args: ManageArgs) -> Result<()> {
    warn_pending_related(db)?;
    let original_root = git_worktree_root(&args.path)?;
    let remotes = git_remotes(&original_root)?;
    let assume_origin_as_canonical =
        args.assume_origin_as_canonical || config.assume_origin_as_canonical;
    let canonical_url = choose_manage_canonical_url(&remotes, assume_origin_as_canonical)?;
    let locator = Locator::parse(&canonical_url)?;
    let (repo_root, moved_from) = move_repo_into_managed_path(config, &original_root, &locator)?;
    record_manage_remote_relationships(db, &locator, &repo_root, &remotes)?;
    let history_review_requested =
        request_daemon_history_review(config, &locator, &repo_root, &canonical_url);
    output_manage(
        output,
        &ManageResult {
            action: "manage",
            locator,
            canonical_url,
            path: repo_root,
            moved_from,
            history_review_requested,
        },
    )
}

fn choose_manage_canonical_url(
    remotes: &[GitRemote],
    assume_origin_as_canonical: bool,
) -> Result<String> {
    let origin = remotes.iter().find(|remote| remote.name == "origin");
    if let Some(origin) = origin
        && (assume_origin_as_canonical || confirm_origin_as_canonical(origin)?)
    {
        return Ok(origin.url.clone());
    }
    prompt_canonical_url(remotes)
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

fn prompt_canonical_url(remotes: &[GitRemote]) -> Result<String> {
    if remotes.is_empty() {
        return prompt_manual_canonical_url();
    }
    eprintln!("Select the canonical remote:");
    for (index, remote) in remotes.iter().enumerate() {
        eprintln!("  {}. {} {}", index + 1, remote.name, remote.url);
    }
    eprintln!("  none. Enter a canonical URL manually");

    loop {
        eprint!("canonical remote [1-{} or none]: ", remotes.len());
        io::stderr().flush().context("flushing prompt")?;
        let answer = read_prompt_line()?;
        let answer = answer.trim();
        if answer.eq_ignore_ascii_case("none") {
            return prompt_manual_canonical_url();
        }
        if let Ok(index) = answer.parse::<usize>()
            && let Some(remote) = remotes.get(index.saturating_sub(1))
        {
            return Ok(remote.url.clone());
        }
        eprintln!("enter a number from 1 to {} or none", remotes.len());
    }
}

fn prompt_manual_canonical_url() -> Result<String> {
    loop {
        eprint!("canonical URL: ");
        io::stderr().flush().context("flushing prompt")?;
        let answer = read_prompt_line()?;
        let url = answer.trim();
        if !url.is_empty() {
            return Ok(url.to_string());
        }
        eprintln!("canonical URL is required");
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
    db: &Store,
    canonical_locator: &Locator,
    repo_root: &Path,
    remotes: &[GitRemote],
) -> Result<()> {
    let canonical_id = db.upsert_repo(canonical_locator, repo_root, None)?;
    for remote in remotes {
        let Ok(remote_locator) = Locator::parse(&remote.url) else {
            debug!(
                "skipping non-locator-compatible remote {}: {}",
                remote.name, remote.url
            );
            continue;
        };
        if remote_locator == *canonical_locator {
            continue;
        }
        let remote_id =
            db.upsert_repo(&remote_locator, repo_root, Some(&canonical_locator.key()))?;
        db.record_fork(remote_id, canonical_id)?;
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
    Ok((expected_path, Some(repo_root.to_path_buf())))
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

fn git_clone_command(url: &str, path: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_PREFIX")
        .arg("clone")
        .arg(url)
        .arg(path);
    command
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
    let canonical_locator = Locator::parse(canonical_url)?;
    let fork_path = locator_path(&config.clone_root, &fork_locator);
    let canonical_path = locator_path(&config.clone_root, &canonical_locator);
    let fork_remote = fork_remote_name(&fork_locator);
    fs::create_dir_all(fork_path.parent().context("fork path has no parent")?)?;
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
    let canonical_id = db.upsert_repo(&canonical_locator, &canonical_path, None)?;
    let fork_id = db.upsert_repo(&fork_locator, &fork_path, Some(&canonical_locator.key()))?;
    db.record_fork(fork_id, canonical_id)?;
    output_fork(
        output,
        &ForkResult {
            action: "fork",
            fork_locator,
            canonical_locator,
            fork_path,
            canonical_path,
            fork_remote,
        },
    )
}

fn add_worktree(config: &Config, db: &Store, output: &Output, args: WorktreeAddArgs) -> Result<()> {
    warn_pending_related(db)?;
    let locator = Locator::parse(&args.canonical_url)?;
    let plan = plan_worktree_add(
        &config.clone_root,
        &config.dev_worktree_root,
        locator,
        &args.name,
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
    db.upsert_repo(&plan.canonical_locator, &plan.canonical_path, None)?;
    output_worktree(output, &plan)
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

fn related_resolve(db: &Store, output: &Output, id: i64, kind: &str) -> Result<()> {
    validate_relationship_kind(kind)?;
    let shared_git_dir = matches!(kind, "fork" | "mirror")
        .then(|| resolve_related_shared_git_dir(db, id, kind))
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
    )
}

fn materialize_related_shared_git_dir(
    db: &Store,
    dependent_locator: &Locator,
    dependent_path: &Path,
    controlling_locator: &Locator,
    controlling_path: &Path,
    relationship: &str,
) -> Result<SharedGitDirResolution> {
    if !dependent_path.exists() {
        bail!(
            "dependent checkout does not exist: {}",
            dependent_path.display()
        );
    }
    if !controlling_path.exists() {
        bail!(
            "controlling checkout does not exist: {}",
            controlling_path.display()
        );
    }

    let controlling_origin = git_origin_url(controlling_path)?;
    let dependent_url = remote_url_for_locator(controlling_origin.as_deref(), dependent_locator);
    let dependent_remote = related_remote_name(relationship, dependent_locator);
    let default_branch = dependent_default_branch(dependent_path)?;
    let local_branch = dependent_local_branch(relationship, dependent_locator, &default_branch);
    let remote_branch = format!("{dependent_remote}/{default_branch}");
    let already_shared = git_common_dir(dependent_path)? == git_common_dir(controlling_path)?;

    ensure_remote(controlling_path, &dependent_remote, &dependent_url)?;

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
            "moving existing dependent checkout {} to {}",
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
                "creating dependent worktree {} from {}",
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
        "could not determine dependent checkout default branch from origin/HEAD or current branch: {}",
        path.display()
    )
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
                "fetching local dependent refs from {} into {}",
                dependent_path.display(),
                controlling_path.display()
            )
        })?;
    if !status.success() {
        bail!("git fetch from local dependent checkout failed with status {status}");
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
    run_unix_daemon(config, &path, daemon_state)
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
        if path.join(".git").exists() {
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

fn should_prune_scan_dir(name: &std::ffi::OsStr) -> bool {
    matches!(
        name.to_str(),
        Some(".git" | ".direnv" | ".jj" | "target" | "node_modules")
    )
}

fn repo_locator_from_origin(path: &Path) -> Result<Option<Locator>> {
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
            "{} now reuses the Git directory controlled by {}",
            shared_git_dir.dependent_locator.key(),
            shared_git_dir.controlling_locator.key()
        );
        println!(
            "remote on controlling checkout: {} -> {}",
            shared_git_dir.dependent_remote, shared_git_dir.dependent_url
        );
        println!(
            "tracking branch: {} -> {}",
            shared_git_dir.local_branch, shared_git_dir.remote_branch
        );
        if shared_git_dir.converted_to_worktree {
            println!(
                "converted dependent checkout to Git worktree: {}",
                shared_git_dir.dependent_path.display()
            );
        }
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
    use clap::CommandFactory;

    #[test]
    fn top_level_help_uses_grouped_commands_without_duplicate_command_section() {
        let mut command = Cli::command().help_template(<Commands as HelpTemplate>::help_template());
        let help = command.render_help().to_string();

        assert!(help.contains("Command groups:"));
        assert!(help.contains("Repository operations:"));
        assert!(help.contains("Organizational Changes:"));
        assert!(help.contains("Organizational Analysis:"));
        assert!(help.contains("Options:"));
        assert!(!help.contains("\nCommands:\n"));
        assert!(!help.contains("\n    audit"));
        assert!(help.find("Command groups:") < help.find("Options:"));
    }

    #[test]
    fn repo_top_level_help_does_not_advertise_daemon_controls() {
        let mut command = Cli::command().help_template(<Commands as HelpTemplate>::help_template());
        let help = command.render_help().to_string();

        assert!(!help.to_lowercase().contains("daemon"));
        assert!(!help.contains("--detect-related"));
        assert!(!help.contains("--clone-start-ttl-minutes"));
        assert!(!help.contains("--rpc-rate-limit-per-second"));
        assert!(help.contains("--root"));
        assert!(!help.contains("--clone-root"));
        assert!(!help.contains("--worktree-root"));
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
    fn repod_help_keeps_daemon_controls() {
        let help = RepodCli::command().render_help().to_string();

        assert!(help.contains("--detect-related"));
        assert!(help.contains("--clone-start-ttl-minutes"));
        assert!(help.contains("--rpc-rate-limit-per-second"));
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
            rpc_url: default_rpc_url(),
            client_id: generate_client_id().unwrap(),
            assume_origin_as_canonical: false,
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
            rpc_url: default_rpc_url(),
            client_id: generate_client_id().unwrap(),
            assume_origin_as_canonical: false,
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
        FileConfig {
            state: Some(dir.path().join("state/from-file.sqlite")),
            cache_root: Some(dir.path().join("cache/from-file")),
            root: Some(dir.path().join("code/from-file")),
            rpc_url: Some("unix:///tmp/repo-manager-from-file.sock".to_string()),
            client_id: Some("00000000-0000-4000-8000-000000000001".to_string()),
            assume_origin_as_canonical: Some(false),
            detect_related: Some(true),
            clone_start_ttl_minutes: Some(45),
            rpc_rate_limit_per_second: Some(7),
        }
        .save(&config_path)
        .unwrap();

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
            rpc_url: default_rpc_url(),
            client_id: "00000000-0000-4000-8000-000000000003".to_string(),
            assume_origin_as_canonical: false,
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
    }

    #[test]
    fn file_config_merge_lets_later_layers_override_earlier_ones() {
        let dir = tempfile::tempdir().unwrap();
        let mut base = FileConfig {
            state: Some(dir.path().join("state/base.sqlite")),
            cache_root: Some(dir.path().join("cache/base")),
            root: None,
            rpc_url: Some("unix:///run/base.sock".to_string()),
            client_id: None,
            assume_origin_as_canonical: Some(false),
            detect_related: Some(false),
            clone_start_ttl_minutes: Some(60),
            rpc_rate_limit_per_second: Some(1),
        };

        base.merge(FileConfig {
            state: None,
            cache_root: Some(dir.path().join("cache/user")),
            root: Some(dir.path().join("code/user")),
            rpc_url: None,
            client_id: Some("00000000-0000-4000-8000-000000000005".to_string()),
            assume_origin_as_canonical: Some(true),
            detect_related: Some(true),
            clone_start_ttl_minutes: Some(10),
            rpc_rate_limit_per_second: Some(9),
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

    fn test_config(root: &Path) -> Config {
        Config {
            config_path: root.join("config.json"),
            state: root.join("repos.sqlite"),
            cache_root: root.join("cache"),
            root: root.join("code"),
            clone_root: root.join("code/clones"),
            dev_worktree_root: root.join("code/dev-worktrees"),
            rpc_url: default_rpc_url(),
            client_id: "00000000-0000-4000-8000-000000000099".to_string(),
            assume_origin_as_canonical: false,
        }
    }

    fn test_daemon_config(root: &Path) -> DaemonConfig {
        DaemonConfig {
            state: root.join("repos.sqlite"),
            detect_related: true,
            clone_start_ttl_minutes: 60,
            rpc_rate_limit_per_second: 0,
        }
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

        related_resolve(&store, &Output { json: true }, suggestion.id, "fork").unwrap();

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

        related_resolve(&store, &Output { json: true }, suggestion.id, "mirror").unwrap();

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
