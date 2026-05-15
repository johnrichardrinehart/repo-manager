use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use directories::BaseDirs;
use repo_help_derive::{HelpGroup, HelpTemplate};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Parser)]
#[command(
    name = "repo",
    version,
    disable_help_subcommand = true,
    about = "Manage local Git repository placement, metadata, forks, worktrees, and old-path aliases",
    long_about = "Manage local Git repositories using a stable locator model: <authority>/<remote-path>.\n\nCanonical repositories and forks live under the clone root. Development worktrees live under the worktree root."
)]
pub struct Cli {
    #[arg(
        long,
        env = "REPO_MANAGER_CONFIG",
        value_name = "PATH",
        help = "Config file path (default: $XDG_CONFIG_HOME/repo-manager/config.json)",
        long_help = "Config file path to load. Defaults to $XDG_CONFIG_HOME/repo-manager/config.json, or ~/.config/repo-manager/config.json when XDG_CONFIG_HOME is unset."
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
        env = "REPO_MANAGER_CLONE_ROOT",
        value_name = "DIR",
        help = "Root directory for canonical clones and fork worktrees (default: ~/code/clones)",
        long_help = "Root directory for canonical repositories and fork worktrees. Defaults to ~/code/clones."
    )]
    clone_root: Option<PathBuf>,

    #[arg(
        long,
        env = "REPO_MANAGER_WORKTREE_ROOT",
        value_name = "DIR",
        help = "Root directory for development worktrees (default: ~/code/worktrees)",
        long_help = "Root directory for development worktrees created by `repo worktree add`. Defaults to ~/code/worktrees."
    )]
    worktree_root: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
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
    #[command(about = "Create or register a fork worktree for a canonical repository")]
    Fork(ForkArgs),
    #[command(about = "Manage development worktrees under the managed worktree root")]
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
        help = "Persist the root directory for canonical clones and fork worktrees"
    )]
    clone_root: Option<PathBuf>,

    #[arg(
        long,
        value_name = "DIR",
        help = "Persist the root directory for development worktrees"
    )]
    worktree_root: Option<PathBuf>,
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
    #[command(about = "Create a development worktree under the managed worktree root")]
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
    clone_root: PathBuf,
    worktree_root: PathBuf,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct FileConfig {
    state: Option<PathBuf>,
    cache_root: Option<PathBuf>,
    clone_root: Option<PathBuf>,
    worktree_root: Option<PathBuf>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    let cli = parse_cli();
    let config = Config::from_cli(&cli)?;

    match cli.command {
        Commands::Setup(command) => match command {
            SetupCommands::Setup(args) => setup_config(&config, args),
        },
        Commands::RepositoryOperations(command) => match command {
            RepositoryOperationCommands::Clone(args) => {
                let db = Store::open(&config.state)?;
                clone_repo(&config, &db, &args.url)
            }
            RepositoryOperationCommands::Fork(args) => {
                let db = Store::open(&config.state)?;
                fork_repo(&config, &db, &args.fork_url, &args.canonical)
            }
            RepositoryOperationCommands::Worktree(command) => match command.command {
                WorktreeSubcommand::Add(args) => {
                    let db = Store::open(&config.state)?;
                    add_worktree(&config, &db, args)
                }
            },
        },
        Commands::OrganizationalChanges(command) => match command {
            OrganizationalChangeCommands::Move(args) => {
                let db = Store::open(&config.state)?;
                move_repo(&config, &db, &args.repo_ref, &args.new_url)
            }
            OrganizationalChangeCommands::Reconcile => {
                let db = Store::open(&config.state)?;
                reconcile(&config, &db)
            }
            OrganizationalChangeCommands::Successor(command) => match command.command {
                SuccessorSubcommand::Set(args) => {
                    let db = Store::open(&config.state)?;
                    successor_set(&config, &db, &args.old_ref, &args.new_url)
                }
            },
        },
        Commands::OrganizationalAnalysis(command) => match command {
            OrganizationalAnalysisCommands::Aliases(command) => match command.command {
                AliasesSubcommand::List(args) => {
                    let db = Store::open(&config.state)?;
                    aliases_list(&db, &args.repo_ref)
                }
            },
        },
    }
}

fn parse_cli() -> Cli {
    let matches = Cli::command()
        .help_template(<Commands as HelpTemplate>::help_template())
        .get_matches();
    Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit())
}

impl Config {
    fn from_cli(cli: &Cli) -> Result<Self> {
        let config_path = cli.config.clone().unwrap_or(default_config_path()?);
        let file_config = FileConfig::load(&config_path)?;
        let state = cli
            .state
            .clone()
            .or(file_config.state)
            .unwrap_or(default_state_path()?);
        let cache_root = cli
            .cache_root
            .clone()
            .or(file_config.cache_root)
            .unwrap_or(default_cache_root()?);
        let clone_root = cli
            .clone_root
            .clone()
            .or(file_config.clone_root)
            .unwrap_or(default_clone_root()?);
        let worktree_root = cli
            .worktree_root
            .clone()
            .or(file_config.worktree_root)
            .unwrap_or(default_worktree_root()?);
        Ok(Self {
            config_path,
            state,
            cache_root,
            clone_root,
            worktree_root,
        })
    }
}

impl FileConfig {
    fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content =
            fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_str(&content).with_context(|| format!("parsing {}", path.display()))
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

fn setup_config(config: &Config, args: SetupArgs) -> Result<()> {
    let config_path = args.file.unwrap_or_else(|| config.config_path.clone());
    let file_config = FileConfig {
        state: Some(args.state.unwrap_or_else(|| config.state.clone())),
        cache_root: Some(args.cache_root.unwrap_or_else(|| config.cache_root.clone())),
        clone_root: Some(args.clone_root.unwrap_or_else(|| config.clone_root.clone())),
        worktree_root: Some(
            args.worktree_root
                .unwrap_or_else(|| config.worktree_root.clone()),
        ),
    };
    file_config.save(&config_path)?;
    print_json(&serde_json::json!({
        "action": "setup",
        "config_path": &config_path,
        "config": &file_config,
        "note": "Environment variables and top-level CLI options override these persisted values at runtime."
    }))
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

fn default_clone_root() -> Result<PathBuf> {
    Ok(home_dir()?.join("code/clones"))
}

fn default_worktree_root() -> Result<PathBuf> {
    Ok(home_dir()?.join("code/worktrees"))
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

#[derive(Debug)]
struct RepoRecord {
    id: i64,
    current: Locator,
}

#[derive(Debug)]
struct ManagedRepoRecord {
    id: i64,
    current: Locator,
    path: PathBuf,
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
}

fn clone_repo(config: &Config, db: &Store, url: &str) -> Result<()> {
    let locator = Locator::parse(url)?;
    let path = locator_path(&config.clone_root, &locator);
    fs::create_dir_all(path.parent().context("clone path has no parent")?)?;
    if which::which("ghq").is_ok() {
        let status = ghq_get_command(&config.clone_root, url)
            .status()
            .context("running ghq get")?;
        if !status.success() {
            run_git(["clone", url, &path.display().to_string()])?;
        }
    } else {
        run_git(["clone", url, &path.display().to_string()])?;
    }
    db.upsert_repo(&locator, &path, None)?;
    print_json(&serde_json::json!({
        "action": "clone",
        "locator": locator,
        "path": path,
    }))
}

fn ghq_get_command(root: &Path, url: &str) -> Command {
    let mut command = Command::new("ghq");
    command.env("GHQ_ROOT", root).arg("get").arg(url);
    command
}

fn fork_repo(config: &Config, db: &Store, fork_url: &str, canonical_url: &str) -> Result<()> {
    let fork_locator = Locator::parse(fork_url)?;
    let canonical_locator = Locator::parse(canonical_url)?;
    let fork_path = locator_path(&config.clone_root, &fork_locator);
    let canonical_path = locator_path(&config.clone_root, &canonical_locator);
    let fork_remote = fork_remote_name(&fork_locator);
    fs::create_dir_all(fork_path.parent().context("fork path has no parent")?)?;
    ensure_remote(&canonical_path, "origin", canonical_url)?;
    ensure_remote(&canonical_path, &fork_remote, fork_url)?;
    run_git_in(&canonical_path, ["fetch", &fork_remote])?;
    let status = Command::new("git")
        .arg("-C")
        .arg(&canonical_path)
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
    print_json(&serde_json::json!({
        "action": "fork",
        "fork_locator": fork_locator,
        "canonical_locator": canonical_locator,
        "fork_path": fork_path,
        "canonical_path": canonical_path,
        "fork_remote": fork_remote,
    }))
}

fn add_worktree(config: &Config, db: &Store, args: WorktreeAddArgs) -> Result<()> {
    let locator = Locator::parse(&args.canonical_url)?;
    let plan = plan_worktree_add(
        &config.clone_root,
        &config.worktree_root,
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
    print_json(&plan)
}

fn move_repo(config: &Config, db: &Store, repo_ref: &str, new_url: &str) -> Result<()> {
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
    print_json(&plan)
}

fn reconcile(config: &Config, db: &Store) -> Result<()> {
    let report = reconcile_repos(config, db)?;
    print_json(&report)
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

fn successor_set(config: &Config, db: &Store, old_ref: &str, new_url: &str) -> Result<()> {
    let new_locator = Locator::parse(new_url)?;
    db.record_successor(old_ref, &new_locator)?;
    print_json(&serde_json::json!({
        "action": "successor-set",
        "old_ref": old_ref,
        "new_locator": new_locator,
        "new_path": locator_path(&config.clone_root, &new_locator),
    }))
}

fn aliases_list(db: &Store, repo_ref: &str) -> Result<()> {
    print_json(&db.aliases(repo_ref)?)
}

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
    format!("fork-{}", sanitize_remote_name(&locator.key()))
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

fn run_git<const N: usize>(args: [&str; N]) -> Result<()> {
    let status = Command::new("git")
        .args(args)
        .status()
        .context("running git")?;
    if !status.success() {
        bail!("git command failed with status {status}");
    }
    Ok(())
}

fn run_git_in<I, S>(cwd: &Path, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let status = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .status()
        .with_context(|| format!("running git in {}", cwd.display()))?;
    if !status.success() {
        bail!("git command failed with status {status}");
    }
    Ok(())
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

fn git_remote_url(cwd: &Path, name: &str) -> Result<Option<String>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
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
            Path::new("/tmp/worktrees"),
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
                "/tmp/worktrees/github.com/torvalds/linux/topic",
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
    fn reconcile_applies_origin_locator_drift() {
        let dir = tempfile::tempdir().unwrap();
        let clone_root = dir.path().join("clones");
        let worktree_root = dir.path().join("worktrees");
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
            clone_root,
            worktree_root,
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
        let clone_root = dir.path().join("clones");
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
            clone_root,
            worktree_root: dir.path().join("worktrees"),
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
            clone_root: Some(dir.path().join("clones/from-file")),
            worktree_root: Some(dir.path().join("worktrees/from-file")),
        }
        .save(&config_path)
        .unwrap();

        let cli = Cli {
            config: Some(config_path.clone()),
            state: None,
            cache_root: Some(dir.path().join("cache/from-cli")),
            clone_root: None,
            worktree_root: None,
            command: Commands::Setup(SetupCommands::Setup(SetupArgs {
                file: None,
                state: None,
                cache_root: None,
                clone_root: None,
                worktree_root: None,
            })),
        };
        let config = Config::from_cli(&cli).unwrap();

        assert_eq!(config.config_path, config_path);
        assert_eq!(config.state, dir.path().join("state/from-file.sqlite"));
        assert_eq!(config.cache_root, dir.path().join("cache/from-cli"));
        assert_eq!(config.clone_root, dir.path().join("clones/from-file"));
        assert_eq!(config.worktree_root, dir.path().join("worktrees/from-file"));
    }

    #[test]
    fn setup_can_write_an_explicit_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            config_path: dir.path().join("default/config.json"),
            state: dir.path().join("state/repos.sqlite"),
            cache_root: dir.path().join("cache"),
            clone_root: dir.path().join("clones"),
            worktree_root: dir.path().join("worktrees"),
        };
        let explicit_file = dir.path().join("custom/repo-config.json");

        setup_config(
            &config,
            SetupArgs {
                file: Some(explicit_file.clone()),
                state: None,
                cache_root: None,
                clone_root: Some(dir.path().join("custom-clones")),
                worktree_root: None,
            },
        )
        .unwrap();

        assert!(!config.config_path.exists());
        let saved = FileConfig::load(&explicit_file).unwrap();
        assert_eq!(saved.state, Some(config.state));
        assert_eq!(saved.cache_root, Some(config.cache_root));
        assert_eq!(saved.clone_root, Some(dir.path().join("custom-clones")));
        assert_eq!(saved.worktree_root, Some(config.worktree_root));
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
