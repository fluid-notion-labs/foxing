// SPDX-License-Identifier: GPL-2.0-or-later
// xtask — generate man pages and shell completions for foxing tools

use clap::{Command, Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate man pages to dist/man/
    Man,
    /// Generate shell completions to dist/completions/
    Completions,
    /// Generate both man pages and completions
    All,
}

/// Build the fxcp CLI command tree (mirrors fxcp-core::sync::FxcpCli)
fn fxcp_cmd() -> Command {
    Command::new("fxcp")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Smart filesystem copy with CoW/reflink/io_uring support")
        .arg(clap::Arg::new("paths").required(true).num_args(2..).help("Source path(s) and destination — last argument is destination"))
        .arg(clap::Arg::new("archive").short('a').long("archive").action(clap::ArgAction::SetTrue).help("Archive mode (recursive, preserve attributes)"))
        .arg(clap::Arg::new("recursive").short('r').long("recursive").action(clap::ArgAction::SetTrue).help("Recursive copy (implied by -a)"))
        .arg(clap::Arg::new("verify").short('v').long("verify").action(clap::ArgAction::SetTrue).help("BLAKE3 verification after copy"))
        .arg(clap::Arg::new("delete").long("delete").action(clap::ArgAction::SetTrue).help("Delete files in target not present in source"))
        .arg(clap::Arg::new("dry_run").short('n').long("dry-run").action(clap::ArgAction::SetTrue).help("Dry run — show what would be copied"))
        .arg(clap::Arg::new("exclude").short('e').long("exclude").action(clap::ArgAction::Append).help("Exclude pattern (glob)"))
        .arg(clap::Arg::new("include").long("include").action(clap::ArgAction::Append).help("Include pattern — override excludes (glob)"))
        .arg(clap::Arg::new("exclude_from").long("exclude-from").help("Read exclude patterns from FILE (one per line)"))
        .arg(clap::Arg::new("include_from").long("include-from").help("Read include patterns from FILE (one per line)"))
        .arg(clap::Arg::new("cleanup").long("cleanup").action(clap::ArgAction::SetTrue).help("Clean orphaned .tmp files and stale dirty flags"))
        .arg(clap::Arg::new("size").long("size").value_parser(clap::value_parser!(u64)).help("Expected size in bytes (for stdin pre-allocation)"))
        .arg(clap::Arg::new("checkpoint_interval").long("checkpoint-interval").value_parser(clap::value_parser!(u64)).help("Interval in seconds to create CoW checkpoints of stdin stream"))
        .arg(clap::Arg::new("checkpoint_keep").long("checkpoint-keep").default_value("5").value_parser(clap::value_parser!(usize)).help("Number of stream checkpoints to keep"))
        .arg(clap::Arg::new("zero_copy").long("zero-copy").action(clap::ArgAction::SetTrue).help("Use zero-copy splice (mutually exclusive with sparse detection)"))
        .arg(clap::Arg::new("debug").long("debug").action(clap::ArgAction::SetTrue).help("Increase verbosity"))
        .arg(clap::Arg::new("generate_sigs").long("generate-sigs").action(clap::ArgAction::SetTrue).help("Generate foxingd-compatible sync signatures (xattr/sidecar) for fast resync"))
}

/// Build the foxingd CLI command tree (mirrors foxingd main.rs Cli)
fn foxingd_cmd() -> Command {
    Command::new("foxing")
        .version(env!("CARGO_PKG_VERSION"))
        .about("High-Performance Filesystem Replication Daemon")
        .arg(clap::Arg::new("verbose").short('v').long("verbose").global(true).action(clap::ArgAction::Count).help("Increase verbosity (-v: Debug, -vv: Trace)"))
        .arg(clap::Arg::new("ui").long("ui").action(clap::ArgAction::SetTrue).help("Launch the interactive TUI"))
        .subcommand(
            Command::new("daemon")
                .about("Start the replication daemon")
                .arg(clap::Arg::new("config").short('c').long("config").default_value("config.toml"))
                .arg(clap::Arg::new("tui").short('t').long("tui").action(clap::ArgAction::SetTrue)),
        )
        .subcommand(
            Command::new("sync")
                .visible_alias("cp")
                .visible_alias("copy")
                .about("Rsync-like one-shot synchronization")
                .arg(clap::Arg::new("source").required(true).help("Source path"))
                .arg(clap::Arg::new("destination").required(true).help("Destination path"))
                .arg(clap::Arg::new("archive").short('a').long("archive").action(clap::ArgAction::SetTrue).help("Archive mode"))
                .arg(clap::Arg::new("recursive").short('r').long("recursive").action(clap::ArgAction::SetTrue).help("Recursive copy"))
                .arg(clap::Arg::new("snapshot").long("snapshot").action(clap::ArgAction::SetTrue).help("Preserve versions using Reflinks/CoW"))
                .arg(clap::Arg::new("watch").long("watch").action(clap::ArgAction::SetTrue).help("Watch for changes after initial sync"))
                .arg(clap::Arg::new("exclude").short('e').long("exclude").action(clap::ArgAction::Append).help("Exclude pattern (glob)"))
                .arg(clap::Arg::new("profile").long("profile").help("I/O Profile: NVMe, SSD, HDD, Network"))
                .arg(clap::Arg::new("dry_run").short('n').long("dry-run").action(clap::ArgAction::SetTrue).help("Dry run")),
        )
        .subcommand(
            Command::new("check")
                .about("Validate configuration")
                .arg(clap::Arg::new("config").short('c').long("config").required(true)),
        )
        .subcommand(
            Command::new("snapshot")
                .visible_alias("snap")
                .about("Version management (MARS)")
                .subcommand(Command::new("list").about("List versions").arg(clap::Arg::new("path").required(true)))
                .subcommand(Command::new("revert").about("Revert to version").arg(clap::Arg::new("path").required(true)).arg(clap::Arg::new("epoch").required(true).value_parser(clap::value_parser!(u64))))
                .subcommand(Command::new("copy").about("Copy a version").arg(clap::Arg::new("path").required(true)).arg(clap::Arg::new("epoch").required(true).value_parser(clap::value_parser!(u64))).arg(clap::Arg::new("destination").required(true)))
                .subcommand(Command::new("cleanup").about("Clean old versions").arg(clap::Arg::new("path").required(true)).arg(clap::Arg::new("dry_run").long("dry-run").action(clap::ArgAction::SetTrue)))
                .subcommand(Command::new("force").about("Force a snapshot").arg(clap::Arg::new("path").required(true)).arg(clap::Arg::new("tag").short('t').long("tag").required(true))),
        )
        .subcommand(Command::new("explain").about("Show configuration guide"))
        .subcommand(Command::new("status").about("Show daemon status"))
        .subcommand(
            Command::new("metrics")
                .about("Show Prometheus metrics")
                .arg(clap::Arg::new("ui").long("ui").action(clap::ArgAction::SetTrue).help("Launch TUI monitor")),
        )
}

fn generate_man_pages(outdir: &PathBuf) {
    fs::create_dir_all(outdir).expect("failed to create man output dir");

    // fxcp man page
    let fxcp = fxcp_cmd();
    let man = clap_mangen::Man::new(fxcp);
    let mut buf = Vec::new();
    man.render(&mut buf).expect("failed to render fxcp man page");
    fs::write(outdir.join("fxcp.1"), buf).expect("failed to write fxcp.1");

    // foxingd man page + subcommand pages
    let foxingd = foxingd_cmd();
    let man = clap_mangen::Man::new(foxingd.clone());
    let mut buf = Vec::new();
    man.render(&mut buf).expect("failed to render foxingd man page");
    fs::write(outdir.join("foxingd.1"), buf).expect("failed to write foxingd.1");

    for subcmd in foxingd.get_subcommands() {
        let name = format!("foxingd-{}", subcmd.get_name());
        let man = clap_mangen::Man::new(subcmd.clone());
        let mut buf = Vec::new();
        man.render(&mut buf).expect("failed to render subcommand man page");
        fs::write(outdir.join(format!("{name}.1")), buf)
            .unwrap_or_else(|_| panic!("failed to write {name}.1"));
    }

    eprintln!("Man pages generated in {}", outdir.display());
}

fn generate_completions(outdir: &PathBuf) {
    use clap_complete::{generate, Shell};

    fs::create_dir_all(outdir).expect("failed to create completions output dir");

    for (cmd_fn, name) in [(fxcp_cmd as fn() -> Command, "fxcp"), (foxingd_cmd as fn() -> Command, "foxingd")] {
        for shell in [Shell::Bash, Shell::Zsh, Shell::Fish] {
            let mut cmd = cmd_fn();
            let mut buf = Vec::new();
            generate(shell, &mut cmd, name, &mut buf);
            let ext = match shell {
                Shell::Bash => "bash",
                Shell::Zsh => "zsh",
                Shell::Fish => "fish",
                _ => unreachable!(),
            };
            fs::write(outdir.join(format!("{name}.{ext}")), buf)
                .unwrap_or_else(|_| panic!("failed to write {name}.{ext}"));
        }
    }

    eprintln!("Shell completions generated in {}", outdir.display());
}

fn main() {
    let cli = Cli::parse();
    let man_dir = PathBuf::from("dist/man");
    let comp_dir = PathBuf::from("dist/completions");

    match cli.cmd {
        Cmd::Man => generate_man_pages(&man_dir),
        Cmd::Completions => generate_completions(&comp_dir),
        Cmd::All => {
            generate_man_pages(&man_dir);
            generate_completions(&comp_dir);
        }
    }
}
