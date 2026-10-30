//! walbench: where to put the durable write log (the "notebook") of the write-ack path.
//! See docs/walbench.md for what each subcommand measures.

mod bench;
mod codec;
mod committer;
mod coord;
mod counters;
mod crash;
mod ebs;
mod quorum;
mod quorumtest;
mod store;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "walbench")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// closed-loop latency cell (conc writers on one log)
    Cell(bench::CellArgs),
    /// open-loop fleet cell (many databases, shared log or one log per database)
    Fleet(bench::FleetArgs),
    /// crash-test child: writes until killed, prints "A <id>" per acknowledged write
    Writer(crash::WriterArgs),
    /// read a log back, print the recovered ids
    Recover(crash::RecoverArgs),
    /// kill -9 / restart / verify cycles
    Crashtest(crash::CrashArgs),
    /// quorum log node
    Node {
        #[arg(long, default_value = "0.0.0.0:7700")]
        listen: String,
        #[arg(long, default_value = "/var/walbench/node")]
        dir: String,
    },
    /// quorum failure scenario (node killed, stale node returning)
    Quorumtest(quorumtest::QtArgs),
    /// EBS reattach holder
    EbsHolder(ebs::HolderArgs),
    /// upload a file to the run bucket
    S3put { #[arg(long)] bucket: String, #[arg(long)] key: String, #[arg(long)] file: String },
    /// download an object of the run bucket
    S3get { #[arg(long)] bucket: String, #[arg(long)] key: String, #[arg(long)] file: String },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // every short-lived walbench process dies with its parent (no orphan writer burning requests); the node and the
    // EBS holder are long-lived and supervised, they keep the default
    if !matches!(cli.cmd, Cmd::Node { .. } | Cmd::EbsHolder(_)) {
        unsafe {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
        }
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().worker_threads(4).max_blocking_threads(64).build()?;
    rt.block_on(async move {
        match cli.cmd {
            Cmd::Cell(a) => bench::cell(a).await,
            Cmd::Fleet(a) => bench::fleet(a).await,
            Cmd::Writer(a) => crash::writer(a).await,
            Cmd::Recover(a) => crash::recover(a).await,
            Cmd::Crashtest(a) => crash::crashtest(a).await,
            Cmd::Node { listen, dir } => quorum::run_node(listen, dir).await,
            Cmd::Quorumtest(a) => quorumtest::quorumtest(a).await,
            Cmd::EbsHolder(a) => ebs::holder(a).await,
            Cmd::S3put { bucket, key, file } => coord::cli_put(&bucket, &key, &file).await,
            Cmd::S3get { bucket, key, file } => coord::cli_get(&bucket, &key, &file).await,
        }
    })
}
