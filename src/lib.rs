pub mod base;
pub mod controller;
pub mod node;
pub mod webhook;

/// 上游 csi.proto（构建时下载）经 tonic-build 生成的代码，不满足 clippy
/// （doc_overindented_list_items / result_large_err），仅对生成模块豁免。
#[allow(clippy::all)]
pub mod v1 {
    tonic::include_proto!("csi.v1");
}

use clap::Parser;

#[derive(Parser)]
pub struct OverlayFlags {
    /// CSI name (driver name)
    #[clap(long)]
    pub name: String,
    #[clap(long, alias = "nodeid")]
    pub node: String,
    /// Host storage root containing bases/, volumes/, work/, vm/
    #[clap(long)]
    pub bases: std::path::PathBuf,
    /// StorageClass name this driver provisions for
    #[clap(long)]
    pub storage_class: String,
    /// Maximum age of a base (seconds) before cleanup
    #[clap(long)]
    pub max_age_s: i64,
    /// VM-like pod webhook listen address (e.g. 0.0.0.0:9443); disabled when unset
    #[clap(long)]
    pub webhook_addr: Option<std::net::SocketAddr>,
    /// TLS certificate PEM for the webhook (required with --webhook-addr)
    #[clap(long, requires = "webhook_addr")]
    pub webhook_cert: Option<std::path::PathBuf>,
    /// TLS private key PEM for the webhook (required with --webhook-addr)
    #[clap(long, requires = "webhook_addr")]
    pub webhook_key: Option<std::path::PathBuf>,
}
