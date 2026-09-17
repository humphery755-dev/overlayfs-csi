use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use overlayfs_csi::v1;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tracing::*;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
struct Flags {
    #[clap(flatten)]
    overlay: overlayfs_csi::OverlayFlags,
    #[clap(long, alias = "endpoint")]
    socket: PathBuf,
    #[clap(long, short)]
    debug: bool,
}

struct IdentityService {
    name: String,
}

#[async_trait::async_trait]
impl v1::identity_server::Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _req: tonic::Request<v1::GetPluginInfoRequest>,
    ) -> Result<tonic::Response<v1::GetPluginInfoResponse>, tonic::Status> {
        Ok(tonic::Response::new(v1::GetPluginInfoResponse {
            name: self.name.clone(),
            vendor_version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        }))
    }
    async fn get_plugin_capabilities(
        &self,
        _request: tonic::Request<v1::GetPluginCapabilitiesRequest>,
    ) -> Result<tonic::Response<v1::GetPluginCapabilitiesResponse>, tonic::Status> {
        Ok(tonic::Response::new(Default::default()))
    }
    async fn probe(
        &self,
        _request: tonic::Request<v1::ProbeRequest>,
    ) -> Result<tonic::Response<v1::ProbeResponse>, tonic::Status> {
        Ok(tonic::Response::new(v1::ProbeResponse {
            ready: Some(true),
        }))
    }
}

async fn main_impl(args: Flags) -> anyhow::Result<()> {
    let overlay = args.overlay;
    let store = Arc::new(overlayfs_csi::base::Store::new(overlay.bases.clone()));

    info!("Connecting to Kubernetes API");
    let kube_client = kube::Client::try_default().await?;

    let controller = overlayfs_csi::controller::Controller {
        kube: kube_client.clone(),
        store: store.clone(),
        storage_class: overlay.storage_class.clone(),
        node: overlay.node.clone(),
        driver_name: overlay.name.clone(),
        max_age_s: overlay.max_age_s,
    };
    // provision/cleanup 后台循环；意外退出则进程退出（fast-fail，交给 k8s 重启）
    tokio::spawn(async move {
        if let Err(e) = controller.run().await {
            error!("controller loop exited: {e:#}");
            std::process::exit(1);
        }
    });

    let identity_service = IdentityService {
        name: overlay.name.clone(),
    };
    let node_service = overlayfs_csi::node::NodeService {
        node_id: overlay.node.clone(),
        store,
        max_age_s: overlay.max_age_s,
    };
    let controller_service = overlayfs_csi::node::ControllerService;

    info!("Connecting to socket {:?}", args.socket);
    let _ = std::fs::remove_file(&args.socket);
    let uds = UnixListener::bind(&args.socket)?;
    let uds_stream = UnixListenerStream::new(uds);

    info!("Started server on socket {:?}", args.socket);
    tonic::transport::Server::builder()
        .add_service(v1::node_server::NodeServer::new(node_service))
        .add_service(v1::controller_server::ControllerServer::new(controller_service))
        .add_service(v1::identity_server::IdentityServer::new(identity_service))
        .serve_with_incoming(uds_stream)
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Flags::parse();
    tracing_subscriber::registry()
        .with(Some(tracing_subscriber::fmt::layer().with_filter(
            if args.debug {
                LevelFilter::DEBUG
            } else {
                LevelFilter::INFO
            },
        )))
        .init();

    if let Err(e) = main_impl(args).await {
        error!("{:#?}", e);
        std::process::exit(1);
    }
}
