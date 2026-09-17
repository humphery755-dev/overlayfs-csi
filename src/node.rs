use std::path::Path;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::base::{self, Base, Store};
use crate::v1;

/// stage 决策：返回 Some(base) 表示 overlay stage，None 表示 bind mount。
pub fn stage_overlay_decision(store: &Store, max_age_s: i64) -> anyhow::Result<Option<Base>> {
    store.find_valid_base(max_age_s)
}

pub struct NodeService {
    pub node_id: String,
    pub store: Arc<Store>,
    pub max_age_s: i64,
}

fn unimplemented() -> Status {
    Status::unimplemented("Unimplemented")
}

// tonic 服务契约：返回 Result<_, Status>；Status 体积由 tonic 定义，非本层可缩小
#[allow(clippy::result_large_err)]
fn volume_dir_checked(volume_id: &str, store: &Store) -> Result<std::path::PathBuf, Status> {
    let dir = store.volume_dir(volume_id);
    if dir.exists() {
        Ok(dir)
    } else {
        Err(Status::internal(format!(
            "volume directory {} does not exist on this node",
            dir.display()
        )))
    }
}

#[async_trait::async_trait]
impl v1::node_server::Node for NodeService {
    async fn node_stage_volume(
        &self,
        req: Request<v1::NodeStageVolumeRequest>,
    ) -> Result<Response<v1::NodeStageVolumeResponse>, Status> {
        let req = req.into_inner();
        // CSI spec: volume_id/staging_target_path/target_path 均为 required 字段，prost 生成为 String
        let staging = req.staging_target_path;
        tracing::info!(volume_id = %req.volume_id, %staging, "staging volume");
        let vdir = volume_dir_checked(&req.volume_id, &self.store)?;
        let work = self.store.work_dir(&req.volume_id);
        std::fs::create_dir_all(&work).map_err(|e| Status::internal(e.to_string()))?;
        let result = match stage_overlay_decision(&self.store, self.max_age_s) {
            Ok(Some(base)) => {
                tracing::info!(base = %base.0.display(), "staging as overlay");
                base::mount_overlay(&req.volume_id, &base.0, &vdir, &work, false, Path::new(&staging))
            }
            Ok(None) => {
                tracing::info!("no valid base, staging as bind mount");
                base::mount_bind(&vdir, Path::new(&staging))
            }
            Err(e) => Err(e),
        };
        result.map_err(|e| {
            tracing::error!(volume_id = %req.volume_id, "stage failed: {e:#}");
            Status::internal(format!("stage failed: {e:#}"))
        })?;
        Ok(Response::new(Default::default()))
    }

    async fn node_unstage_volume(
        &self,
        req: Request<v1::NodeUnstageVolumeRequest>,
    ) -> Result<Response<v1::NodeUnstageVolumeResponse>, Status> {
        let req = req.into_inner();
        let staging = req.staging_target_path;
        tracing::info!(volume_id = %req.volume_id, %staging, "unstaging volume");
        base::umount_idempotent(Path::new(&staging)).map_err(|e| {
            tracing::error!(volume_id = %req.volume_id, "unstage umount failed: {e:#}");
            Status::internal(format!("unstage failed: {e:#}"))
        })?;

        // base 固化：upper 中有 .as_base 且当前无有效 base（spec 规则）
        let vdir = self.store.volume_dir(&req.volume_id);
        let marker = vdir.join(base::AS_BASE_FILENAME);
        if marker.exists() {
            // TOCTOU：stage 决策只评估一次，promote 复用同一结果。
            // 若 promote 内部重新评估，并发 unstage 可能在间隙固化出新 base，
            // 导致把别的 volume 数据合并进本次固化结果。
            let decision = stage_overlay_decision(&self.store, self.max_age_s)
                .map_err(|e| Status::internal(e.to_string()))?;
            match &decision {
                None => {
                    let new_id = uuid::Uuid::new_v4().to_string();
                    tracing::info!(volume_id = %req.volume_id, base_id = %new_id, "promoting volume to base");
                    self.promote(&req.volume_id, &vdir, &new_id, decision.clone())
                        .map_err(|e| {
                            tracing::error!("promote failed: {e:#}");
                            Status::internal(format!("base promotion failed: {e:#}"))
                        })?;
                    std::fs::remove_file(&marker).map_err(|e| Status::internal(e.to_string()))?;
                }
                Some(_) => tracing::info!("valid base exists, skipping promotion"),
            }
        }
        // work 目录回收
        let work = self.store.work_dir(&req.volume_id);
        if work.exists() {
            std::fs::remove_dir_all(&work).map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(Default::default()))
    }

    async fn node_publish_volume(
        &self,
        req: Request<v1::NodePublishVolumeRequest>,
    ) -> Result<Response<v1::NodePublishVolumeResponse>, Status> {
        let req = req.into_inner();
        let target = req.target_path;
        let staging = req.staging_target_path;
        tracing::info!(volume_id = %req.volume_id, %target, %staging, "publishing volume");
        std::fs::create_dir_all(&target).map_err(|e| Status::internal(e.to_string()))?;
        base::mount_bind(Path::new(&staging), Path::new(&target)).map_err(|e| {
            tracing::error!("publish failed: {e:#}");
            Status::internal(format!("publish failed: {e:#}"))
        })?;
        Ok(Response::new(Default::default()))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<v1::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<v1::NodeUnpublishVolumeResponse>, Status> {
        let req = req.into_inner();
        let target = req.target_path;
        tracing::info!(volume_id = %req.volume_id, %target, "unpublishing volume");
        base::umount_idempotent(Path::new(&target)).map_err(|e| {
            tracing::error!("unpublish failed: {e:#}");
            Status::internal(format!("unpublish failed: {e:#}"))
        })?;
        Ok(Response::new(Default::default()))
    }

    async fn node_get_volume_stats(
        &self,
        _req: Request<v1::NodeGetVolumeStatsRequest>,
    ) -> Result<Response<v1::NodeGetVolumeStatsResponse>, Status> {
        Err(unimplemented())
    }

    async fn node_expand_volume(
        &self,
        _req: Request<v1::NodeExpandVolumeRequest>,
    ) -> Result<Response<v1::NodeExpandVolumeResponse>, Status> {
        Err(unimplemented())
    }

    async fn node_get_capabilities(
        &self,
        _req: Request<v1::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<v1::NodeGetCapabilitiesResponse>, Status> {
        // 必须通告 STAGE_UNSTAGE_VOLUME：kubelet 仅在驱动声明该能力时才调用
        // NodeStageVolume；否则两段式退化为无 staging 的 publish，流程无法工作。
        Ok(Response::new(v1::NodeGetCapabilitiesResponse {
            capabilities: vec![v1::NodeServiceCapability {
                r#type: Some(v1::node_service_capability::Type::Rpc(
                    v1::node_service_capability::Rpc {
                        r#type: v1::node_service_capability::rpc::Type::StageUnstageVolume as i32,
                    },
                )),
            }],
        }))
    }

    async fn node_get_info(
        &self,
        _req: Request<v1::NodeGetInfoRequest>,
    ) -> Result<Response<v1::NodeGetInfoResponse>, Status> {
        Ok(Response::new(v1::NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            ..Default::default()
        }))
    }
}

impl NodeService {
    /// 固化：有旧 base → overlay ro 合并视图；无 → 数据目录即完整视图。
    /// `lower` 由调用方评估后传入：固化路径内不得重新评估 stage 决策（TOCTOU）。
    fn promote(
        &self,
        volume_id: &str,
        vdir: &Path,
        new_id: &str,
        lower: Option<Base>,
    ) -> anyhow::Result<std::path::PathBuf> {
        match lower {
            Some(b) => {
                let merged = std::env::temp_dir().join(format!("ofcsi-merged-{new_id}"));
                std::fs::create_dir_all(&merged)?;
                let work = self.store.work_dir(volume_id);
                std::fs::create_dir_all(&work)?;
                let mounted = base::mount_overlay(
                    &format!("promote-{new_id}"),
                    &b.0,
                    vdir,
                    &work,
                    true,
                    &merged,
                );
                let outcome = mounted.and_then(|()| {
                    base::promote_to_base_with_mount(&self.store, &merged, new_id)
                });
                let um = base::umount_idempotent(&merged);
                std::fs::remove_dir_all(&merged)?;
                um?;
                outcome
            }
            None => base::promote_to_base_with_mount(&self.store, vdir, new_id),
        }
    }
}

/// Controller 服务：实际供给由 controller.rs 的 watcher 驱动（spec 锁定）。
pub struct ControllerService;

#[async_trait::async_trait]
impl v1::controller_server::Controller for ControllerService {
    async fn controller_get_capabilities(
        &self,
        _req: Request<v1::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<v1::ControllerGetCapabilitiesResponse>, Status> {
        Ok(Response::new(v1::ControllerGetCapabilitiesResponse {
            capabilities: vec![v1::ControllerServiceCapability {
                r#type: Some(v1::controller_service_capability::Type::Rpc(
                    v1::controller_service_capability::Rpc {
                        r#type: v1::controller_service_capability::rpc::Type::CreateDeleteVolume
                            as i32,
                    },
                )),
            }],
        }))
    }

    async fn create_volume(
        &self,
        _req: Request<v1::CreateVolumeRequest>,
    ) -> Result<Response<v1::CreateVolumeResponse>, Status> {
        Err(unimplemented())
    }

    async fn delete_volume(
        &self,
        _req: Request<v1::DeleteVolumeRequest>,
    ) -> Result<Response<v1::DeleteVolumeResponse>, Status> {
        Err(unimplemented())
    }

    async fn controller_publish_volume(
        &self,
        _req: Request<v1::ControllerPublishVolumeRequest>,
    ) -> Result<Response<v1::ControllerPublishVolumeResponse>, Status> {
        Err(unimplemented())
    }

    async fn controller_unpublish_volume(
        &self,
        _req: Request<v1::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<v1::ControllerUnpublishVolumeResponse>, Status> {
        Err(unimplemented())
    }

    async fn validate_volume_capabilities(
        &self,
        _req: Request<v1::ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<v1::ValidateVolumeCapabilitiesResponse>, Status> {
        Err(unimplemented())
    }

    async fn list_volumes(
        &self,
        _req: Request<v1::ListVolumesRequest>,
    ) -> Result<Response<v1::ListVolumesResponse>, Status> {
        Err(unimplemented())
    }

    async fn get_capacity(
        &self,
        _req: Request<v1::GetCapacityRequest>,
    ) -> Result<Response<v1::GetCapacityResponse>, Status> {
        Err(unimplemented())
    }

    async fn create_snapshot(
        &self,
        _req: Request<v1::CreateSnapshotRequest>,
    ) -> Result<Response<v1::CreateSnapshotResponse>, Status> {
        Err(unimplemented())
    }

    async fn delete_snapshot(
        &self,
        _req: Request<v1::DeleteSnapshotRequest>,
    ) -> Result<Response<v1::DeleteSnapshotResponse>, Status> {
        Err(unimplemented())
    }

    async fn list_snapshots(
        &self,
        _req: Request<v1::ListSnapshotsRequest>,
    ) -> Result<Response<v1::ListSnapshotsResponse>, Status> {
        Err(unimplemented())
    }

    async fn controller_expand_volume(
        &self,
        _req: Request<v1::ControllerExpandVolumeRequest>,
    ) -> Result<Response<v1::ControllerExpandVolumeResponse>, Status> {
        Err(unimplemented())
    }

    async fn controller_get_volume(
        &self,
        _req: Request<v1::ControllerGetVolumeRequest>,
    ) -> Result<Response<v1::ControllerGetVolumeResponse>, Status> {
        Err(unimplemented())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::AS_BASE_FILENAME;

    #[test]
    fn decision_returns_valid_base_only() {
        let dir = std::env::temp_dir().join(format!("ofcsi-node-{}", std::process::id()));
        let store = Store::new(&dir);
        std::fs::create_dir_all(store.bases_dir()).unwrap();
        // 只有过期 base → bind
        let expired = store.bases_dir().join("expired");
        std::fs::create_dir_all(&expired).unwrap();
        std::fs::write(
            expired.join(AS_BASE_FILENAME),
            "2020-01-01T00:00:00.000000000Z",
        )
        .unwrap();
        assert_eq!(stage_overlay_decision(&store, 3600).unwrap(), None);
        // 新鲜 base → overlay
        let fresh = store.bases_dir().join("fresh");
        std::fs::create_dir_all(&fresh).unwrap();
        Base(fresh.clone()).write_time().unwrap();
        assert_eq!(stage_overlay_decision(&store, 3600).unwrap(), Some(Base(fresh)));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
