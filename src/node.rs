use std::path::Path;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::base::{self, Base, Store};
use crate::controller::VOLUME_ATTR_MAX_AGE;
use crate::v1;

#[derive(Clone)]
pub struct NodeService {
    pub node_id: String,
    pub store: Arc<Store>,
    pub max_age_s: i64,
}

fn unimplemented() -> Status {
    Status::unimplemented("Unimplemented")
}

/// io/anyhow 错误 → internal Status（无日志，用于预期内失败如缺目录）。
fn internal<E: Into<anyhow::Error>>(e: E) -> Status {
    Status::internal(e.into().to_string())
}

/// 带卷上下文的操作错误：记 ERROR 日志并转为 internal Status。
fn logged<'a>(op: &'static str, volume_id: &'a str) -> impl Fn(anyhow::Error) -> Status + 'a {
    move |e| {
        tracing::error!(volume_id, "{op} failed: {e:#}");
        Status::internal(format!("{op} failed: {e:#}"))
    }
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
        std::fs::create_dir_all(&work).map_err(internal)?;
        // 卷级 TTL：PVC 注解经 provision 写入 PV volume_attributes，kubelet 在 stage
        // 请求的 volume_context 中透传。unstage 请求没有该字段，故持久化到数据目录
        // 的元文件供 unstage 固化时读取。PV 的 volume_attributes 不可变，元文件不会陈旧。
        if let Some(v) = match req.volume_context.get(VOLUME_ATTR_MAX_AGE).map(|s| s.parse::<i64>()) {
            Some(Ok(v)) => Some(v),
            Some(Err(e)) => {
                tracing::warn!(
                    value = req.volume_context.get(VOLUME_ATTR_MAX_AGE).map(String::as_str),
                    err = %e,
                    "invalid volume maxAgeSeconds, ignoring"
                );
                None
            }
            None => None,
        } {
            std::fs::write(vdir.join(base::VOLUME_TTL_FILENAME), v.to_string()).map_err(internal)?;
        }
        let result = match self.store.find_valid_base(self.max_age_s) {
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
        result.map_err(logged("stage", &req.volume_id))?;
        Ok(Response::new(Default::default()))
    }

    async fn node_unstage_volume(
        &self,
        req: Request<v1::NodeUnstageVolumeRequest>,
    ) -> Result<Response<v1::NodeUnstageVolumeResponse>, Status> {
        let req = req.into_inner();
        let staging = req.staging_target_path;
        tracing::info!(volume_id = %req.volume_id, %staging, "unstaging volume");
        // umount 前读 mountinfo：staging 若是 overlay，其 lowerdir（旧 base）必须并入本次固化。
        // 注意旧 base 的 TTL 可能已过期——固化条件由 find_valid_base 单独评估，
        // 但它的内容仍属于新 base。此处 .ok() 是约定的上下文读取：mountinfo/挂载缺失
        // 即无 lower，与 bind-stage 卷语义一致（None → 直接拷贝）。
        let mounts = std::fs::read_to_string("/proc/self/mountinfo")
            .ok()
            .map(|mi| base::overlay_lowerdirs(&mi));
        let mounted_lower = mounts
            .as_deref()
            .and_then(|m| base::mounted_lower_in(m, &staging));
        base::umount_idempotent(Path::new(&staging)).map_err(logged("unstage umount", &req.volume_id))?;

        // base 固化：upper 中有 .as_base 且当前无有效 base（spec 规则）
        let vdir = self.store.volume_dir(&req.volume_id);
        let marker = vdir.join(base::AS_BASE_FILENAME);
        if marker.exists() {
            // TOCTOU：stage 决策只评估一次，promote 复用同一结果。
            // 若 promote 内部重新评估，并发 unstage 可能在间隙固化出新 base，
            // 导致把别的 volume 数据合并进本次固化结果。
            let decision = self
                .store
                .find_valid_base(self.max_age_s)
                .map_err(internal)?;
            match decision {
                None => {
                    let new_id = uuid::Uuid::new_v4().to_string();
                    // 卷级 TTL：stage 时持久化在数据目录元文件（unstage 请求无
                    // volume_context 字段）。缺失/损坏 → 回退全局。
                    let volume_ttl =
                        match std::fs::read_to_string(vdir.join(base::VOLUME_TTL_FILENAME)) {
                            Ok(s) => match s.trim().parse::<i64>() {
                                Ok(v) => Some(v),
                                Err(e) => {
                                    tracing::warn!(
                                        value = %s,
                                        err = %e,
                                        "invalid volume ttl file, falling back to global"
                                    );
                                    None
                                }
                            },
                            Err(_) => None,
                        };
                    tracing::info!(volume_id = %req.volume_id, base_id = %new_id, ttl = ?volume_ttl, "promoting volume to base");
                    // cp 可能分钟级（无 reflink 的文件系统）：放阻塞线程池，
                    // 不占用 async worker。
                    let this = self.clone();
                    let vid = req.volume_id.clone();
                    let vdir = vdir.clone();
                    let lower = mounted_lower.map(base::Base);
                    tokio::task::spawn_blocking(move || {
                        this.promote(&vid, &vdir, &new_id, lower, volume_ttl)
                    })
                    .await
                    .map_err(|e| Status::internal(format!("promote task aborted: {e}")))?
                    .map_err(logged("base promotion", &req.volume_id))?;
                    std::fs::remove_file(&marker).map_err(internal)?;
                }
                Some(_) => tracing::info!("valid base exists, skipping promotion"),
            }
        }
        // work 目录回收
        let work = self.store.work_dir(&req.volume_id);
        base::remove_dir_all_if_exists(&work).map_err(internal)?;
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
        std::fs::create_dir_all(&target).map_err(internal)?;
        base::mount_bind(Path::new(&staging), Path::new(&target))
            .map_err(logged("publish", &req.volume_id))?;
        Ok(Response::new(Default::default()))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<v1::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<v1::NodeUnpublishVolumeResponse>, Status> {
        let req = req.into_inner();
        let target = req.target_path;
        tracing::info!(volume_id = %req.volume_id, %target, "unpublishing volume");
        base::umount_idempotent(Path::new(&target)).map_err(logged("unpublish", &req.volume_id))?;
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
    /// lower 来自 staging 挂载在 mountinfo 中的记录（可能已过 TTL；
    /// 其内容仍属于新 base，必须并入合并视图）。
    fn promote(
        &self,
        volume_id: &str,
        vdir: &Path,
        new_id: &str,
        lower: Option<Base>,
        ttl: Option<i64>,
    ) -> anyhow::Result<std::path::PathBuf> {
        match lower {
            Some(b) => {
                let merged = std::env::temp_dir().join(format!("ofcsi-merged-{new_id}"));
                std::fs::create_dir_all(&merged)?;
                let work = self.store.work_dir(volume_id);
                std::fs::create_dir_all(&work)?;
                let outcome = (|| {
                    base::mount_overlay(
                        &format!("promote-{new_id}"),
                        &b.0,
                        vdir,
                        &work,
                        true,
                        &merged,
                    )?;
                    base::promote_to_base_with_mount(&self.store, &merged, new_id, ttl)
                })();
                let um = base::umount_idempotent(&merged);
                std::fs::remove_dir_all(&merged)?;
                um?;
                outcome
            }
            None => base::promote_to_base_with_mount(&self.store, vdir, new_id, ttl),
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
    fn find_valid_base_returns_only_valid() {
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
        assert_eq!(store.find_valid_base(3600).unwrap(), None);
        // 新鲜 base → overlay
        let fresh = store.bases_dir().join("fresh");
        std::fs::create_dir_all(&fresh).unwrap();
        Base(fresh.clone()).write_time(None).unwrap();
        assert_eq!(store.find_valid_base(3600).unwrap(), Some(Base(fresh)));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
