use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{
    CSIPersistentVolumeSource, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    ObjectReference, PersistentVolume, PersistentVolumeClaim, PersistentVolumeSpec,
    VolumeNodeAffinity,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::runtime::watcher;
use kube::Client;

use crate::base::{
    is_referenced_in, overlay_lowerdirs, parse_max_age_s, remove_dir_all_if_exists, Store,
    MAX_AGE_ANNOTATION,
};

pub const PV_PREFIX: &str = "overlayfs-";
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
pub const MANAGED_BY_VALUE: &str = "overlayfs-csi";
pub const SELECTED_NODE_ANNOTATION: &str = "volume.kubernetes.io/selected-node";
// MAX_AGE_ANNOTATION 已上提到 crate::base（PVC 与 Pod 两域共享的 wire 契约）。
/// 上述值的透传载体：PV volume_attributes 键，kubelet 会作为 volume_context
/// 放进 NodeStage/NodeUnstage 请求。
pub const VOLUME_ATTR_MAX_AGE: &str = "maxAgeSeconds";

/// 解析 PVC 的 max-age-s 注解；缺失返回 None，非法值记录告警并返回 None（回退全局）。
fn pvc_max_age_override(pvc: &PersistentVolumeClaim) -> Option<i64> {
    let raw = pvc
        .metadata
        .annotations
        .as_ref()?
        .get(MAX_AGE_ANNOTATION)?
        .as_str();
    // 失败处置（warn+回退全局）是 PVC 域的策略；解析规则共享自 crate::base。
    match parse_max_age_s(raw) {
        Ok(v) => Some(v),
        Err(msg) => {
            tracing::warn!(annotation = MAX_AGE_ANNOTATION, value = raw, "{msg}; falling back to global max-age-s");
            None
        }
    }
}

pub fn pv_name(pvc_uid: &str) -> String {
    format!("{PV_PREFIX}{pvc_uid}")
}

pub fn should_provision(pvc: &PersistentVolumeClaim, storage_class: &str, node: &str) -> bool {
    let phase = pvc.status.as_ref().and_then(|s| s.phase.as_deref());
    let sc = pvc.spec.as_ref().and_then(|s| s.storage_class_name.as_deref());
    let selected = pvc
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(SELECTED_NODE_ANNOTATION).map(|v| v.as_str()));
    phase == Some("Pending") && sc == Some(storage_class) && selected == Some(node)
}

pub fn build_pv(
    pvc: &PersistentVolumeClaim,
    node: &str,
    driver_name: &str,
) -> anyhow::Result<PersistentVolume> {
    let uid = pvc.metadata.uid.as_deref().context("PVC has no uid")?;
    let spec = pvc.spec.as_ref().context("PVC has no spec")?;
    let storage = spec
        .resources
        .as_ref()
        .and_then(|r| r.requests.as_ref())
        .and_then(|r| r.get("storage"))
        .map(|q| q.0.clone())
        .context("PVC requests.storage missing")?;
    let name = pv_name(uid);
    Ok(PersistentVolume {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            labels: Some(BTreeMap::from([(
                MANAGED_BY_LABEL.to_string(),
                MANAGED_BY_VALUE.to_string(),
            )])),
            ..Default::default()
        },
        spec: Some(PersistentVolumeSpec {
            capacity: Some(BTreeMap::from([(
                "storage".to_string(),
                Quantity(storage),
            )])),
            access_modes: spec.access_modes.clone(),
            persistent_volume_reclaim_policy: Some("Delete".to_string()),
            claim_ref: Some(ObjectReference {
                namespace: pvc.metadata.namespace.clone(),
                name: pvc.metadata.name.clone(),
                ..Default::default()
            }),
            node_affinity: Some(VolumeNodeAffinity {
                required: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_fields: Some(vec![NodeSelectorRequirement {
                            key: "metadata.name".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec![node.to_string()]),
                        }]),
                        ..Default::default()
                    }],
                }),
            }),
            csi: Some(CSIPersistentVolumeSource {
                driver: driver_name.to_string(),
                volume_handle: name,
                // 卷级 TTL 经 volume_attributes 透传：kubelet 将其作为 volume_context
                // 放进 NodeStage/NodeUnstage 请求，固化时写入新 base。
                volume_attributes: pvc_max_age_override(pvc).map(|v| {
                    BTreeMap::from([(VOLUME_ATTR_MAX_AGE.to_string(), v.to_string())])
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}

pub const GC_GRACE_S: u64 = 300;
const RECONCILE_INTERVAL_S: u64 = 30;

#[derive(Clone)]
pub struct Controller {
    pub kube: Client,
    pub store: Arc<Store>,
    pub storage_class: String,
    pub node: String,
    pub driver_name: String,
    pub max_age_s: i64,
}

impl Controller {
    pub async fn run(&self) -> anyhow::Result<()> {
        // janitor 独立任务：删除数 GB 目录（remove_dir_all）时不得阻塞 watch 事件
        // 处理（Applied 事件延迟 = provision 延迟）。
        tokio::spawn({
            let this = self.clone();
            async move {
                let mut janitor = tokio::time::interval_at(
                    tokio::time::Instant::now()
                        + std::time::Duration::from_secs(RECONCILE_INTERVAL_S),
                    std::time::Duration::from_secs(RECONCILE_INTERVAL_S),
                );
                loop {
                    janitor.tick().await;
                    // VM GC 不依赖 mountinfo（mountinfo 解析异常不拖累它），
                    // 且与 cleanup_once 操作互不相交的目录树——并行执行，
                    // 一方慢/错不拖延另一方（错误各自记录，故障隔离）。
                    let (r1, r2) = tokio::join!(
                        this.cleanup_once(),
                        crate::webhook::vm_gc_once(&this.kube, &this.store)
                    );
                    if let Err(e) = r1 {
                        tracing::error!("cleanup/GC failed: {e:#}");
                    }
                    if let Err(e) = r2 {
                        tracing::error!("VM GC failed: {e:#}");
                    }
                }
            }
        });

        let pvcs: Api<PersistentVolumeClaim> = Api::all(self.kube.clone());
        let mut events = watcher(pvcs, watcher::Config::default()).boxed();
        // 首个 tick 延后一个周期：启动状态由 watcher 的 Restarted 重放覆盖，
        // 避免 t=0 时同一批 PVC 被全量处理两次。
        let mut reconciler = tokio::time::interval_at(
            tokio::time::Instant::now() + std::time::Duration::from_secs(RECONCILE_INTERVAL_S),
            std::time::Duration::from_secs(RECONCILE_INTERVAL_S),
        );
        loop {
            tokio::select! {
                ev = events.next() => match ev {
                    Some(Ok(watcher::Event::Applied(pvc))) => {
                        if let Err(e) = self.handle_pvc(&pvc).await {
                            tracing::error!(pvc = ?pvc.metadata.name, "provision failed: {e:#}");
                        }
                    }
                    Some(Ok(watcher::Event::Deleted(pvc))) => {
                        if let Err(e) = self.handle_pvc_deleted(&pvc).await {
                            tracing::error!(pvc = ?pvc.metadata.name, "cleanup of deleted PVC failed: {e:#}");
                        }
                    }
                    Some(Ok(watcher::Event::Restarted(pvcs))) => {
                        for pvc in &pvcs {
                            if let Err(e) = self.handle_pvc(pvc).await {
                                tracing::error!(pvc = ?pvc.metadata.name, "reconcile-provision failed: {e:#}");
                            }
                        }
                    }
                    Some(Err(e)) => tracing::error!("PVC watch error (kube_runtime will reconnect): {e}"),
                    None => anyhow::bail!("PVC watch stream ended unexpectedly"),
                },
                _ = reconciler.tick() => {
                    let pvcs: Api<PersistentVolumeClaim> = Api::all(self.kube.clone());
                    match pvcs.list(&ListParams::default()).await {
                        Ok(list) => {
                            for pvc in &list.items {
                                if let Err(e) = self.handle_pvc(pvc).await {
                                    tracing::error!(pvc = ?pvc.metadata.name, "reconcile failed: {e:#}");
                                }
                            }
                        }
                        Err(e) => tracing::error!("reconcile list failed: {e}"),
                    }
                },
            }
        }
    }

    pub async fn handle_pvc(&self, pvc: &PersistentVolumeClaim) -> anyhow::Result<()> {
        if !should_provision(pvc, &self.storage_class, &self.node) {
            return Ok(());
        }
        let uid = pvc.metadata.uid.as_deref().context("PVC uid missing")?;
        let name = pv_name(uid);
        let vdir = self.store.volume_dir(&name);
        tracing::info!(%name, dir = %vdir.display(), "provisioning volume");
        std::fs::create_dir_all(&vdir)?; // 幂等：多实例/重放安全

        let pv = build_pv(pvc, &self.node, &self.driver_name)?;
        let pvs: Api<PersistentVolume> = Api::all(self.kube.clone());
        match pvs.create(&PostParams::default(), &pv).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.reason == "AlreadyExists" => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn handle_pvc_deleted(&self, pvc: &PersistentVolumeClaim) -> anyhow::Result<()> {
        let uid = pvc
            .metadata
            .uid
            .as_deref()
            .context("deleted PVC has no uid")?;
        let name = pv_name(uid);
        let pvs: Api<PersistentVolume> = Api::all(self.kube.clone());
        match pvs.delete(&name, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.reason == "NotFound" => {}
            Err(e) => return Err(e.into()),
        }
        // 数据目录只存在于 selected-node；其他实例自然找不到
        let vdir = self.store.volume_dir(&name);
        if vdir.exists() {
            tracing::info!(%name, dir = %vdir.display(), "removing released volume directory");
            std::fs::remove_dir_all(&vdir)?;
        }
        remove_dir_all_if_exists(&self.store.work_dir(&name))?;
        Ok(())
    }

    pub async fn cleanup_once(&self) -> anyhow::Result<()> {
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
        // 同一份 mountinfo 本轮只解析一次，全部引用检查复用解析结果。
        let mounts = overlay_lowerdirs(&mountinfo);
        // 0 个 overlay 意味着 mountinfo 格式漂移或节点状态异常：引用检查将静默放行，
        // 过期 base 会被误删（README 承诺被引用的删除一律跳过）——本轮整体跳过并告警。
        if mounts.is_empty() {
            tracing::warn!(
                "parsed 0 overlay mounts from mountinfo; skipping this cleanup round"
            );
            return Ok(());
        }
        // 1. base TTL 清理：被 overlay 引用的不删
        for base in self.store.list_bases()? {
            if base.valid(self.max_age_s) {
                continue;
            }
            if is_referenced_in(&mounts, &base.0) {
                tracing::debug!(base = %base.0.display(), "expired base still referenced, keeping");
                continue;
            }
            tracing::warn!(base = %base.0.display(), "removing expired base");
            std::fs::remove_dir_all(&base.0)?;
        }
        // 2. 孤儿 volumes/ GC：保留条件 = managed 集合中有对应 volume_handle
        let pvs: Api<PersistentVolume> = Api::all(self.kube.clone());
        let list = pvs
            .list(&ListParams::default().labels(&format!("{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}")))
            .await?;
        let managed: HashSet<String> = list
            .items
            .iter()
            .filter_map(|pv| Some(pv.spec.as_ref()?.csi.as_ref()?.volume_handle.clone()))
            .collect();
        self.gc_orphans(&self.store.volumes_dir(), &mounts, &|n| managed.contains(n))?;
        // 3. 孤儿 work/ GC：保留条件 = 对应 volume 目录仍存在（挂载中的 staging 必然有 volume 目录）
        let work_root = self.store.work_root();
        if work_root.exists() {
            self.gc_orphans(&work_root, &mounts, &|n| self.store.volume_dir(n).exists())?;
        }
        Ok(())
    }

    /// 统一孤儿清理：保留条件由调用方以 `keep` 谓词注入（volumes 看 PV 集合、
    /// work 看兄弟 volume 目录），grace 与 mountinfo 引用检查为共享逻辑。
    fn gc_orphans(
        &self,
        dir: &Path,
        mounts: &[(String, Vec<PathBuf>)],
        keep: &dyn Fn(&str) -> bool,
    ) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if keep(&name) {
                continue;
            }
            // 廉价检查在前：grace 内的目录不做引用表扫描
            if dir_age_s(&path)? <= GC_GRACE_S {
                continue;
            }
            if is_referenced_in(mounts, &path) {
                continue;
            }
            tracing::warn!(dir = %path.display(), "removing orphaned directory");
            std::fs::remove_dir_all(&path)?;
        }
        Ok(())
    }
}

fn dir_age_s(path: &Path) -> anyhow::Result<u64> {
    let meta = std::fs::metadata(path)?;
    let modified = meta.modified()?;
    let age = std::time::SystemTime::now().duration_since(modified)?;
    Ok(age.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_pvc(
        uid: &str,
        storage_class: &str,
        selected_node: Option<&str>,
        phase: &str,
    ) -> PersistentVolumeClaim {
        serde_yaml::from_str(&format!(
            r#"
metadata:
  name: demo
  namespace: default
  uid: {uid}
  annotations:
    volume.kubernetes.io/selected-node: {node}
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: {sc}
  resources:
    requests:
      storage: 10Gi
status:
  phase: {phase}
"#,
            uid = uid,
            node = selected_node.unwrap_or("other-node"),
            sc = storage_class,
            phase = phase,
        ))
        .unwrap()
    }

    #[test]
    fn pv_name_is_deterministic() {
        assert_eq!(pv_name("abc-123"), "overlayfs-abc-123");
        assert_eq!(pv_name("abc-123"), pv_name("abc-123"));
    }

    #[test]
    fn should_provision_matches_all_conditions() {
        let pvc = fixture_pvc("u1", "overlayfs.csi.k8s.io", Some("node1"), "Pending");
        assert!(should_provision(&pvc, "overlayfs.csi.k8s.io", "node1"));
        assert!(
            !should_provision(&pvc, "overlayfs.csi.k8s.io", "node2"),
            "别人节点的 PVC 不处理"
        );
        assert!(!should_provision(&pvc, "other-sc", "node1"), "SC 不匹配不处理");

        let pending_no_node = fixture_pvc("u2", "overlayfs.csi.k8s.io", None, "Pending");
        assert!(
            !should_provision(&pending_no_node, "overlayfs.csi.k8s.io", "node1"),
            "调度器未选节点不处理"
        );

        let bound = fixture_pvc("u3", "overlayfs.csi.k8s.io", Some("node1"), "Bound");
        assert!(!should_provision(&bound, "overlayfs.csi.k8s.io", "node1"), "已绑定不处理");
    }

    #[test]
    fn build_pv_fields() {
        let pvc = fixture_pvc("u4", "overlayfs.csi.k8s.io", Some("node1"), "Pending");
        let pv = build_pv(&pvc, "node1", "overlayfs.csi.k8s.io").unwrap();
        let spec = pv.spec.as_ref().unwrap();
        let name = pv.metadata.name.as_deref().unwrap();

        assert_eq!(name, "overlayfs-u4");
        assert_eq!(
            pv.metadata
                .labels
                .as_ref()
                .unwrap()
                .get(MANAGED_BY_LABEL)
                .map(String::as_str),
            Some(MANAGED_BY_VALUE)
        );
        assert_eq!(spec.persistent_volume_reclaim_policy.as_deref(), Some("Delete"));
        assert_eq!(spec.storage_class_name, None, "预绑定 PV 不设 storageClassName");
        assert_eq!(spec.access_modes.as_deref(), Some(&["ReadWriteOnce".to_string()][..]));
        let claim = spec.claim_ref.as_ref().unwrap();
        assert_eq!(claim.name.as_deref(), Some("demo"));
        assert_eq!(claim.namespace.as_deref(), Some("default"));
        let csi = spec.csi.as_ref().unwrap();
        assert_eq!(csi.driver, "overlayfs.csi.k8s.io");
        assert_eq!(
            csi.volume_handle, name,
            "volume_handle 必须等于 PV 名（路径推导依赖它）"
        );
        assert_eq!(
            spec.capacity.as_ref().unwrap().get("storage").map(|q| q.0.clone()),
            Some("10Gi".to_string())
        );
        let terms = &spec.node_affinity.as_ref().unwrap().required.as_ref().unwrap().node_selector_terms;
        let req = terms[0].match_fields.as_ref().unwrap()[0].clone();
        assert_eq!(req.key, "metadata.name");
        assert_eq!(req.operator, "In");
        assert_eq!(req.values.as_ref().unwrap(), &vec!["node1".to_string()]);
    }

    #[test]
    fn build_pv_volume_attributes_from_annotation() {
        // 有注解：透传进 volume_attributes（kubelet 将作为 volume_context 传给 node RPC）
        let mut pvc = fixture_pvc("u5", "overlayfs.csi.k8s.io", Some("node1"), "Pending");
        pvc.metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(MAX_AGE_ANNOTATION.to_string(), "7776000".to_string());
        let pv = build_pv(&pvc, "node1", "overlayfs.csi.k8s.io").unwrap();
        let attrs = pv.spec.as_ref().unwrap().csi.as_ref().unwrap()
            .volume_attributes
            .as_ref()
            .expect("合法注解必须透传进 volume_attributes");
        assert_eq!(
            attrs.get(VOLUME_ATTR_MAX_AGE).map(String::as_str),
            Some("7776000")
        );

        // 无注解：不写 volume_attributes（固化时回退全局 TTL）
        let pvc = fixture_pvc("u6", "overlayfs.csi.k8s.io", Some("node1"), "Pending");
        let pv = build_pv(&pvc, "node1", "overlayfs.csi.k8s.io").unwrap();
        assert!(
            pv.spec.as_ref().unwrap().csi.as_ref().unwrap().volume_attributes.is_none(),
            "无注解不得写 volume_attributes"
        );

        // 非法注解：不写（回退全局），provision 不失败
        let mut pvc = fixture_pvc("u7", "overlayfs.csi.k8s.io", Some("node1"), "Pending");
        pvc.metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(MAX_AGE_ANNOTATION.to_string(), "not-a-number".to_string());
        let pv = build_pv(&pvc, "node1", "overlayfs.csi.k8s.io").unwrap();
        assert!(
            pv.spec.as_ref().unwrap().csi.as_ref().unwrap().volume_attributes.is_none(),
            "非法注解不得写 volume_attributes"
        );
    }
}
