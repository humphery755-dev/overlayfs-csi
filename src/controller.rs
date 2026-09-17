use std::collections::{BTreeMap, HashSet};
use std::path::Path;
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

use crate::base::{is_base_referenced, overlay_lowerdirs, Store};

pub const PV_PREFIX: &str = "overlayfs-";
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
pub const MANAGED_BY_VALUE: &str = "overlayfs-csi";
pub const SELECTED_NODE_ANNOTATION: &str = "volume.kubernetes.io/selected-node";

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
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}

pub const GC_GRACE_S: u64 = 300;
const RECONCILE_INTERVAL_S: u64 = 30;

pub fn is_orphan(dir_name: &str, managed: &HashSet<String>, dir_age_s: u64) -> bool {
    !managed.contains(dir_name) && dir_age_s > GC_GRACE_S
}

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
        let pvcs: Api<PersistentVolumeClaim> = Api::all(self.kube.clone());
        let mut events = watcher(pvcs, watcher::Config::default()).boxed();
        let mut reconciler =
            tokio::time::interval(std::time::Duration::from_secs(RECONCILE_INTERVAL_S));
        let mut janitor =
            tokio::time::interval(std::time::Duration::from_secs(RECONCILE_INTERVAL_S));
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
                _ = janitor.tick() => {
                    if let Err(e) = self.cleanup_once().await {
                        tracing::error!("cleanup/GC failed: {e:#}");
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
        let wdir = self.store.work_dir(&name);
        if wdir.exists() {
            std::fs::remove_dir_all(&wdir)?;
        }
        Ok(())
    }

    pub async fn cleanup_once(&self) -> anyhow::Result<()> {
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
        // 0 个 overlay 意味着 mountinfo 格式漂移：引用检查将静默放行，
        // 过期 base 会被误删 —— 立即告警暴露。
        if overlay_lowerdirs(&mountinfo).is_empty() {
            tracing::warn!(
                "parsed 0 overlay mounts from mountinfo; base-reference checks will silently pass"
            );
        }
        // 1. base TTL 清理：被 overlay 引用的不删
        for base in self.store.list_bases()? {
            if base.valid(self.max_age_s) {
                continue;
            }
            if is_base_referenced(&mountinfo, &base.0) {
                tracing::debug!(base = %base.0.display(), "expired base still referenced, keeping");
                continue;
            }
            tracing::warn!(base = %base.0.display(), "removing expired base");
            std::fs::remove_dir_all(&base.0)?;
        }
        // 2. 孤儿 volumes/ GC
        let pvs: Api<PersistentVolume> = Api::all(self.kube.clone());
        let list = pvs
            .list(&ListParams::default().labels(&format!("{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}")))
            .await?;
        let managed: HashSet<String> = list
            .items
            .iter()
            .filter_map(|pv| Some(pv.spec.as_ref()?.csi.as_ref()?.volume_handle.clone()))
            .collect();
        self.gc_orphans(&self.store.volumes_dir(), &managed, &mountinfo)?;
        // 3. 孤儿 work/ GC：对应 volume 目录不存在即孤儿（挂载中的 staging 必然有 volume 目录）
        let work_root = self.store.root.join("work");
        if work_root.exists() {
            for entry in std::fs::read_dir(&work_root)?.filter_map(Result::ok) {
                let vid = entry.file_name().to_string_lossy().to_string();
                if self.store.volume_dir(&vid).exists() {
                    continue;
                }
                let age = dir_age_s(&entry.path())?;
                if is_orphan(&format!("work/{vid}"), &HashSet::new(), age) {
                    tracing::warn!(dir = %entry.path().display(), "removing orphaned work dir");
                    std::fs::remove_dir_all(entry.path())?;
                }
            }
        }
        Ok(())
    }

    fn gc_orphans(
        &self,
        dir: &Path,
        managed: &HashSet<String>,
        mountinfo: &str,
    ) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir)?.filter_map(Result::ok) {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if managed.contains(&name) {
                continue;
            }
            if is_base_referenced(mountinfo, &path) {
                continue;
            }
            let age = dir_age_s(&path)?;
            if !is_orphan(&name, managed, age) {
                continue;
            }
            tracing::warn!(dir = %path.display(), "removing orphaned volume directory");
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
        assert_eq!(
            spec.access_modes.as_ref().map(|v| v.as_slice()),
            Some(&["ReadWriteOnce".to_string()][..])
        );
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
    fn orphan_detection_logic() {
        let mut managed = std::collections::HashSet::new();
        managed.insert("overlayfs-u1".to_string());
        // 有对应 PV → 非孤儿；无 PV 但太新（grace 内）→ 非孤儿；无 PV 且超 grace → 孤儿
        assert!(!is_orphan("overlayfs-u1", &managed, 0));
        assert!(!is_orphan("overlayfs-u9", &managed, 10));
        assert!(is_orphan("overlayfs-u9", &managed, 10_000));
    }
}
