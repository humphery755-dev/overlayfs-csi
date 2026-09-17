use std::collections::BTreeMap;

use anyhow::Context;
use k8s_openapi::api::core::v1::{
    CSIPersistentVolumeSource, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    ObjectReference, PersistentVolume, PersistentVolumeClaim, PersistentVolumeSpec,
    VolumeNodeAffinity,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

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
        assert_eq!(req.values.as_ref().unwrap(), &vec!["node1".to_string()]);
    }
}
