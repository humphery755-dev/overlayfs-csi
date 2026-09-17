//! VM-like pod 注入 webhook。
//!
//! Pod 打上 [`MAX_AGE_ANNOTATION`]（复用 PVC 的 TTL 注解名）即开启 VM 模式：
//! webhook 注入 privileged wrapper + hostPath 持久卷，容器启动时对白名单系统目录
//! 逐个做「bind 固定 lower + 持久 upperdir」的 overlay 自叠加——写入穿透落盘，
//! pod 删除重建后修改原样回来（写时持久，零同步窗口）。
//!
//! 设计与错误处理矩阵见
//! docs/superpowers/specs/2026-09-17-pod-annotation-vm-persistence-design.md。

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use base64::Engine;
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, StatusCode};
use k8s_openapi::api::core::v1::Pod;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::base::Store;
use crate::controller::MAX_AGE_ANNOTATION;

pub const VM_VOLUME_NAME: &str = "ofcsi-vm";
pub const VM_BIN_VOLUME_NAME: &str = "ofcsi-vm-bin";
/// hostPath 快照卷在容器内的挂载点（wrapper 由此读写持久 upper/work/bind）
pub const VM_DIR: &str = "/.ofcsi-vm";
/// wrapper 脚本 ConfigMap 的只读挂载点
pub const VM_BIN_DIR: &str = "/.ofcsi-vm-bin";
pub const VM_INIT: &str = "/.ofcsi-vm-bin/vm-init.sh";
/// vm-init.sh 所在 ConfigMap 名；与 chart/templates/webhook.yaml 保持一致（驱动与
/// ConfigMap 同 namespace 单实例部署）
pub const VM_CONFIGMAP: &str = "overlayfs-csi-vm-init";
/// 注入成功标记：watcher 兜底检测依据——带 VM 注解却没有本标记 = webhook 失效期
/// 建成的 pod（静默不持久 = 数据丢失风险），删除重建。
pub const VM_INJECTED_ANNOTATION: &str = "overlayfs.csi.k8s.io/vm-injected";

pub const META_FILE: &str = "meta.json";

/// meta.json：GC 唯一依据。webhook 注入时写 ttl_s（取自 pod 注解）；GC 循环对活跃
/// VM 刷新 last_seen；目录失去活跃 pod 后从 last_seen 起 TTL 过期才删除。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct VmMeta {
    pub ttl_s: i64,
    pub last_seen: i64,
}

/// 注解语义：None = 非 VM pod（放行）；Some(Err) = 注解存在但非法（拒绝创建）。
///
/// 与 PVC 注解的 warn+回退刻意不同：PVC 回退只损失 TTL 精度，而这里静默放行等于
/// 「用户以为持久了、实际没有」——数据丢失，必须 fail fast。
pub fn vm_ttl(pod: &Pod) -> Option<Result<i64, String>> {
    let raw = pod
        .metadata
        .annotations
        .as_ref()?
        .get(MAX_AGE_ANNOTATION)?;
    Some(match raw.parse::<i64>() {
        Ok(v) if v > 0 => Ok(v),
        Ok(v) => Err(format!(
            "annotation {MAX_AGE_ANNOTATION} must be a positive integer, got {v}"
        )),
        Err(e) => Err(format!(
            "annotation {MAX_AGE_ANNOTATION} must be a positive integer, got {raw:?} ({e})"
        )),
    })
}

/// 是否已完成注入：spec.volumes 中存在我们的 hostPath 卷。
/// webhook 不可用（failurePolicy: Ignore）时 pod 带注解建成但没有该卷——watcher 据此兜底删除。
pub fn is_vm_injected(pod: &Pod) -> bool {
    pod.spec.as_ref().is_some_and(|s| {
        s.volumes
            .as_ref()
            .is_some_and(|vs| vs.iter().any(|v| v.name == VM_VOLUME_NAME))
    })
}

/// 注入前置校验，失败即拒绝创建（消息直接给 kubectl）。
fn validate(pod: &Pod) -> Result<(), String> {
    let spec = pod.spec.as_ref().ok_or("pod has no spec")?;
    // mount(2) 需要 root：显式拒绝 runAsNonRoot/非 0 uid，防止注入后容器反复崩溃
    if let Some(sc) = &spec.security_context {
        if sc.run_as_non_root == Some(true) {
            return Err(
                "pod.securityContext.runAsNonRoot=true conflicts with VM mode (mount requires root)"
                    .to_string(),
            );
        }
    }
    for c in &spec.containers {
        if c.command.as_ref().is_none_or(|cmd| cmd.is_empty()) {
            return Err(format!(
                "container {:?}: VM mode requires an explicit command (the injected wrapper re-execs it)",
                c.name
            ));
        }
        if let Some(sc) = &c.security_context {
            if sc.run_as_non_root == Some(true) {
                return Err(format!(
                    "container {:?}: securityContext.runAsNonRoot=true conflicts with VM mode (mount requires root)",
                    c.name
                ));
            }
        }
    }
    Ok(())
}

pub fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

pub fn vm_expired(ttl_s: i64, last_seen: i64, now: i64) -> bool {
    now.saturating_sub(last_seen) >= ttl_s
}

fn write_meta(dir: &Path, meta: &VmMeta) -> anyhow::Result<()> {
    let data = serde_json::to_string(meta)?;
    std::fs::write(dir.join(META_FILE), data)?;
    Ok(())
}

/// 读 meta；缺失/损坏返回 Ok(None)——GC 侧对 None 一律保守跳过（不误删数据）。
pub fn read_meta(dir: &Path) -> anyhow::Result<Option<VmMeta>> {
    let data = match std::fs::read_to_string(dir.join(META_FILE)) {
        Ok(d) => d,
        // 首次启动/无快照是预期状态
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(serde_json::from_str(&data).ok())
}

/// 只刷新 last_seen、保留 ttl_s：GC 循环对活跃 VM 调用。
pub fn touch_meta(dir: &Path, now: i64) -> anyhow::Result<()> {
    if let Some(mut meta) = read_meta(dir)? {
        meta.last_seen = now;
        write_meta(dir, &meta)?;
    }
    Ok(())
}

/// 注入前建快照目录树并写 meta。hostPath `DirectoryOrCreate` 会自建目录，这里提前建
/// 是为了让 meta 在 pod 首次 Running 前就存在（pod 刚建即删也不至于丢 TTL 信息）。
pub fn ensure_vm_dir(
    store: &Store,
    namespace: &str,
    pod_name: &str,
    ttl_s: i64,
) -> anyhow::Result<PathBuf> {
    let dir = store.vm_dir(namespace, pod_name);
    for sub in ["up", "work", "bind"] {
        std::fs::create_dir_all(dir.join(sub))?;
    }
    write_meta(&dir, &VmMeta { ttl_s, last_seen: now_unix() })?;
    Ok(dir)
}

fn container_patches(
    spec: &k8s_openapi::api::core::v1::PodSpec,
    index: usize,
) -> Vec<Value> {
    let c = &spec.containers[index];
    let base = format!("/spec/containers/{index}");
    let mut patches = Vec::new();

    // command：wrapper 前缀 + 原命令（validate 已保证原 command 非空）
    let mut wrapped = vec!["sh".to_string(), VM_INIT.to_string()];
    wrapped.extend(c.command.clone().unwrap_or_default());
    patches.push(json!({
        "op": "add", "path": format!("{base}/command"), "value": wrapped,
    }));

    // privileged：mount 的内核要求（用户已在设计阶段确认接受）
    if c.security_context.is_some() {
        patches.push(json!({
            "op": "add", "path": format!("{base}/securityContext/privileged"), "value": true,
        }));
    } else {
        patches.push(json!({
            "op": "add", "path": format!("{base}/securityContext"),
            "value": {"privileged": true},
        }));
    }

    let mounts = json!([
        {"name": VM_VOLUME_NAME, "mountPath": VM_DIR},
        {"name": VM_BIN_VOLUME_NAME, "mountPath": VM_BIN_DIR, "readOnly": true},
    ]);
    if c.volume_mounts.is_some() {
        for m in mounts.as_array().expect("mounts is an array") {
            patches.push(json!({"op": "add", "path": format!("{base}/volumeMounts/-"), "value": m}));
        }
    } else {
        patches.push(json!({
            "op": "add", "path": format!("{base}/volumeMounts"), "value": mounts,
        }));
    }
    patches
}

/// 构造注入 JSON Patch（RFC 6902）。校验失败 → Err（上层转为拒绝响应）。
pub fn injection_patches(pod: &Pod, namespace: &str, store: &Store) -> Result<Vec<Value>, String> {
    validate(pod)?;
    let name = pod
        .metadata
        .name
        .clone()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| {
            "VM mode requires an explicit metadata.name (snapshot dir is keyed by pod name)"
                .to_string()
        })?;
    let spec = pod.spec.as_ref().ok_or("pod has no spec")?;

    let snapshot_dir = store.vm_dir(namespace, &name);
    let volumes = json!([
        {"name": VM_VOLUME_NAME, "hostPath": {"path": snapshot_dir.display().to_string(), "type": "DirectoryOrCreate"}},
        {"name": VM_BIN_VOLUME_NAME, "configMap": {"name": VM_CONFIGMAP}},
    ]);

    let mut patches = Vec::new();
    if spec.volumes.is_some() {
        for v in volumes.as_array().expect("volumes is an array") {
            patches.push(json!({"op": "add", "path": "/spec/volumes/-", "value": v}));
        }
    } else {
        patches.push(json!({"op": "add", "path": "/spec/volumes", "value": volumes}));
    }

    for i in 0..spec.containers.len() {
        patches.extend(container_patches(spec, i));
    }

    // 注入标记：watcher 兜底判定 + 用户可见的自描述（JSON Pointer 中 / 转义为 ~1）
    let annotations_key = VM_INJECTED_ANNOTATION.replace('/', "~1");
    match &pod.metadata.annotations {
        Some(_) => patches.push(json!({
            "op": "add", "path": format!("/metadata/annotations/{annotations_key}"), "value": "true",
        })),
        None => patches.push(json!({
            "op": "add", "path": "/metadata/annotations",
            "value": {VM_INJECTED_ANNOTATION: "true"},
        })),
    }
    Ok(patches)
}

/// 处理 AdmissionReview 请求体，返回 AdmissionReview 响应体。
/// 系统错误（解析失败等）上抛 → HTTP 500；业务校验失败 → allowed=false + message。
pub fn handle_admission(body: &[u8], store: &Store) -> anyhow::Result<Value> {
    let v: Value = serde_json::from_slice(body).context("admission request is not valid JSON")?;
    let req = &v["request"];
    let uid = req["uid"].as_str().unwrap_or_default().to_string();
    let operation = req["operation"].as_str().unwrap_or("");
    // CREATE 阶段 object.metadata.namespace 可能缺省，apiserver 把目标 namespace 放在 request.namespace
    let namespace = req["namespace"].as_str().unwrap_or("default").to_string();

    let allowed_obj = if operation != "CREATE" {
        // 只在创建时注入；UPDATE/CONNECT/DELETE 一律放行
        json!({"allowed": true})
    } else {
        let pod: Pod =
            serde_json::from_value(req["object"].clone()).context("admission object is not a Pod")?;
        match vm_ttl(&pod) {
            None => json!({"allowed": true}),
            Some(Err(msg)) => json!({
                "allowed": false,
                "status": {"code": 400, "message": msg},
            }),
            Some(Ok(ttl)) => {
                let name = pod.metadata.name.clone().unwrap_or_default();
                if name.is_empty() {
                    json!({
                        "allowed": false,
                        "status": {"code": 400, "message":
                            "VM mode requires an explicit metadata.name (snapshot dir is keyed by pod name)"},
                    })
                } else {
                    match injection_patches(&pod, &namespace, store) {
                        Ok(patches) => {
                            // 目录/meta 在注入被 apiserver 接受前先落地：pod 若随后被
                            // 拒绝（配额等），残留目录由 GC 按 TTL 清理。
                            ensure_vm_dir(store, &namespace, &name, ttl)
                                .context("failed to create VM snapshot dir")?;
                            let patch = base64::engine::general_purpose::STANDARD
                                .encode(Value::Array(patches).to_string());
                            json!({
                                "allowed": true,
                                "patch": patch,
                                "patchType": "JSONPatch",
                            })
                        }
                        Err(msg) => json!({
                            "allowed": false,
                            "status": {"code": 400, "message": msg},
                        }),
                    }
                }
            }
        }
    };

    let mut response = json!({"uid": uid});
    if let (Value::Object(map), Value::Object(allowed)) = (&mut response, &allowed_obj) {
        for (k, val) in allowed {
            map.insert(k.clone(), val.clone());
        }
    }
    Ok(json!({
        "apiVersion": v["apiVersion"].as_str().unwrap_or("admission.k8s.io/v1"),
        "kind": "AdmissionReview",
        "response": response,
    }))
}

async fn mutate(
    req: Request<Body>,
    store: Arc<Store>,
) -> Result<Response<Body>, std::convert::Infallible> {
    let resp = match (req.method(), req.uri().path()) {
        (&Method::POST, "/mutate") => {
            let body = match hyper::body::to_bytes(req.into_body()).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("webhook read body failed: {e}");
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("read body failed"))
                        .expect("static response"));
                }
            };
            match handle_admission(&body, &store) {
                Ok(review) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(review.to_string()))
                    .expect("static response"),
                Err(e) => {
                    tracing::error!("webhook admission failed: {e:#}");
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(format!("{e:#}")))
                        .expect("static response")
                }
            }
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("static response"),
    };
    Ok(resp)
}

pub struct WebhookFlags {
    pub addr: SocketAddr,
    pub cert: PathBuf,
    pub key: PathBuf,
}

fn tls_acceptor(cert: &Path, key: &Path) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(cert)
            .with_context(|| format!("open webhook cert {}", cert.display()))?,
    ))?
    .into_iter()
    .map(tokio_rustls::rustls::Certificate)
    .collect();
    if certs.is_empty() {
        anyhow::bail!("webhook cert {} contains no certificates", cert.display());
    }
    let mut key_file =
        std::io::BufReader::new(std::fs::File::open(key).with_context(|| {
            format!("open webhook key {}", key.display())
        })?);
    // PKCS8 优先（helm genSelfSignedCert 输出），失败回退 RSA/PKCS1
    let key_der = rustls_pemfile::pkcs8_private_keys(&mut key_file)
        .or_else(|_| {
            use std::io::Seek;
            key_file.seek(std::io::SeekFrom::Start(0))?;
            rustls_pemfile::rsa_private_keys(&mut key_file)
        })?
        .into_iter()
        .next()
        .context("webhook key file contains no private key")?;
    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, tokio_rustls::rustls::PrivateKey(key_der))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// 阻塞式运行 webhook server；失败上抛（main 侧 fast-fail 交给 k8s 重启）。
pub async fn run(flags: WebhookFlags, store: Arc<Store>) -> anyhow::Result<()> {
    let acceptor = tls_acceptor(&flags.cert, &flags.key)?;
    let listener = tokio::net::TcpListener::bind(flags.addr).await?;
    tracing::info!(addr = %flags.addr, "VM webhook listening");
    loop {
        let (tcp, _peer) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let store = store.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(tcp).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("webhook tls handshake failed: {e}");
                    return;
                }
            };
            let _ = hyper::server::conn::Http::new()
                .serve_connection(tls, service_fn(move |req| mutate(req, store.clone())))
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture_pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "test", "namespace": "kube-system"},
            "spec": {"containers": [{"name": "c1", "image": "debian", "command": ["sleep", "infinity"]}]},
        }))
        .unwrap()
    }

    fn with_annotation(mut pod: Pod, key: &str, value: &str) -> Pod {
        pod.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .insert(key.to_string(), value.to_string());
        pod
    }

    #[test]
    fn vm_ttl_parses_and_rejects() {
        let pod = fixture_pod();
        assert!(vm_ttl(&pod).is_none(), "无注解 = 非 VM pod");

        let pod = with_annotation(pod, MAX_AGE_ANNOTATION, "7776000");
        assert_eq!(vm_ttl(&pod), Some(Ok(7776000)));

        for bad in ["0", "-5", "not-a-number"] {
            let pod = with_annotation(fixture_pod(), MAX_AGE_ANNOTATION, bad);
            assert!(vm_ttl(&pod).unwrap().is_err(), "非法值 {bad} 必须被拒绝");
        }
    }

    #[test]
    fn validate_rejects_missing_command_and_non_root() {
        let mut pod = fixture_pod();
        pod.spec.as_mut().unwrap().containers[0].command = None;
        let err = validate(&pod).unwrap_err();
        assert!(err.contains("explicit command"), "缺 command 必须拒绝: {err}");

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "test"},
            "spec": {
                "securityContext": {"runAsNonRoot": true},
                "containers": [{"name": "c1", "image": "debian", "command": ["sleep"]}],
            },
        }))
        .unwrap();
        let err = validate(&pod).unwrap_err();
        assert!(err.contains("runAsNonRoot"), "runAsNonRoot 必须拒绝: {err}");

        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "test"},
            "spec": {"containers": [
                {"name": "c1", "image": "debian", "command": ["sleep"]},
                {"name": "c2", "image": "debian"},
            ]},
        }))
        .unwrap();
        assert!(validate(&pod).is_err(), "任一容器缺 command 都拒绝");

        assert!(validate(&fixture_pod()).is_ok());
    }

    #[test]
    fn injection_patches_cover_volumes_containers_and_marker() {
        let store = Store::new("/var/lib/overlayfs-csi");
        let pod = with_annotation(fixture_pod(), MAX_AGE_ANNOTATION, "600");
        let patches = injection_patches(&pod, "kube-system", &store).unwrap();
        let text = serde_json::to_string(&patches).unwrap();

        // pod 级：两个 volume + hostPath 指向 ns/name 快照目录
        assert!(text.contains(r#""path":"/spec/volumes""#));
        assert!(text.contains(VM_VOLUME_NAME));
        assert!(text.contains("/var/lib/overlayfs-csi/vm/kube-system/test"));
        assert!(text.contains(VM_CONFIGMAP));

        // 容器级：command 包 wrapper、privileged、双挂载
        assert!(text.contains(VM_INIT));
        assert!(text.contains(r#""privileged":true"#));
        assert!(text.contains(VM_DIR));
        assert!(text.contains(VM_BIN_DIR));
        let wrapped: Vec<&Value> = patches
            .iter()
            .filter(|p| p["path"].as_str() == Some("/spec/containers/0/command"))
            .collect();
        assert_eq!(wrapped.len(), 1);
        assert_eq!(
            wrapped[0]["value"],
            json!(["sh", VM_INIT, "sleep", "infinity"]),
            "wrapper 必须转发原命令"
        );

        // 标记注解：JSON Pointer 中 / 转义为 ~1
        assert!(text.contains("/metadata/annotations/overlayfs.csi.k8s.io~1vm-injected"));
    }

    #[test]
    fn injection_patches_append_when_volumes_exist() {
        let store = Store::new("/var/lib/overlayfs-csi");
        let mut pod = fixture_pod();
        let spec = pod.spec.as_mut().unwrap();
        spec.volumes = Some(vec![Default::default()]);
        spec.containers[0].security_context = Some(Default::default());
        let patches = injection_patches(&pod, "ns", &store).unwrap();
        let paths: Vec<&str> = patches.iter().filter_map(|p| p["path"].as_str()).collect();
        assert!(
            paths.iter().all(|p| *p != "/spec/volumes"),
            "已有 volumes 时必须用 /-/ 追加，不得覆盖整组"
        );
        assert!(paths.contains(&"/spec/volumes/-"));
        assert!(
            paths.contains(&"/spec/containers/0/securityContext/privileged"),
            "已有 securityContext 时只加 privileged 字段"
        );
    }

    #[test]
    fn admission_flow_allow_inject_and_reject() {
        let store = Store::new(std::env::temp_dir().join(format!("ofcsi-wh-{}", std::process::id())));

        // 非 VM pod：放行且无 patch
        let pod = fixture_pod();
        let body = serde_json::to_vec(&json!({
            "apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
            "request": {"uid": "u1", "operation": "CREATE", "namespace": "kube-system",
                         "object": serde_json::to_value(&pod).unwrap()},
        }))
        .unwrap();
        let resp = handle_admission(&body, &store).unwrap();
        assert_eq!(resp["response"]["allowed"], json!(true));
        assert!(resp["response"]["patch"].is_null());

        // VM pod：allowed + base64(JSONPatch)，且 meta 落地
        let pod = with_annotation(fixture_pod(), MAX_AGE_ANNOTATION, "600");
        let body = serde_json::to_vec(&json!({
            "apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
            "request": {"uid": "u2", "operation": "CREATE", "namespace": "kube-system",
                         "object": serde_json::to_value(&pod).unwrap()},
        }))
        .unwrap();
        let resp = handle_admission(&body, &store).unwrap();
        assert_eq!(resp["response"]["allowed"], json!(true));
        assert_eq!(resp["response"]["patchType"], json!("JSONPatch"));
        let meta = read_meta(&store.vm_dir("kube-system", "test"))
            .unwrap()
            .expect("meta 必须在注入时落地");
        assert_eq!(meta.ttl_s, 600);

        // 非法注解：拒绝且带消息
        let pod = with_annotation(fixture_pod(), MAX_AGE_ANNOTATION, "nope");
        let body = serde_json::to_vec(&json!({
            "apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
            "request": {"uid": "u3", "operation": "CREATE", "namespace": "kube-system",
                         "object": serde_json::to_value(&pod).unwrap()},
        }))
        .unwrap();
        let resp = handle_admission(&body, &store).unwrap();
        assert_eq!(resp["response"]["allowed"], json!(false));
        assert!(resp["response"]["status"]["message"]
            .as_str()
            .unwrap()
            .contains("positive integer"));

        // UPDATE：一律放行（注入只在 CREATE）
        let body = serde_json::to_vec(&json!({
            "apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
            "request": {"uid": "u4", "operation": "UPDATE", "namespace": "kube-system",
                         "object": serde_json::to_value(&pod).unwrap()},
        }))
        .unwrap();
        let resp = handle_admission(&body, &store).unwrap();
        assert_eq!(resp["response"]["allowed"], json!(true));

        std::fs::remove_dir_all(&store.root).ok();
    }

    #[test]
    fn meta_roundtrip_and_expiry() {
        let dir = std::env::temp_dir().join(format!("ofcsi-meta-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(read_meta(&dir).unwrap().is_none(), "冷目录无 meta = None（首次启动）");

        write_meta(&dir, &VmMeta { ttl_s: 100, last_seen: 1000 }).unwrap();
        assert_eq!(
            read_meta(&dir).unwrap(),
            Some(VmMeta { ttl_s: 100, last_seen: 1000 })
        );

        touch_meta(&dir, 2000).unwrap();
        assert_eq!(
            read_meta(&dir).unwrap(),
            Some(VmMeta { ttl_s: 100, last_seen: 2000 })
        );

        assert!(!vm_expired(100, 2000, 2099), "TTL 内不得过期");
        assert!(vm_expired(100, 2000, 2100), "TTL 届满即过期");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn is_vm_injected_detects_volume() {
        let mut pod = fixture_pod();
        assert!(!is_vm_injected(&pod));
        pod.spec.as_mut().unwrap().volumes = Some(vec![serde_json::from_value(json!({
            "name": VM_VOLUME_NAME, "hostPath": {"path": "/x", "type": "DirectoryOrCreate"},
        }))
        .unwrap()]);
        assert!(is_vm_injected(&pod));
    }

    #[test]
    fn ensure_vm_dir_creates_tree() {
        let store = Store::new(std::env::temp_dir().join(format!("ofcsi-vmdir-{}", std::process::id())));
        let dir = ensure_vm_dir(&store, "ns1", "p1", 60).unwrap();
        for sub in ["up", "work", "bind"] {
            assert!(dir.join(sub).is_dir(), "{sub} 必须存在");
        }
        assert_eq!(
            read_meta(&dir).unwrap().map(|m| m.ttl_s),
            Some(60)
        );
        std::fs::remove_dir_all(&store.root).ok();
    }

    /// 非 root 环境自动 skip（集成测试需要真实 mount），与 base::tests 同款。
    fn require_root() -> bool {
        let ok = std::process::Command::new("id")
            .arg("-u")
            .output()
            .map(|o| o.stdout == b"0\n")
            .unwrap_or(false);
        if !ok {
            eprintln!("skipping: requires root");
        }
        ok
    }

    /// 复刻 vm-init.sh 的关键序列（bind 固定 lower → 自叠加 overlay），在真内核上
    /// 验证方案的两大技术风险点：overlay 的 lowerdir 位于另一 overlay 视图（bind）上
    /// 可用；写入穿透 upper，删除重建（重挂）后新增/修改/whiteout 全部回粘。
    #[test]
    fn overlay_self_stack_persists_across_remount() {
        if !require_root() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ofcsi-vm-stack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        for sub in ["lower", "up", "work", "bind", "mnt"] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        // 「镜像内容」
        std::fs::write(dir.join("lower/a.txt"), "orig-a").unwrap();
        std::fs::write(dir.join("lower/b.txt"), "orig-b").unwrap();

        let mount = || {
            let out = duct::cmd!(
                "sh",
                "-c",
                "mount --bind lower bind && \
                 mount -t overlay overlay -o lowerdir=bind,upperdir=up,workdir=work mnt"
            )
            .dir(&dir)
            .run()
            .expect("mount failed");
            assert!(
                out.status.success(),
                "mount failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        let umount = || {
            let out = duct::cmd!("sh", "-c", "umount mnt && umount bind")
                .dir(&dir)
                .run()
                .expect("umount failed");
            assert!(
                out.status.success(),
                "umount failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };

        // 第一次「pod 启动」：修改（新增/删除/覆写）写穿透进 up/
        mount();
        std::fs::write(dir.join("mnt/c.txt"), "new-c").unwrap();
        std::fs::remove_file(dir.join("mnt/b.txt")).unwrap();
        std::fs::write(dir.join("mnt/a.txt"), "modified-a").unwrap();
        umount();

        // 第二次「pod 删除重建」：重挂同一批 upper，修改必须原样回来
        mount();
        assert_eq!(
            std::fs::read_to_string(dir.join("mnt/a.txt")).unwrap(),
            "modified-a",
            "覆写必须跨重建保留"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("mnt/c.txt")).unwrap(),
            "new-c",
            "新增文件必须跨重建保留"
        );
        assert!(!dir.join("mnt/b.txt").exists(), "whiteout 必须跨重建生效");
        umount();

        std::fs::remove_dir_all(&dir).ok();
    }
}
