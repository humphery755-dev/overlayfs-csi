//! VM-like pod 注入 webhook。
//!
//! Pod 打上 [`MAX_AGE_ANNOTATION`]（复用 PVC 的 TTL 注解名）即开启 VM 模式：
//! webhook 注入 privileged wrapper + hostPath 持久卷，容器启动时对白名单系统目录
//! 逐个做「bind 固定 lower + 持久 upperdir」的 overlay 自叠加——写入穿透落盘，
//! pod 删除重建后修改原样回来（写时持久，零同步窗口）。
//!
//! 设计与错误处理矩阵见
//! docs/superpowers/specs/2026-09-17-pod-annotation-vm-persistence-design.md。

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use base64::Engine;
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, StatusCode};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams};
use kube::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::base::{parse_max_age_s, Store, MAX_AGE_ANNOTATION};

pub const VM_VOLUME_NAME: &str = "ofcsi-vm";
/// hostPath 快照卷在容器内的挂载点；wrapper 脚本也随卷分发（写在卷根）。
/// 脚本内 BASE 从自身位置推导（`dirname $0`），此处为该路径的唯一硬编码点。
pub const VM_DIR: &str = "/.ofcsi-vm";
/// wrapper 脚本文件名与容器内完整路径：由 [`ensure_vm_dir`] 写进快照卷根，
/// 随 hostPath 卷挂载进容器。不走 ConfigMap——ConfigMap 是 namespace 域资源，
/// 会让其他 namespace 的 VM pod 卡在 FailedMount（「任意 namespace 打注解
/// 即用」是 VM 模式的硬承诺）。
pub const VM_INIT_FILE: &str = "vm-init.sh";
pub const VM_INIT: &str = "/.ofcsi-vm/vm-init.sh";

pub const META_FILE: &str = "meta.json";

/// vm-init.sh 全文（`src/vm-init.sh`，编译期嵌入）：随快照卷分发
/// （[`ensure_vm_dir`] 落盘），每次注入都重写——驱动升级后新注入的 pod
/// 自动用上新脚本。
///
/// 用户挂载语义（与普通 pod 一致）：白名单目录下已存在的挂载点一律
/// 「排除」在 overlay 之外——bind 到容器私有中转位，overlay 挂好后按
/// 浅→深 bind 回原路径。用户挂载的数据走用户自己的卷，不进快照、不受
/// TTL 管辖；只有未挂载的部分随 overlay 持久。
pub const VM_INIT_SCRIPT: &str = include_str!("vm-init.sh");

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
/// 「用户以为持久了、实际没有」——数据丢失，必须 fail fast。解析规则共享自
/// [`parse_max_age_s`]，只有失败处置是本域的。
pub fn vm_ttl(pod: &Pod) -> Option<Result<i64, String>> {
    let raw = pod
        .metadata
        .annotations
        .as_ref()?
        .get(MAX_AGE_ANNOTATION)?;
    Some(parse_max_age_s(raw))
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
/// last_seen 只需支持「pod 删除后从冻结点起算 TTL」，30s 周期的精确刷新毫无
/// 信息量——距上次不足 min(ttl/10, 60s) 时跳过写盘（读一次换掉一次写+脏页）。
pub fn touch_meta(dir: &Path, now: i64) -> anyhow::Result<()> {
    if let Some(mut meta) = read_meta(dir)? {
        if now - meta.last_seen < (meta.ttl_s / 10).min(60) {
            return Ok(());
        }
        meta.last_seen = now;
        write_meta(dir, &meta)?;
    }
    Ok(())
}

/// 注入前建快照目录并写 meta + wrapper 脚本。up/work/bind 子目录由 vm-init.sh
/// 按需创建（目录布局的唯一真源在脚本），这里只保证根与 meta 存在——pod 刚建
/// 即删也不至于丢 TTL 信息。
pub fn ensure_vm_dir(
    store: &Store,
    namespace: &str,
    pod_name: &str,
    ttl_s: i64,
) -> anyhow::Result<PathBuf> {
    let dir = store.vm_dir(namespace, pod_name);
    std::fs::create_dir_all(&dir)?;
    // wrapper 随卷分发：每次注入都重写，驱动升级后新 pod 自动用上新脚本；
    // 内容一致则跳过写盘（稳态下零写入）。
    let script_path = dir.join(VM_INIT_FILE);
    let needs_write = std::fs::read_to_string(&script_path)
        .map(|existing| existing != VM_INIT_SCRIPT)
        .unwrap_or(true);
    if needs_write {
        std::fs::write(&script_path, VM_INIT_SCRIPT)?;
    }
    write_meta(&dir, &VmMeta { ttl_s, last_seen: now_unix() })?;
    Ok(dir)
}

/// VM 域的周期 GC 与未注入兜底（由 controller 的 janitor 每 30s 调度——
/// 本模块是 VM 域逻辑的唯一 owner，controller 只负责定时）。
/// 活跃 pod 的目录只刷 last_seen（节流见 [`touch_meta`]）；失去活跃 pod 的
/// 目录按 meta 的 TTL 过期删除；meta 缺失/损坏一律保守跳过（宁可少删，
/// 不可误删用户数据）。
pub async fn vm_gc_once(kube: &Client, store: &Store) -> anyhow::Result<()> {
    let pods: Api<Pod> = Api::all(kube.clone());
    let list = pods.list(&ListParams::default()).await?;
    // 借用 list.items 的键：集合生命周期不超过 list，零分配
    let mut active: HashSet<(&str, &str)> = HashSet::new();
    for pod in &list.items {
        // guard 先行：绝大多数 pod 无注解，省掉 name/ns 的取用
        if vm_ttl(pod).is_none() {
            continue;
        }
        let (Some(ns), Some(name)) = (
            pod.metadata.namespace.as_deref(),
            pod.metadata.name.as_deref(),
        ) else {
            continue;
        };
        if !is_vm_injected(pod) {
            // webhook 失效期（failurePolicy: Ignore）建成的 pod 静默失去持久化——
            // 放行等于数据丢失，删除强制重走注入（fail fast）。
            tracing::warn!(ns, name, "VM pod was not injected (webhook down?); deleting so it is recreated with injection");
            let api: Api<Pod> = Api::namespaced(kube.clone(), ns);
            api.delete(name, &DeleteParams::default()).await?;
            continue;
        }
        active.insert((ns, name));
    }

    let now = now_unix();
    for (ns, name) in store.list_vm_dirs()? {
        let dir = store.vm_dir(&ns, &name);
        if active.contains(&(ns.as_str(), name.as_str())) {
            touch_meta(&dir, now)?;
            continue;
        }
        let Some(meta) = read_meta(&dir)? else {
            tracing::warn!(dir = %dir.display(), "VM dir missing/corrupt meta.json; skipping this round");
            continue;
        };
        if vm_expired(meta.ttl_s, meta.last_seen, now) {
            tracing::warn!(dir = %dir.display(), "removing expired VM snapshot dir");
            std::fs::remove_dir_all(&dir)?;
        }
    }
    Ok(())
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

    let m = json!({"name": VM_VOLUME_NAME, "mountPath": VM_DIR});
    if c.volume_mounts.is_some() {
        patches.push(json!({"op": "add", "path": format!("{base}/volumeMounts/-"), "value": m}));
    } else {
        patches.push(json!({
            "op": "add", "path": format!("{base}/volumeMounts"), "value": [m],
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
    let volume = json!(
        {"name": VM_VOLUME_NAME, "hostPath": {"path": snapshot_dir.display().to_string(), "type": "DirectoryOrCreate"}}
    );

    let mut patches = Vec::new();
    if spec.volumes.is_some() {
        patches.push(json!({"op": "add", "path": "/spec/volumes/-", "value": volume}));
    } else {
        patches.push(json!({"op": "add", "path": "/spec/volumes", "value": [volume]}));
    }

    for i in 0..spec.containers.len() {
        patches.extend(container_patches(spec, i));
    }
    Ok(patches)
}

fn deny(msg: impl Into<String>) -> Value {
    json!({
        "allowed": false,
        "status": {"code": 400, "message": msg.into()},
    })
}

/// CREATE 请求的业务处置：Ok = 注入响应字段（allowed/patch/patchType），
/// Err = 拒绝消息（由调用方转 deny 响应）。全程早返回，无嵌套。
fn handle_create(object: &Value, namespace: &str, store: &Store) -> Result<Value, String> {
    let pod: Pod =
        Pod::deserialize(object).map_err(|e| format!("admission object is not a Pod: {e}"))?;
    let ttl = match vm_ttl(&pod) {
        Some(Ok(ttl)) => ttl,
        Some(Err(msg)) => return Err(msg),
        // 外层已按注解放行；双保险：无注解的 Pod 到这里直接放行
        None => return Ok(json!({"allowed": true})),
    };
    // 显式 metadata.name 等校验由 injection_patches 承载（Err → deny）
    let patches = injection_patches(&pod, namespace, store)?;
    // 目录/meta 在注入被 apiserver 接受前先落地：pod 若随后被拒绝（配额等），
    // 残留目录由 GC 按 TTL 清理。
    let name = pod.metadata.name.clone().unwrap_or_default();
    ensure_vm_dir(store, namespace, &name, ttl)
        .map_err(|e| format!("failed to create VM snapshot dir: {e}"))?;
    let patch =
        base64::engine::general_purpose::STANDARD.encode(Value::Array(patches).to_string());
    Ok(json!({
        "allowed": true,
        "patch": patch,
        "patchType": "JSONPatch",
    }))
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

    // 本 webhook 匹配全集群 pod CREATE：先在裸 Value 上廉价判注解，非 VM pod
    // 直接放行——不做完整 pod 的深拷贝与 typed 反序列化。
    let allowed_obj = if operation != "CREATE"
        || req["object"]["metadata"]["annotations"]
            .get(MAX_AGE_ANNOTATION)
            .is_none()
    {
        // 只在创建时注入；UPDATE/CONNECT/DELETE 一律放行
        json!({"allowed": true})
    } else {
        handle_create(&req["object"], &namespace, store).unwrap_or_else(deny)
    };

    let mut response = serde_json::Map::new();
    response.insert("uid".to_string(), Value::String(uid));
    if let Value::Object(fields) = allowed_obj {
        for (k, val) in fields {
            response.insert(k, val);
        }
    }
    Ok(json!({
        "apiVersion": v["apiVersion"].as_str().unwrap_or("admission.k8s.io/v1"),
        "kind": "AdmissionReview",
        "response": Value::Object(response),
    }))
}

fn http(status: StatusCode, body: impl Into<Body>) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(body.into())
        .expect("static response")
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
                    return Ok(http(StatusCode::BAD_REQUEST, "read body failed"));
                }
            };
            match handle_admission(&body, &store) {
                // JSON 响应必须带 content-type（apiserver 解析响应体的前提）
                Ok(review) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from(review.to_string()))
                    .expect("static response"),
                Err(e) => {
                    tracing::error!("webhook admission failed: {e:#}");
                    http(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
                }
            }
        }
        _ => http(StatusCode::NOT_FOUND, ""),
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
    // 先 PKCS1（helm genSelfSignedCert 输出 "RSA PRIVATE KEY"），空/异型再试
    // PKCS8——注意 rustls-pemfile 1.x 对不认识的块返回 Ok(空) 而非 Err，必须以
    // 「结果非空」为判据回退，不能依赖 or_else 捕获 Err。
    let key_der = rustls_pemfile::rsa_private_keys(&mut key_file)
        .ok()
        .filter(|keys| !keys.is_empty())
        .or_else(|| {
            use std::io::Seek;
            key_file.seek(std::io::SeekFrom::Start(0)).ok()?;
            rustls_pemfile::pkcs8_private_keys(&mut key_file).ok()
        })
        .and_then(|keys| keys.into_iter().next())
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

        // pod 级：hostPath 卷指向 ns/name 快照目录（无 ConfigMap——脚本随卷分发）
        assert!(text.contains(r#""path":"/spec/volumes""#));
        assert!(text.contains(VM_VOLUME_NAME));
        assert!(text.contains("/var/lib/overlayfs-csi/vm/kube-system/test"));
        assert!(
            !text.contains("configMap"),
            "注入不得再引用 ConfigMap（namespace 域，跨 ns 必挂）"
        );

        // 容器级：command 包 wrapper、privileged、单卷挂载
        assert!(text.contains(VM_INIT));
        assert!(text.contains(r#""privileged":true"#));
        assert!(text.contains(VM_DIR));
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

        // 兜底检测唯一依据是 spec.volumes（is_vm_injected）；注入产物里
        // 不再有别的标记（历史上的 vm-injected 注解写后无人读，已删除）。
        assert!(
            !text.contains("vm-injected"),
            "注入不得再产出死标记注解"
        );
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
        // wrapper 随卷分发：脚本必须落盘且内容完整；up/work/bind 由脚本按需
        // 创建（布局唯一真源在 vm-init.sh），这里只保证根、脚本、meta。
        let script = std::fs::read_to_string(dir.join(VM_INIT_FILE)).unwrap();
        assert!(script.starts_with("#!/bin/sh"), "脚本必须有 shebang");
        assert!(script.contains("mount -t overlay"), "脚本必须包含 overlay 挂载");
        assert_eq!(
            read_meta(&dir).unwrap().map(|m| m.ttl_s),
            Some(60)
        );
        std::fs::remove_dir_all(&store.root).ok();
    }

    use crate::base::require_root;

    /// root 集成测试共用的 shell 执行助手（失败即 panic，带 stderr）。
    fn sh_in(dir: &Path, cmd: &str) {
        let out = duct::cmd!("sh", "-c", cmd).dir(dir).run().expect("sh failed");
        assert!(
            out.status.success(),
            "{cmd} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
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
            sh_in(
                &dir,
                "mount --bind lower bind && \
                 mount -t overlay overlay -o lowerdir=bind,upperdir=up,workdir=work mnt",
            );
        };
        let umount = || sh_in(&dir, "umount mnt && umount bind");

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

    /// 用户挂载排除（vm-init.sh 三阶段的内核行为验证）：白名单目录下的用户
    /// 挂载在 overlay 之上完好保留——写入直达用户卷（不进快照），而 overlay
    /// 的持久化照常工作；跨「pod 重建」（拆栈重挂）两边语义都保持。
    #[test]
    fn submounts_are_excluded_from_snapshot() {
        if !require_root() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ofcsi-vm-keep-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        for sub in [
            "lower/usr", "up/usr", "work/usr", "bind/usr", "mnt/usr", "real-data", "tmp-keep",
        ] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        std::fs::write(dir.join("lower/usr/app.txt"), "image").unwrap();
        std::fs::write(dir.join("real-data/hostfile"), "user-data").unwrap();

        let stack = || {
            // 模拟 kubelet 挂用户卷到白名单目录下，随后三阶段：排除 → overlay → bind 回
            sh_in(&dir, "mount --bind real-data mnt/usr/keepme");
            sh_in(&dir, "mount --bind mnt/usr/keepme tmp-keep");
            sh_in(&dir, "mount --bind lower bind && mount -t overlay overlay -o lowerdir=bind,upperdir=up,workdir=work mnt/usr");
            sh_in(&dir, "mount --bind tmp-keep mnt/usr/keepme");
        };
        let unstack = || {
            sh_in(&dir, "umount mnt/usr/keepme && umount mnt/usr && umount bind && umount mnt/usr/keepme");
        };

        // 第一次「pod 启动」
        stack();
        // 用户数据写进用户卷，不进快照
        std::fs::write(dir.join("mnt/usr/keepme/newfile"), "user").unwrap();
        assert_eq!(std::fs::read_to_string(dir.join("real-data/newfile")).unwrap(), "user");
        assert!(!dir.join("up/usr/keepme").exists(), "用户挂载不得泄漏进快照");
        // overlay 持久化照常工作
        std::fs::write(dir.join("mnt/usr/written.txt"), "persisted").unwrap();
        assert_eq!(std::fs::read_to_string(dir.join("up/usr/written.txt")).unwrap(), "persisted");
        unstack();

        // 第二次「pod 删除重建」：用户卷重新挂载 + 三阶段重跑
        stack();
        assert_eq!(
            std::fs::read_to_string(dir.join("mnt/usr/keepme/newfile")).unwrap(),
            "user",
            "用户卷数据跨重建保留（走用户卷，与普通 pod 一致）"
        );
        assert_eq!(std::fs::read_to_string(dir.join("mnt/usr/written.txt")).unwrap(), "persisted");
        assert_eq!(std::fs::read_to_string(dir.join("mnt/usr/app.txt")).unwrap(), "image");
        unstack();

        std::fs::remove_dir_all(&dir).ok();
    }

    /// mountPath 恰好是白名单目录自身（如 volumeMount 到 /var）时同样必须排除：
    /// 用户显式接管整个目录，overlay 挂上后被用户的挂载完全遮蔽（无害），
    /// 但绝不能反向遮蔽用户的数据。
    #[test]
    fn self_mount_is_excluded_from_snapshot() {
        if !require_root() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ofcsi-vm-self-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        for sub in [
            "lower/usr", "up/usr", "work/usr", "bind/usr", "mnt/usr", "real-data", "tmp-keep",
        ] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        std::fs::write(dir.join("lower/usr/app.txt"), "image").unwrap();
        std::fs::write(dir.join("real-data/userfile"), "user-data").unwrap();

        // kubelet 把用户卷挂到白名单目录自身（mnt/usr），随后三阶段排除
        sh_in(&dir, "mount --bind real-data mnt/usr");
        sh_in(&dir, "mount --bind mnt/usr tmp-keep");
        sh_in(&dir, "mount --bind lower bind && mount -t overlay overlay -o lowerdir=bind,upperdir=up,workdir=work mnt/usr");
        sh_in(&dir, "mount --bind tmp-keep mnt/usr");

        // 访问 /usr 看到的是用户卷；写入直达用户卷，不进快照
        assert_eq!(std::fs::read_to_string(dir.join("mnt/usr/userfile")).unwrap(), "user-data");
        std::fs::write(dir.join("mnt/usr/newfile"), "user").unwrap();
        assert_eq!(std::fs::read_to_string(dir.join("real-data/newfile")).unwrap(), "user");
        assert!(
            std::fs::read_dir(dir.join("up/usr")).unwrap().next().is_none(),
            "整目录挂载时 overlay 不得捕获任何写入"
        );

        sh_in(&dir, "umount mnt/usr && umount mnt/usr && umount bind && umount mnt/usr");
        std::fs::remove_dir_all(&dir).ok();
    }
}
