# overlayfs-csi 持久卷改造 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 overlayfs-csi 从「emptyDir + base TTL 缓存」改造为基于 PVC 的真正持久卷（数据落节点本地盘 overlay upper，pod 删除重建不丢），保留 overlay/base 增量缓存。

**Architecture:** 单 Rust 二进制（DaemonSet）自实现 provisioning：watch PVC（`WaitForFirstConsumer` 调度器打 `volume.kubernetes.io/selected-node` 注解）→ 本地建目录 + 创建带 nodeAffinity 的预绑定 PV；Node 侧标准两段式（stage=overlay/bind mount，publish=bind）；`.as_base` 在 unstage 时固化为新 base；删除与孤儿 GC 由 watcher/周期任务驱动。

**Tech Stack:** Rust 2021 / tokio / kube 0.87（runtime feature）/ k8s-openapi 0.20 (v1_23) / tonic 0.10 / duct / time / uuid（新增）

**Spec:** `docs/superpowers/specs/2026-09-17-persistent-pvc-overlayfs-csi-design.md`

## Global Constraints

- **性能零损失（硬约束）**：写路径仅节点本地盘 overlayfs（lower=base, upper=PVC 数据目录）；禁止任何网络存储或复制式 fallback。
- **错误直接上抛**（工程铁律 5/6/11）：禁止 `let _ =` 吞 Result、禁止 catch-all；仅两类豁免：`umount ... || true`（幂等 umount，沿用现有 `unchecked()` 惯例）与「已存在即成功」的幂等 API 调用（`AlreadyExists`/`NotFound`）。
- kube 0.87.2 API 风格：`Api::all::<T>(client)`；watcher 事件用 `kube::runtime::watcher::Event::{Apply,Delete,Restarted}`；API 错误幂等判定用 `kube::Error::Api(e)` 且 `e.reason.as_deref() == Some("AlreadyExists" | "NotFound")`。
- mount/umount/mkdir/cp 一律 `duct::cmd!`（项目惯例）；不引入 nix/libc。
- 单元测试必须无 root 全绿；需要 root 的集成测试在非 root 环境自动 skip（`require_root()` helper）。
- 目录布局（spec 锁定）：存储根（`--bases` flag 值）下 `bases/{base-id}/`、`volumes/{volume-id}/`、`work/{volume-id}/`；PV 名 = `overlayfs-<pvc-uid>`；volume-id = PV 名。
- Commit 信息用 conventional commits（feat/fix/docs/test/chore）。

---

### Task 1: `base.rs` — Store 路径推导 + Base TTL

**Files:**
- Create: `src/base.rs`
- Modify: `src/lib.rs`（顶部加 `pub mod base;`，其余暂不动）

**Interfaces:**
- Consumes: 无（首批模块）。
- Produces（后续任务依赖的精确签名）:
  - `pub struct Store { pub root: PathBuf }`；`Store::new(root: impl Into<PathBuf>) -> Self`
  - `Store::bases_dir(&self) -> PathBuf`；`Store::volumes_dir(&self) -> PathBuf`
  - `Store::volume_dir(&self, volume_id: &str) -> PathBuf`（= `volumes/{volume_id}`）
  - `Store::work_dir(&self, volume_id: &str) -> PathBuf`（= `work/{volume_id}`）
  - `Store::list_bases(&self) -> anyhow::Result<Vec<Base>>`；`Store::find_valid_base(&self, max_age_s: i64) -> anyhow::Result<Option<Base>>`
  - `pub struct Base(pub PathBuf)`；`Base::as_base_file(&self) -> PathBuf`；`Base::write_time(&self) -> anyhow::Result<()>`；`Base::read_time(&self) -> anyhow::Result<OffsetDateTime>`；`Base::valid(&self, max_age_s: i64) -> bool`
  - `pub const AS_BASE_FILENAME: &str = ".as_base";`

- [ ] **Step 1: 写失败测试**

`src/base.rs` 初版（含测试；实现函数先留空返回默认值让测试失败）：

```rust
use std::path::PathBuf;

use anyhow::Context;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub const AS_BASE_FILENAME: &str = ".as_base";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Base(pub PathBuf);

impl Base {
    pub fn as_base_file(&self) -> PathBuf {
        self.0.join(AS_BASE_FILENAME)
    }

    pub fn write_time(&self) -> anyhow::Result<()> {
        std::fs::write(
            self.as_base_file(),
            OffsetDateTime::now_utc().format(&Rfc3339)?,
        )?;
        Ok(())
    }

    pub fn read_time(&self) -> anyhow::Result<OffsetDateTime> {
        let data = std::fs::read_to_string(self.as_base_file())?;
        Ok(OffsetDateTime::parse(&data, &Rfc3339)?)
    }

    pub fn valid(&self, max_age_s: i64) -> bool {
        // TODO(fail-first): 返回 false 让测试先失败
        false
    }
}

#[derive(Debug, Clone)]
pub struct Store {
    pub root: PathBuf,
}

impl Store {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
    pub fn bases_dir(&self) -> PathBuf {
        self.root.join("bases")
    }
    pub fn volumes_dir(&self) -> PathBuf {
        self.root.join("volumes")
    }
    pub fn volume_dir(&self, volume_id: &str) -> PathBuf {
        self.volumes_dir().join(volume_id)
    }
    pub fn work_dir(&self, volume_id: &str) -> PathBuf {
        self.root.join("work").join(volume_id)
    }
    pub fn list_bases(&self) -> anyhow::Result<Vec<Base>> {
        // TODO(fail-first)
        Ok(vec![])
    }
    pub fn find_valid_base(&self, _max_age_s: i64) -> anyhow::Result<Option<Base>> {
        // TODO(fail-first)
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn past_timestamp(seconds_ago: i64) -> String {
        (OffsetDateTime::now_utc() - time::Duration::seconds(seconds_ago))
            .format(&Rfc3339)
            .unwrap()
    }

    #[test]
    fn paths_derive_from_root() {
        let s = Store::new("/var/lib/overlayfs-csi");
        assert_eq!(s.bases_dir(), PathBuf::from("/var/lib/overlayfs-csi/bases"));
        assert_eq!(
            s.volume_dir("v1"),
            PathBuf::from("/var/lib/overlayfs-csi/volumes/v1")
        );
        assert_eq!(s.work_dir("v1"), PathBuf::from("/var/lib/overlayfs-csi/work/v1"));
    }

    #[test]
    fn base_valid_respects_ttl() {
        let dir = std::env::temp_dir().join(format!("ofcsi-test-{}", std::process::id()));
        let base = Base(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        base.write_time().unwrap();
        assert!(base.valid(3600), "fresh base must be valid");

        std::fs::write(base.as_base_file(), past_timestamp(7200)).unwrap();
        assert!(!base.valid(3600), "base older than TTL must be invalid");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn list_and_find_bases() {
        let dir = std::env::temp_dir().join(format!("ofcsi-list-{}", std::process::id()));
        let store = Store::new(&dir);
        std::fs::create_dir_all(store.bases_dir().join("b1")).unwrap();
        std::fs::create_dir_all(store.bases_dir().join("b2")).unwrap();
        std::fs::write(store.bases_dir().join("not-a-dir"), "x").unwrap();
        Base(store.bases_dir().join("b1")).write_time().unwrap();
        // b2 过期
        std::fs::write(
            store.bases_dir().join("b2").join(AS_BASE_FILENAME),
            past_timestamp(999_999),
        )
        .unwrap();

        let all = store.list_bases().unwrap();
        assert_eq!(all.len(), 2, "files are not bases");

        let valid = store.find_valid_base(3600).unwrap();
        assert_eq!(valid, Some(Base(store.bases_dir().join("b1"))));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test base:: 2>&1 | tail -30`
Expected: `base_valid_respects_ttl`、`list_and_find_bases` FAIL（valid 返回 false / list_bases 空）。

- [ ] **Step 3: 最小实现**

替换 `valid` / `list_bases` / `find_valid_base`：

```rust
    pub fn valid(&self, max_age_s: i64) -> bool {
        let Ok(dt) = self.read_time() else {
            return false;
        };
        let age = OffsetDateTime::now_utc() - dt;
        if age.is_negative() {
            tracing::warn!(?self, "Base in the future");
            false
        } else {
            age.whole_seconds() < max_age_s
        }
    }
```

```rust
    pub fn list_bases(&self) -> anyhow::Result<Vec<Base>> {
        Ok(std::fs::read_dir(&self.bases_dir())?
            .filter_map(Result::ok)
            .filter(|x| x.file_type().map_or(false, |t| t.is_dir()))
            .map(|x| Base(x.path()))
            .collect())
    }

    pub fn find_valid_base(&self, max_age_s: i64) -> anyhow::Result<Option<Base>> {
        Ok(self.list_bases()?.into_iter().find(|b| b.valid(max_age_s)))
    }
```

`src/lib.rs` 顶部（`use` 区之后）加：

```rust
pub mod base;
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test base::`
Expected: 3 个测试全部 `ok`，且 `cargo build` 无警告。

- [ ] **Step 5: Commit**

```bash
git add src/base.rs src/lib.rs
git commit -m "feat: Store 路径推导与 Base TTL（base.rs）"
```

---

### Task 2: `base.rs` — mountinfo 解析与 base 引用判定

**Files:**
- Modify: `src/base.rs`

**Interfaces:**
- Consumes: 无（纯解析）。
- Produces:
  - `pub fn overlay_lowerdirs(mountinfo: &str) -> Vec<(String, Vec<PathBuf>)>` — 入参为 `/proc/self/mountinfo` 文本，返回 `(挂载点, lowerdir 列表)`，仅 overlay 挂载。
  - `pub fn is_base_referenced(mountinfo: &str, base: &Path) -> bool`

- [ ] **Step 1: 写失败测试**

追加到 `src/base.rs` 的 `mod tests`：

```rust
    const MOUNTINFO: &str = "\
36 35 98:0 / /mnt/host rw - overlay none lowerdir=/var/lib/overlayfs-csi/bases/abc,upperdir=/volumes/v1,workdir=/work/v1 rw\n\
37 36 0:41 / /proc rw,nosuid - proc proc rw\n\
38 36 259:2 /data /mnt/host/data rw - overlay none lowerdir=/other/base,upperdir=/u2,workdir=/w2 rw\n";

    #[test]
    fn parses_overlay_lowerdirs() {
        let m = overlay_lowerdirs(MOUNTINFO);
        assert_eq!(m.len(), 2, "proc 行不是 overlay，必须被过滤");
        assert_eq!(m[0].0, "/mnt/host");
        assert_eq!(m[0].1, vec![PathBuf::from("/var/lib/overlayfs-csi/bases/abc")]);
    }

    #[test]
    fn detects_base_reference() {
        assert!(is_base_referenced(
            MOUNTINFO,
            Path::new("/var/lib/overlayfs-csi/bases/abc")
        ));
        assert!(!is_base_referenced(MOUNTINFO, Path::new("/bases/nope")));
    }
```

文件顶部 `use` 增加 `use std::path::{Path, PathBuf};`（替换原 `use std::path::PathBuf;`）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test base::tests::parses 2>&1 | tail -10`
Expected: FAIL（`overlay_lowerdirs` 未定义，编译错误）。

- [ ] **Step 3: 最小实现**

`src/base.rs` 加（tests 模块之前）：

```rust
/// Parse `/proc/self/mountinfo` content into (mount point, lowerdirs) of overlay mounts.
/// 注：mount point 内空格按 mountinfo 规则转义为 `\040`；K8s 路径不含空格，不做反转义。
pub fn overlay_lowerdirs(mountinfo: &str) -> Vec<(String, Vec<PathBuf>)> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let (_info, fs) = line.split_once(" - ")?;
            let mut fields = fs.split_whitespace();
            if fields.next()? != "overlay" {
                return None;
            }
            let opts = fields.next()?;
            let lower = opts.split(',').find_map(|o| o.strip_prefix("lowerdir="))?;
            let mountpoint = _info.split_whitespace().nth(4)?.to_string();
            Some((mountpoint, lower.split(':').map(PathBuf::from).collect()))
        })
        .collect()
}

/// Whether any overlay mount's lowerdir points at `base`.
pub fn is_base_referenced(mountinfo: &str, base: &Path) -> bool {
    overlay_lowerdirs(mountinfo)
        .iter()
        .any(|(_, dirs)| dirs.iter().any(|d| d == base))
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test base::`
Expected: 5 个测试全部 `ok`。

- [ ] **Step 5: Commit**

```bash
git add src/base.rs
git commit -m "feat: mountinfo 解析与 base 引用判定"
```

---

### Task 3: `base.rs` — mount/unmount/promote 原语（root 集成测试）

**Files:**
- Modify: `src/base.rs`

**Interfaces:**
- Consumes: Task 1 的 `Store`/`Base`。
- Produces:
  - `pub fn mount_overlay(id: &str, lower: &Path, upper: &Path, work: &Path, ro: bool, mountpoint: &Path) -> anyhow::Result<()>`
  - `pub fn mount_bind(src: &Path, mountpoint: &Path) -> anyhow::Result<()>`
  - `pub fn umount_idempotent(mountpoint: &Path) -> anyhow::Result<()>` — 容忍 "not mounted"（现有 `unchecked()` 惯例），其他错误上抛。
  - `pub fn promote_to_base_with_mount(store: &Store, merged_mountpoint: &Path, new_id: &str) -> anyhow::Result<PathBuf>` — 固化：时间戳先行（防 TTL cleanup 误删）、`cp -a --reflink=auto`、失败删除半成品回滚并返回原错误（回滚自身失败则返回组合错误）。合并视图的挂载由调用方完成（node.rs Task 6 或测试）。

- [ ] **Step 1: 写失败测试**

追加到 `mod tests`：

```rust
    /// 非 root 环境自动 skip（集成测试需要真实 mount）。
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

    #[test]
    fn overlay_mount_unmount_roundtrip() {
        if !require_root() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ofcsi-mount-{}", std::process::id()));
        let lower = dir.join("lower");
        let upper = dir.join("upper");
        let work = dir.join("work");
        let mnt = dir.join("mnt");
        for d in [&lower, &upper, &work, &mnt] {
            std::fs::create_dir_all(d).unwrap();
        }
        std::fs::write(lower.join("from-base"), "base").unwrap();

        mount_overlay("t1", &lower, &upper, &work, false, &mnt).unwrap();
        assert_eq!(std::fs::read_to_string(mnt.join("from-base")).unwrap(), "base");
        std::fs::write(mnt.join("new-file"), "upper").unwrap();
        assert_eq!(std::fs::read_to_string(upper.join("new-file")).unwrap(), "upper", "写入必须落 upper");

        umount_idempotent(&mnt).unwrap();
        umount_idempotent(&mnt).unwrap(); // 二次 umount 必须幂等成功
        assert!(!mnt.join("from-base").exists(), "umount 后应露回空目录");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn promote_merges_overlay_view() {
        if !require_root() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ofcsi-promote-{}", std::process::id()));
        let store = Store::new(&dir);
        std::fs::create_dir_all(store.bases_dir()).unwrap();
        let base_dir = store.bases_dir().join("old");
        std::fs::create_dir_all(&base_dir).unwrap();
        std::fs::write(base_dir.join("old-file"), "old").unwrap();
        Base(base_dir.clone()).write_time().unwrap();

        // volume：删 old-file（产生 whiteout）、写 new-file
        let vdir = store.volume_dir("v1");
        std::fs::create_dir_all(&vdir).unwrap();
        std::fs::write(vdir.join("new-file"), "new").unwrap();

        let work = store.work_dir("v1");
        std::fs::create_dir_all(&work).unwrap();
        let merged = dir.join("merged");
        std::fs::create_dir_all(&merged).unwrap();
        mount_overlay("t2", &base_dir, &vdir, &work, true, &merged).unwrap();

        // 在合并视图里制造 whiteout：删除 old-file
        std::fs::remove_file(merged.join("old-file")).unwrap();

        let dst = promote_to_base_with_mount(&store, &merged, "newbase").unwrap();

        assert_eq!(
            std::fs::read_to_string(dst.join("new-file")).unwrap(),
            "new",
            "固化必须包含 upper 增量"
        );
        assert!(!dst.join("old-file").exists(), "whiteout 语义必须在固化结果中生效");
        assert!(dst.join(AS_BASE_FILENAME).exists(), "固化必须写入时间戳");
        Base(dst.clone()).read_time().unwrap(); // 时间戳可解析
        // 源 volume 不受影响
        assert!(vdir.join("new-file").exists());
        umount_idempotent(&merged).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn promote_without_base_copies_volume() {
        if !require_root() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("ofcsi-promote2-{}", std::process::id()));
        let store = Store::new(&dir);
        std::fs::create_dir_all(store.bases_dir()).unwrap();
        let vdir = store.volume_dir("v2");
        std::fs::create_dir_all(&vdir).unwrap();
        std::fs::write(vdir.join("f"), "1").unwrap();

        let dst = promote_to_base_with_mount(&store, &vdir, "nb2").unwrap();
        assert_eq!(std::fs::read_to_string(dst.join("f")).unwrap(), "1");
        assert!(dst.join(AS_BASE_FILENAME).exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test base::tests::promote 2>&1 | tail -10`
Expected: FAIL（函数未定义）。

- [ ] **Step 3: 最小实现**

`src/base.rs` 加（tests 模块之前）：

```rust
pub fn mount_overlay(
    id: &str,
    lower: &Path,
    upper: &Path,
    work: &Path,
    ro: bool,
    mountpoint: &Path,
) -> anyhow::Result<()> {
    let mut opts = format!(
        "lowerdir={},upperdir={},workdir={}",
        lower.display(),
        upper.display(),
        work.display()
    );
    if ro {
        opts.push_str(",ro");
    }
    duct::cmd!("mount", "-t", "overlay", id, "-o", opts, mountpoint).run()?;
    Ok(())
}

pub fn mount_bind(src: &Path, mountpoint: &Path) -> anyhow::Result<()> {
    duct::cmd!("mount", "--bind", src, mountpoint).run()?;
    Ok(())
}

pub fn umount_idempotent(mountpoint: &Path) -> anyhow::Result<()> {
    let out = duct::cmd!("umount", mountpoint).unchecked().run()?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let not_mounted = stderr.contains("not mounted")
            || stderr.contains("Invalid argument")
            || stderr.contains("No such file or directory");
        anyhow::ensure!(
            not_mounted,
            "umount {} failed: {}",
            mountpoint.display(),
            stderr
        );
    }
    Ok(())
}

pub fn promote_to_base_with_mount(
    store: &Store,
    merged: &Path,
    new_id: &str,
) -> anyhow::Result<PathBuf> {
    let dst = store.bases_dir().join(new_id);
    let result = (|| -> anyhow::Result<()> {
        std::fs::create_dir_all(&dst)?;
        // 时间戳先行：TTL cleanup 只删过期 base，新时间戳保证固化过程中不被误删
        Base(dst.clone()).write_time()?;
        duct::cmd!(
            "cp",
            "-a",
            "--reflink=auto",
            format!("{}/.", merged.display()),
            format!("{}/", dst.display())
        )
        .run()?;
        Ok(())
    })();
    match result {
        Ok(()) => Ok(dst),
        Err(e) => {
            if let Err(re) = std::fs::remove_dir_all(&dst) {
                return Err(anyhow::anyhow!(
                    "promote failed: {e:#}; rollback also failed: {re}"
                ));
            }
            Err(e)
        }
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test base::`（非 root 环境：3 个新测试打印 skip 并通过；root 环境：全部真实执行）
Expected: 全部 `ok`。

- [ ] **Step 5: Commit**

```bash
git add src/base.rs
git commit -m "feat: overlay/bind mount 原语与 base 固化（reflink 优先，失败回滚）"
```

---

### Task 4: `controller.rs` — provision 纯函数（pv_name / should_provision / build_pv）

**Files:**
- Create: `src/controller.rs`
- Modify: `src/lib.rs`（加 `pub mod controller;`）

**Interfaces:**
- Consumes: kube/k8s-openapi 类型。
- Produces:
  - `pub const PV_PREFIX: &str = "overlayfs-"`
  - `pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by"`、`pub const MANAGED_BY_VALUE: &str = "overlayfs-csi"`
  - `pub const SELECTED_NODE_ANNOTATION: &str = "volume.kubernetes.io/selected-node"`
  - `pub fn pv_name(pvc_uid: &str) -> String`
  - `pub fn should_provision(pvc: &PersistentVolumeClaim, storage_class: &str, node: &str) -> bool`
  - `pub fn build_pv(pvc: &PersistentVolumeClaim, node: &str, driver_name: &str) -> anyhow::Result<PersistentVolume>`

- [ ] **Step 1: 写失败测试**

`src/controller.rs` 初版：

```rust
use std::collections::BTreeMap;

use anyhow::Context;
use k8s_openapi::api::core::v1::{
    CSIPersistentVolumeSource, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    ObjectReference, PersistentVolume, PersistentVolumeClaim, PersistentVolumeSpec,
};
use k8s_openapi::api::storage::v1::VolumeNodeAffinity;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

pub const PV_PREFIX: &str = "overlayfs-";
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
pub const MANAGED_BY_VALUE: &str = "overlayfs-csi";
pub const SELECTED_NODE_ANNOTATION: &str = "volume.kubernetes.io/selected-node";

pub fn pv_name(pvc_uid: &str) -> String {
    format!("{PV_PREFIX}{pvc_uid}")
}

pub fn should_provision(_pvc: &PersistentVolumeClaim, _storage_class: &str, _node: &str) -> bool {
    // TODO(fail-first)
    false
}

pub fn build_pv(
    _pvc: &PersistentVolumeClaim,
    _node: &str,
    _driver_name: &str,
) -> anyhow::Result<PersistentVolume> {
    // TODO(fail-first)
    anyhow::bail!("not implemented")
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::PersistentVolumeClaimResources;

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
```

（fixture 中未用到的 `PersistentVolumeClaimResources` 导入若编译器报 unused 则删除。）

`src/lib.rs` 加 `pub mod controller;`。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test controller:: 2>&1 | tail -20`
Expected: `should_provision_matches_all_conditions`、`build_pv_fields` FAIL（todo 实现）。

- [ ] **Step 3: 最小实现**

```rust
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
        .requests
        .as_ref()
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
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test controller::`
Expected: 3 个测试 `ok`；类型字段名如有出入按 k8s-openapi 0.20 实际定义微调，断言语义不变。

- [ ] **Step 5: Commit**

```bash
git add src/controller.rs src/lib.rs
git commit -m "feat: provision 纯函数（deterministic PV 构造与 selected-node 过滤）"
```

---

### Task 5: `controller.rs` — PVC watcher、删除清理、TTL cleanup 与孤儿 GC

**Files:**
- Modify: `src/controller.rs`

**Interfaces:**
- Consumes: Task 1 `Store`/`Base`、Task 2 `is_base_referenced`、Task 4 全部纯函数。
- Produces:
  - `pub const GC_GRACE_S: u64 = 300`
  - `pub fn is_orphan(dir_name: &str, managed_volume_handles: &HashSet<String>, dir_age_s: u64) -> bool`
  - `pub struct Controller { pub kube: kube::Client, pub store: std::sync::Arc<crate::base::Store>, pub storage_class: String, pub node: String, pub driver_name: String, pub max_age_s: i64 }`
  - `impl Controller { pub async fn run(&self) -> anyhow::Result<()> }` — select 循环：PVC watcher（事件驱动，kube_runtime 自动重连）+ 30s reconcile + 30s cleanup/GC。事件处理失败记录 ERROR 后继续；watch 流意外结束直接 bail。
  - `pub async fn handle_pvc(&self, pvc: &PersistentVolumeClaim) -> anyhow::Result<()>` — provision（幂等）
  - `pub async fn handle_pvc_deleted(&self, pvc: &PersistentVolumeClaim) -> anyhow::Result<()>` — 删 PV + 本地目录
  - `pub async fn cleanup_once(&self) -> anyhow::Result<()>` — base TTL 清理 + 孤儿 GC

- [ ] **Step 1: 写失败测试**

追加到 `mod tests`：

```rust
    #[test]
    fn orphan_detection_logic() {
        let mut managed = std::collections::HashSet::new();
        managed.insert("overlayfs-u1".to_string());
        // 有对应 PV → 非孤儿；无 PV 但太新（grace 内）→ 非孤儿；无 PV 且超 grace → 孤儿
        assert!(!is_orphan("overlayfs-u1", &managed, 0));
        assert!(!is_orphan("overlayfs-u9", &managed, 10));
        assert!(is_orphan("overlayfs-u9", &managed, 10_000));
    }
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test controller::tests::orphan 2>&1 | tail -5`
Expected: FAIL（`is_orphan` 未定义）。

- [ ] **Step 3: 实现**

`src/controller.rs` 顶部 use 区补齐：

```rust
use std::collections::HashSet;

use futures::StreamExt;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::runtime::watcher;
use kube::Client;
use std::path::Path;
use std::sync::Arc;

use crate::base::{is_base_referenced, Base, Store};
```

实现代码：

```rust
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
        let mut reconciler = tokio::time::interval(std::time::Duration::from_secs(RECONCILE_INTERVAL_S));
        let mut janitor = tokio::time::interval(std::time::Duration::from_secs(RECONCILE_INTERVAL_S));
        loop {
            tokio::select! {
                ev = events.next() => match ev {
                    Some(Ok(watcher::Event::Apply(pvc))) => {
                        if let Err(e) = self.handle_pvc(&pvc).await {
                            tracing::error!(pvc = ?pvc.metadata.name, "provision failed: {e:#}");
                        }
                    }
                    Some(Ok(watcher::Event::Delete(pvc))) => {
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
            Err(kube::Error::Api(e)) if e.reason.as_deref() == Some("AlreadyExists") => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn handle_pvc_deleted(&self, pvc: &PersistentVolumeClaim) -> anyhow::Result<()> {
        let uid = pvc.metadata.uid.as_deref().context("deleted PVC has no uid")?;
        let name = pv_name(uid);
        let pvs: Api<PersistentVolume> = Api::all(self.kube.clone());
        match pvs.delete(&name, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.reason.as_deref() == Some("NotFound") => {}
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
            .list(&ListParams::default().labels(&format!(
                "{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"
            )))
            .await?;
        let managed: HashSet<String> = list
            .items
            .iter()
            .filter_map(|pv| pv.spec.as_ref()?.csi.as_ref()?.volume_handle.clone())
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
```

（Task 4 的 TODO stub `should_provision`/`build_pv` 已实现，保留。）

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test controller:: && cargo build`
Expected: 测试 `ok`；编译通过（kube 0.87 的 `watcher`/`Event` 命名如有出入按 0.87 实际 API 对齐，事件处理语义不变）。

- [ ] **Step 5: Commit**

```bash
git add src/controller.rs
git commit -m "feat: PVC watcher 驱动的 provision/删除清理/TTL cleanup/孤儿 GC"
```

---

### Task 6: `node.rs` + main.rs 组装 — CSI Node 两段式与固化接线

**Files:**
- Create: `src/node.rs`
- Modify: `src/main.rs`（重写）、`src/lib.rs`（删除旧 `Overlays` 实现，保留模块声明与 `OverlayFlags`）、`Cargo.toml`（加 uuid）

**Interfaces:**
- Consumes: Task 1/2/3 的 `base::*`，Task 5 的 `Controller`，proto 的 `v1::node_server::Node` / `v1::controller_server::Controller`。
- Produces:
  - `pub struct NodeService { pub node_id: String, pub store: std::sync::Arc<crate::base::Store>, pub max_age_s: i64 }`（实现 `v1::node_server::Node`）
  - `pub struct ControllerService;`（实现 `v1::controller_server::Controller`：`controller_get_capabilities` 返回 `CREATE_VOLUME`；`controller_create_volume`/`controller_delete_volume` 返回 `unimplemented`——spec 锁定，调用方不存在）
  - `OverlayFlags`（lib.rs）：`name`、`node`、`bases`（存储根）、`storage_class`、`max_age_s`；删除 `namespace`/`pods`/`size_limit`。

- [ ] **Step 1: 加 uuid 依赖**

`Cargo.toml` `[dependencies]` 加：

```toml
uuid = { version = "1", features = ["v4"] }
```

Run: `cargo build`
Expected: 编译通过。

- [ ] **Step 2: 写失败测试（stage 决策纯函数）**

`src/node.rs` 初版：

```rust
use crate::base::{Base, Store};

/// stage 决策：返回 Some(base) 表示 overlay stage，None 表示 bind mount。
pub fn stage_overlay_decision(store: &Store, max_age_s: i64) -> anyhow::Result<Option<Base>> {
    // TODO(fail-first)
    Ok(None)
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
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test node:: 2>&1 | tail -5`
Expected: FAIL（stub 返回 None，第二断言失败）。

- [ ] **Step 4: 实现**

`stage_overlay_decision` 改为 `store.find_valid_base(max_age_s)`。然后在同文件实现 `NodeService`（含 `promote` 私有方法）与 `ControllerService`：

```rust
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use tonic::{Request, Response, Status};

use crate::base;
use crate::v1;

pub struct NodeService {
    pub node_id: String,
    pub store: Arc<Store>,
    pub max_age_s: i64,
}

fn unimplemented() -> Status {
    Status::unimplemented("Unimplemented")
}

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
        let staging = req
            .staging_target_path
            .clone()
            .context("staging_target_path is required")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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
        let staging = req
            .staging_target_path
            .clone()
            .context("staging_target_path is required")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        tracing::info!(volume_id = %req.volume_id, %staging, "unstaging volume");
        base::umount_idempotent(Path::new(&staging)).map_err(|e| {
            tracing::error!(volume_id = %req.volume_id, "unstage umount failed: {e:#}");
            Status::internal(format!("unstage failed: {e:#}"))
        })?;

        // base 固化：upper 中有 .as_base 且当前无有效 base（spec 规则）
        let vdir = self.store.volume_dir(&req.volume_id);
        let marker = vdir.join(base::AS_BASE_FILENAME);
        if marker.exists() {
            match stage_overlay_decision(&self.store, self.max_age_s) {
                Ok(None) => {
                    let new_id = uuid::Uuid::new_v4().to_string();
                    tracing::info!(volume_id = %req.volume_id, base_id = %new_id, "promoting volume to base");
                    self.promote(&req.volume_id, &vdir, &new_id)
                        .map_err(|e| {
                            tracing::error!("promote failed: {e:#}");
                            Status::internal(format!("base promotion failed: {e:#}"))
                        })?;
                    std::fs::remove_file(&marker).map_err(|e| Status::internal(e.to_string()))?;
                }
                Ok(Some(_)) => tracing::info!("valid base exists, skipping promotion"),
                Err(e) => return Err(Status::internal(e.to_string())),
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
        let target = req
            .target_path
            .clone()
            .context("target_path is required")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let staging = req
            .staging_target_path
            .clone()
            .context("staging_target_path is required for persistent volumes")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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
        let target = req
            .target_path
            .clone()
            .context("target_path is required")
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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
        Ok(Response::new(Default::default()))
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
    fn promote(
        &self,
        volume_id: &str,
        vdir: &Path,
        new_id: &str,
    ) -> anyhow::Result<std::path::PathBuf> {
        let lower = stage_overlay_decision(&self.store, self.max_age_s)?;
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
                    v1::ControllerServiceCapability_Rpc {
                        r#type: v1::ControllerServiceCapability_Rpc_Type::CreateVolume as i32,
                    },
                )),
            }],
        }))
    }

    async fn controller_create_volume(
        &self,
        _req: Request<v1::ControllerCreateVolumeRequest>,
    ) -> Result<Response<v1::ControllerCreateVolumeResponse>, Status> {
        Err(unimplemented())
    }

    async fn controller_delete_volume(
        &self,
        _req: Request<v1::ControllerDeleteVolumeRequest>,
    ) -> Result<Response<v1::ControllerDeleteVolumeResponse>, Status> {
        Err(unimplemented())
    }
}
```

（tonic 0.10 生成的 `Controller` trait 对其余 RPC 提供默认 unimplemented 实现则无需列出；若编译器要求全实现，逐一 `Err(unimplemented())`。）

`src/lib.rs`：删除旧 `Overlays`/`PodUid`/旧 `Base` 与全部相关 impl（`from_flags`/`mount`/`unmount`/`cleanup`/`create_pod`/`watch_pod`/`delete_pod`/`bases`/`find_valid_base`/`empty_dir`/`volume_dir`/`base_host`），替换为：

```rust
pub mod base;
pub mod controller;
pub mod node;

use clap::Parser;

#[derive(Parser)]
pub struct OverlayFlags {
    /// CSI name (driver name)
    #[clap(long)]
    pub name: String,
    #[clap(long, alias = "nodeid")]
    pub node: String,
    /// Host storage root containing bases/, volumes/, work/
    #[clap(long)]
    pub bases: std::path::PathBuf,
    /// StorageClass name this driver provisions for
    #[clap(long)]
    pub storage_class: String,
    /// Maximum age of a base (seconds) before cleanup
    #[clap(long)]
    pub max_age_s: i64,
}
```

`src/main.rs` 重写：

```rust
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tracing::*;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::*;

pub mod v1 {
    tonic::include_proto!("csi.v1");
}

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
```

（注意：main.rs 不再需要 `k8s_openapi`/`kube::Api` 直接导入——PVC watch 在 controller.rs 内部。）

- [ ] **Step 5: 全量测试与编译验证**

Run: `cargo test && cargo clippy -- -D warnings 2>&1 | tail -20`
Expected: 全部测试 `ok`（含 Task 1-5 回归）；无 clippy 错误。

- [ ] **Step 6: Commit**

```bash
git add src/node.rs src/lib.rs src/main.rs Cargo.toml Cargo.lock
git commit -m "feat: CSI Node 两段式（stage/publish）+ unstage 固化 + Controller 骨架；移除 blank pod 机制"
```

---

### Task 7: chart 更新 — 挂载/RBAC/args/CSIDriver/StorageClass

**Files:**
- Modify: `chart/templates/csi.yaml`
- Create: `chart/templates/storageclass.yaml`
- Modify: `chart/values.yaml`

**Interfaces:**
- Consumes: Task 6 的 flags（`--bases` = 存储根、`--storage-class`；已删除 `--namespace`/`--size-limit`/`POD_ID`）。
- Produces: 部署清单；values 键 `storageClassName`（空 → 回退 driver name）、`storageRoot`。

- [ ] **Step 1: 更新 `chart/values.yaml`**

```yaml
namespace: kube-system
image: overlayfs-csi:latest
name: overlayfs.csi.k8s.io
# StorageClass 名；留空则使用 driver name
storageClassName: ""
# Host path root for bases/volumes/work
storageRoot: /var/lib/overlayfs-csi
# Maximum age of a base before cleaning it up
maxAgeSeconds: 86400
```

（删除 `basesSizeLimit`、`sizeLimit`——emptyDir 机制移除。）

- [ ] **Step 2: 更新 `chart/templates/csi.yaml`**

五处变更：

1. CSIDriver：

```yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: "{{ .Values.name }}"
spec:
  attachRequired: false
  podInfoOnMount: false
  volumeLifecycleModes:
    - Persistent
```

2. ClusterRole rules 只保留：

```yaml
rules:
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "delete"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch"]
```

（删除 pods/pv 以外全部规则。）

3. csi 容器 args（env 只留 `KUBE_NODE_NAME`，删除 `POD_ID`）：

```yaml
          args:
            - "--endpoint=/csi/csi.sock"
            - "--nodeid=$(KUBE_NODE_NAME)"
            - "--name={{ .Values.name }}"
            - "--bases={{ .Values.storageRoot }}"
            - "--storage-class={{ .Values.storageClassName | default .Values.name }}"
            - "--max-age-s={{ .Values.maxAgeSeconds }}"
```

4. csi 容器 volumeMounts（删除 storageroot-dir/storagerunroot-dir 两个挂载——containerd storage 目录是模板残留，与本 driver 无关）：

```yaml
          volumeMounts:
            - mountPath: /bases
              name: storage-root
            - mountPath: /csi
              name: socket-dir
            - mountPath: /var/lib/kubelet/pods
              mountPropagation: Bidirectional
              name: mountpoint-dir
            - mountPath: /var/lib/kubelet/plugins
              mountPropagation: Bidirectional
              name: plugins-dir
```

5. volumes 段（删除 bases emptyDir 与两个 containerd storage hostPath）：

```yaml
      volumes:
        - hostPath:
            path: "{{ .Values.storageRoot }}"
            type: DirectoryOrCreate
          name: storage-root
        - hostPath:
            path: "/var/lib/kubelet/plugins/{{ .Values.name }}"
            type: DirectoryOrCreate
          name: socket-dir
        - hostPath:
            path: /var/lib/kubelet/pods
            type: DirectoryOrCreate
          name: mountpoint-dir
        - hostPath:
            path: /var/lib/kubelet/plugins
            type: DirectoryOrCreate
          name: plugins-dir
        - hostPath:
            path: /var/lib/kubelet/plugins_registry
            type: Directory
          name: registration-dir
```

（说明：`/bases` 容器内挂载点即存储根，driver 从 `--bases` flag 读到的是同一 hostPath；socket-dir 与 plugins-dir 的 host 侧嵌套、容器侧路径（/csi 与 /var/lib/kubelet/plugins）互不嵌套，无冲突。）

- [ ] **Step 3: 新增 `chart/templates/storageclass.yaml`**

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: "{{ .Values.storageClassName | default .Values.name }}"
provisioner: "{{ .Values.name }}"
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: false
```

- [ ] **Step 4: 模板渲染验证**

Run: `helm template test chart --set namespace=kube-system | grep -E "kind:|storageRoot|storage-class|Persistent|WaitForFirstConsumer|emptyDir|POD_ID"`
Expected: 含 CSIDriver(Persistent)、StorageClass(WaitForFirstConsumer)、DaemonSet 带 storage-root hostPath 与 `--storage-class`；不含 `emptyDir`/`POD_ID`。

- [ ] **Step 5: Commit**

```bash
git add chart/
git commit -m "feat(chart): Persistent 模式——hostPath 存储根、plugins 挂载、RBAC 瘦身、StorageClass"
```

---

### Task 8: 示例与文档

**Files:**
- Delete: `data_pod.yaml`
- Modify: `pod.yaml`、`README.md`

**Interfaces:** 无代码接口；文档与示例需与最终行为一致。

- [ ] **Step 1: 删除 `data_pod.yaml`，重写 `pod.yaml` 为 PVC 示例**

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: demo
  namespace: overlayfs-csi
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: overlayfs.csi.k8s.io
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: test
  namespace: overlayfs-csi
spec:
  nodeName: node1
  terminationGracePeriodSeconds: 1
  containers:
    - name: test
      image: debian:bullseye-slim
      command: ["sleep", "infinity"]
      volumeMounts:
        - name: data
          mountPath: /test
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: demo
```

（`namespace`/`nodeName` 按实际部署调整；多节点时去掉 `nodeName` 交给调度器。）

- [ ] **Step 2: 更新 README.md**

- Usage：卷改为「独立 PVC + `persistentVolumeClaim` 引用」，注明 `WaitForFirstConsumer` 与数据固定节点语义；注明 generic ephemeral volume 的 PVC 随 pod 删除回收，不满足持久诉求，勿用。
- `.as_base` 语义：数据天然持久（PVC）；`.as_base` 在卷卸载时把「当前环境（base+增量合并视图）」固化为新 base，供后续新 PVC 复用；已有有效 base 时跳过。
- Implementation details：按新架构重写（PVC watcher、selected-node 注解、两段式 mount、固化、TTL cleanup、孤儿 GC、mountinfo 引用判定）；删除 blank pod/ephemeral 限额段落。
- 测试示例段改为 Task 9 的 e2e 场景。

- [ ] **Step 3: Commit**

```bash
git rm data_pod.yaml
git add pod.yaml README.md
git commit -m "docs: PVC 用法示例与新架构说明；移除 data_pod.yaml"
```

---

### Task 9: 全量验证与 e2e 验收

**Files:** 无新文件；运行验证。

**Interfaces:** 消费全部前序任务产出。

- [ ] **Step 1: 全量测试**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: 全绿。

- [ ] **Step 2: 构建镜像（沿用现有交叉编译流程）**

```bash
cd docker && cross build -r --target-dir ../target-cross
cp ../target-cross/release/csi .
docker build -t overlayfs-csi:latest .
docker save overlayfs-csi:latest | gzip > /tmp/overlayfs-csi.tar.gz
```

- [ ] **Step 3: k3s 分发与部署（多节点每台执行导入；本地 import 不需要 --hosts-dir，那是 pull 场景的参数）**

```bash
k3s ctr images import /tmp/overlayfs-csi.tar.gz
helm upgrade --install overlayfs-csi chart
kubectl -n kube-system rollout status ds/overlayfs-csi
```

- [ ] **Step 4: e2e 场景验收**

```bash
kubectl apply -f pod.yaml
kubectl exec test -- sh -c 'apt-get update && apt-get install -y curl && touch /test/.as_base && echo ok'
kubectl delete pod test && kubectl apply -f pod.yaml   # 同 PVC 重建
kubectl exec test -- which curl   # 场景1 PASS 条件：路径存在
```

- 场景 1（核心）：`which curl` 非空 → 数据跨 pod 重建持久。
- 场景 2（base 复用）：新建 PVC `demo2` + pod B 挂载 → `/test` 下可见 `usr/bin/curl` 等（来自固化 base）。
- 场景 3（删除回收）：`kubectl delete pvc demo` → PV `overlayfs-<uid>` 消失、宿主 `volumes/overlayfs-<uid>` 目录被 GC。
- 场景 4（driver 重启）：`kubectl -n kube-system delete pod -l app=overlayfs.csi.k8s.io` → 重建后重复场景 1 验证 base 未丢。
- 场景 5（TTL）：`--max-age-s` 临时调小（如 60）→ 2 分钟内 cleanup 日志确认无引用 base 被删。
- 任一场景失败：收集 `kubectl -n kube-system logs ds/overlayfs-csi -c csi` 与 `kubectl describe pvc/pod`，按铁律 12 先定位根因再修，禁止改程序凑用例。

- [ ] **Step 5: 性能对比验证（硬约束证据）**

改造前基线：对当前（改造前）部署的 test pod 跑：

```bash
kubectl exec test -- sh -c 'apt-get install -y fio && fio --name=t --filename=/test/fio --size=512M --bs=1M --rw=write --ioengine=libaio --iodepth=4 --fsync=16 --runtime=30 --time_based' 2>&1 | grep -E "WRITE|bw="
# 4k 随机：--bs=4k --rw=randrw --rwmixread=50
```

改造后相同命令复测（同节点、pod 空闲时，各 3 次取中位）。PASS 条件：带宽/IOPS 差异在 ±5% 内。超差即违反硬约束，必须回查根因（预期 IO 路径完全一致：本地盘 overlay copy-up）。

- [ ] **Step 6: 最终 Commit**

```bash
git add -A
git commit -m "chore: 全量验证通过（单元/root 集成/e2e 场景/性能对比）" --allow-empty
```
