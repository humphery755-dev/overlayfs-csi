use std::path::{Path, PathBuf};

use anyhow::Context;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub const AS_BASE_FILENAME: &str = ".as_base";
/// 卷级 TTL 元文件：stage 时由 volume_context 写入，unstage 固化时读取。
/// NodeUnstageVolumeRequest 没有 volume_context 字段，无法像 stage 一样透传。
pub const VOLUME_TTL_FILENAME: &str = ".ofcsi-max-age-s";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Base(pub PathBuf);

impl Base {
    pub fn as_base_file(&self) -> PathBuf {
        self.0.join(AS_BASE_FILENAME)
    }

    /// 写入时间戳与可选的卷级 TTL（秒）。两行格式：第一行 RFC3339 时间戳，
    /// 第二行 TTL；ttl 为 None 时只有第一行（旧格式，TTL 回退全局值）。
    pub fn write_time(&self, ttl: Option<i64>) -> anyhow::Result<()> {
        let mut data = OffsetDateTime::now_utc().format(&Rfc3339)?;
        if let Some(ttl) = ttl {
            data.push('\n');
            data.push_str(&ttl.to_string());
        }
        std::fs::write(self.as_base_file(), data)?;
        Ok(())
    }

    pub fn read_time(&self) -> anyhow::Result<OffsetDateTime> {
        let data = std::fs::read_to_string(self.as_base_file())?;
        let first = data.lines().next().context("empty .as_base file")?;
        Ok(OffsetDateTime::parse(first, &Rfc3339)?)
    }

    /// 内嵌的卷级 TTL（秒）。旧单行格式或第二行损坏 → None（回退全局值）。
    pub fn ttl(&self) -> Option<i64> {
        let data = std::fs::read_to_string(self.as_base_file()).ok()?;
        data.lines().nth(1).and_then(|l| l.trim().parse().ok())
    }

    pub fn valid(&self, max_age_s: i64) -> bool {
        // 内嵌 TTL 优先（该卷固化时声明），缺省回退全局值——旧格式 base 天然兼容
        let effective = self.ttl().unwrap_or(max_age_s);
        let Ok(dt) = self.read_time() else {
            return false;
        };
        let age = OffsetDateTime::now_utc() - dt;
        if age.is_negative() {
            tracing::warn!(?self, "Base in the future");
            false
        } else {
            age.whole_seconds() < effective
        }
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
    pub fn work_root(&self) -> PathBuf {
        self.root.join("work")
    }
    pub fn work_dir(&self, volume_id: &str) -> PathBuf {
        self.work_root().join(volume_id)
    }
    /// VM-like pod 快照根：注解驱动的持久化目录（见 src/webhook.rs 模块文档）
    pub fn vm_root(&self) -> PathBuf {
        self.root.join("vm")
    }
    /// 单个 VM-like pod 的快照目录：<vm_root>/<namespace>/<pod-name>
    pub fn vm_dir(&self, namespace: &str, pod_name: &str) -> PathBuf {
        self.vm_root().join(namespace).join(pod_name)
    }
    /// Create the storage-root subtrees (idempotent); call once at startup.
    pub fn init(&self) -> anyhow::Result<()> {
        for d in [self.bases_dir(), self.volumes_dir(), self.work_root(), self.vm_root()] {
            std::fs::create_dir_all(d)?;
        }
        Ok(())
    }
    pub fn list_bases(&self) -> anyhow::Result<Vec<Base>> {
        let mut bases = Vec::new();
        for entry in std::fs::read_dir(self.bases_dir())? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                bases.push(Base(entry.path()));
            }
        }
        Ok(bases)
    }

    pub fn find_valid_base(&self, max_age_s: i64) -> anyhow::Result<Option<Base>> {
        Ok(self.list_bases()?.into_iter().find(|b| b.valid(max_age_s)))
    }
}

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
            // After " - ": <fstype> <source> <super_options>; lowerdir lives in super_options.
            fields.next()?; // source
            let opts = fields.next()?; // super options
            let lower = opts.split(',').find_map(|o| o.strip_prefix("lowerdir="))?;
            let mountpoint = _info.split_whitespace().nth(4)?.to_string();
            Some((mountpoint, lower.split(':').map(PathBuf::from).collect()))
        })
        .collect()
}

/// Whether any overlay mount's lowerdir points at `base`.
/// 入参为 [`overlay_lowerdirs`] 的解析结果：同一份 mountinfo 只解析一次，
/// 供本轮 cleanup 的全部引用检查复用。
pub fn is_referenced_in(mounts: &[(String, Vec<PathBuf>)], base: &Path) -> bool {
    mounts
        .iter()
        .any(|(_, dirs)| dirs.iter().any(|d| d == base))
}

/// lowerdir[0] of the overlay mount at `mountpoint`, if any.
pub fn mounted_lower_in(mounts: &[(String, Vec<PathBuf>)], mountpoint: &str) -> Option<PathBuf> {
    mounts
        .iter()
        .find(|(mp, _)| mp == mountpoint)
        .and_then(|(_, dirs)| dirs.first().cloned())
}

/// 删除目录（若存在）；不存在视为成功（幂等清理惯例，调用点不再各写 exists 守卫）。
pub fn remove_dir_all_if_exists(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}

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
    let out = duct::cmd!("umount", mountpoint)
        .stderr_capture()
        .unchecked()
        .run()?;
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
    ttl: Option<i64>,
) -> anyhow::Result<PathBuf> {
    let dst = store.bases_dir().join(new_id);
    let result = (|| -> anyhow::Result<()> {
        std::fs::create_dir_all(&dst)?;
        // 时间戳先行：TTL cleanup 只删过期 base，新时间戳保证固化过程中不被误删
        Base(dst.clone()).write_time(ttl)?;
        duct::cmd!(
            "cp",
            "-a",
            "--reflink=auto",
            format!("{}/.", merged.display()),
            format!("{}/", dst.display())
        )
        .run()?;
        // cp -a 会把源数据目录里的 `.as_base` 标记（0 字节）覆盖进 dst，
        // 导致时间戳不可解析、base 永久失效、随后被 TTL janitor 误删 —— 拷贝完成后重写。
        Base(dst.clone()).write_time(ttl)?;
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
    fn init_creates_storage_subtrees_idempotently() {
        // 冷节点：bases/、volumes/、work/ 尚不存在是预期状态，非系统错误
        let dir = std::env::temp_dir().join(format!("ofcsi-init-{}", std::process::id()));
        let store = Store::new(&dir);
        store.init().unwrap();
        store.init().unwrap(); // 二次 init 必须幂等成功
        for d in [
            store.bases_dir(),
            store.volumes_dir(),
            store.work_root(),
            store.vm_root(),
        ] {
            assert!(d.is_dir(), "{} 必须存在", d.display());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn base_valid_respects_ttl() {
        let dir = std::env::temp_dir().join(format!("ofcsi-test-{}", std::process::id()));
        let base = Base(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        base.write_time(None).unwrap();
        assert!(base.valid(3600), "fresh base must be valid");

        std::fs::write(base.as_base_file(), past_timestamp(7200)).unwrap();
        assert!(!base.valid(3600), "base older than TTL must be invalid");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn base_ttl_overrides_global() {
        // 两行格式：较早创建 + 内嵌小 TTL → 即使全局 TTL 很大也判定过期
        let dir = std::env::temp_dir().join(format!("ofcsi-ttl-{}", std::process::id()));
        let base = Base(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            base.as_base_file(),
            format!("{}\n100", past_timestamp(200)),
        )
        .unwrap();
        assert_eq!(base.ttl(), Some(100), "第二行为内嵌 TTL");
        assert!(!base.valid(86_400), "内嵌 TTL 100s 必须优先于全局 86400s");

        // 旧单行格式 → ttl() 为 None，回退全局 TTL
        std::fs::write(base.as_base_file(), past_timestamp(200)).unwrap();
        assert_eq!(base.ttl(), None, "旧格式无内嵌 TTL");
        assert!(base.valid(86_400), "旧格式回退全局 TTL");

        // 第二行损坏 → 同样回退
        std::fs::write(base.as_base_file(), format!("{}\nnot-a-number", past_timestamp(200)))
            .unwrap();
        assert_eq!(base.ttl(), None);
        assert!(base.valid(86_400));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn write_time_persists_ttl() {
        let dir = std::env::temp_dir().join(format!("ofcsi-wttl-{}", std::process::id()));
        let base = Base(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        base.write_time(Some(7776000)).unwrap();
        base.read_time().expect("时间戳行必须可解析");
        assert_eq!(base.ttl(), Some(7776000), "内嵌 TTL 必须持久化");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn list_and_find_bases() {
        let dir = std::env::temp_dir().join(format!("ofcsi-list-{}", std::process::id()));
        let store = Store::new(&dir);
        std::fs::create_dir_all(store.bases_dir().join("b1")).unwrap();
        std::fs::create_dir_all(store.bases_dir().join("b2")).unwrap();
        std::fs::write(store.bases_dir().join("not-a-dir"), "x").unwrap();
        Base(store.bases_dir().join("b1")).write_time(None).unwrap();
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
        let mounts = overlay_lowerdirs(MOUNTINFO);
        assert!(is_referenced_in(
            &mounts,
            Path::new("/var/lib/overlayfs-csi/bases/abc")
        ));
        assert!(!is_referenced_in(&mounts, Path::new("/bases/nope")));
    }

    #[test]
    fn mounted_lower_in_returns_overlay_lower() {
        let mounts = overlay_lowerdirs(MOUNTINFO);
        assert_eq!(
            mounted_lower_in(&mounts, "/mnt/host"),
            Some(PathBuf::from("/var/lib/overlayfs-csi/bases/abc"))
        );
        assert_eq!(
            mounted_lower_in(&mounts, "/mnt/host/data"),
            Some(PathBuf::from("/other/base"))
        );
    }

    #[test]
    fn mounted_lower_in_rejects_bind_and_unknown() {
        let mounts = overlay_lowerdirs(
            "40 36 0:43 / /mnt/staging rw - ext4 /dev/sda1 rw\n",
        );
        assert_eq!(
            mounted_lower_in(&mounts, "/mnt/staging"),
            None,
            "bind 挂载没有 lowerdir，语义等同无旧 base"
        );
        assert_eq!(mounted_lower_in(&mounts, "/mnt/absent"), None);
    }

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
        Base(base_dir.clone()).write_time(None).unwrap();

        // volume：删 old-file（产生 whiteout）、写 new-file
        let vdir = store.volume_dir("v1");
        std::fs::create_dir_all(&vdir).unwrap();
        std::fs::write(vdir.join("new-file"), "new").unwrap();

        let work = store.work_dir("v1");
        std::fs::create_dir_all(&work).unwrap();
        let merged = dir.join("merged");
        std::fs::create_dir_all(&merged).unwrap();
        // 测试用 rw 挂载：需要通过合并视图删除文件以制造 whiteout（ro 挂载会 EROFS）
        mount_overlay("t2", &base_dir, &vdir, &work, false, &merged).unwrap();

        // 在合并视图里制造 whiteout：删除 old-file
        std::fs::remove_file(merged.join("old-file")).unwrap();

        let dst = promote_to_base_with_mount(&store, &merged, "newbase", None).unwrap();

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
        // 纯 fs + cp，无 mount，非 root 可跑
        let dir = std::env::temp_dir().join(format!("ofcsi-promote2-{}", std::process::id()));
        let store = Store::new(&dir);
        std::fs::create_dir_all(store.bases_dir()).unwrap();
        let vdir = store.volume_dir("v2");
        std::fs::create_dir_all(&vdir).unwrap();
        std::fs::write(vdir.join("f"), "1").unwrap();
        // 源数据目录里的用户标记（0 字节）：cp -a 必须不能让它覆盖固化时间戳
        std::fs::write(vdir.join(AS_BASE_FILENAME), "").unwrap();

        let dst = promote_to_base_with_mount(&store, &vdir, "nb2", Some(7776000)).unwrap();
        assert_eq!(std::fs::read_to_string(dst.join("f")).unwrap(), "1");
        assert!(dst.join(AS_BASE_FILENAME).exists());
        let t = Base(dst.clone())
            .read_time()
            .expect("0 字节标记绝不能作为固化时间戳存活（base 会永久失效被 janitor 删）");
        assert!(
            (OffsetDateTime::now_utc() - t).whole_seconds() < 3600,
            "固化后的时间戳必须新鲜（1h 内）"
        );
        assert_eq!(
            Base(dst).ttl(),
            Some(7776000),
            "固化必须把卷级 TTL 写入新 base"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
