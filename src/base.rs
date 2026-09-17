use std::path::{Path, PathBuf};

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
        let mut bases = Vec::new();
        for entry in std::fs::read_dir(&self.bases_dir())? {
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
pub fn is_base_referenced(mountinfo: &str, base: &Path) -> bool {
    overlay_lowerdirs(mountinfo)
        .iter()
        .any(|(_, dirs)| dirs.iter().any(|d| d == base))
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
}
