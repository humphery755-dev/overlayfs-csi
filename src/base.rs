use std::path::PathBuf;

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
        Ok(std::fs::read_dir(&self.bases_dir())?
            .filter_map(Result::ok)
            .filter(|x| x.file_type().map_or(false, |t| t.is_dir()))
            .map(|x| Base(x.path()))
            .collect())
    }

    pub fn find_valid_base(&self, max_age_s: i64) -> anyhow::Result<Option<Base>> {
        Ok(self.list_bases()?.into_iter().find(|b| b.valid(max_age_s)))
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
