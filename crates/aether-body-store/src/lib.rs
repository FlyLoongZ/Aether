//! File storage for captured HTTP bodies.
//!
//! The store is deliberately dumb: one body per file, raw bytes, no header, no
//! padding, no compression. A body starts at offset 0 and is written as one
//! contiguous file. The file extension is the encoding marker; the capture
//! ledger in `request_metadata` records everything else.
//!
//! Layout:
//!
//! ```text
//! <root>/<yyyy>/<mm>/<dd>/<sha256(request_id)>/<side>.<ext>
//! ```
//!
//! The date is the request time in UTC. Cleanup removes whole date directories
//! older than the retention cutoff, so it never scans the DB or the whole tree.
//!
//! # Durability
//!
//! A write is atomic for readers: the bytes go to a temporary file in the same
//! directory and are renamed over the final path, so a reader sees either the
//! previous body or the new one, never a partial write. The store does not
//! `fsync`, because the bodies are diagnostic copies and the capture ledger in
//! the database is the authority; a crash can therefore lose the most recent
//! bodies, and the read path reports them as unavailable rather than failing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use aether_data_contracts::repository::usage::{
    BodyEncoding, UsageBodyField, MAX_USAGE_BODY_BYTES,
};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A body read back from the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBodyFile {
    pub encoding: BodyEncoding,
    pub bytes: Vec<u8>,
}

/// How many date directories a cleanup pass removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BodyFileCleanupStats {
    pub removed_days: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum BodyFileStoreError {
    #[error("body file io error: {0}")]
    Io(#[from] std::io::Error),
}

/// The file store rooted at one directory.
///
/// The store itself does not decide whether capture is enabled; that is the
/// capture policy's job. It only writes, reads and removes files.
#[derive(Debug, Clone)]
pub struct BodyFileStore {
    root: PathBuf,
}

impl BodyFileStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `<root>/<yyyy>/<mm>/<dd>/<sha256(request_id)>`.
    pub fn request_dir(&self, day: NaiveDate, request_id: &str) -> PathBuf {
        self.root
            .join(format!("{:04}", day.year()))
            .join(format!("{:02}", day.month()))
            .join(format!("{:02}", day.day()))
            .join(sha256_hex(request_id))
    }

    /// The final path for one side. The extension is the encoding.
    pub fn path(
        &self,
        day: NaiveDate,
        request_id: &str,
        side: UsageBodyField,
        encoding: BodyEncoding,
    ) -> PathBuf {
        self.request_dir(day, request_id)
            .join(format!("{}.{}", side.file_stem(), encoding.ext()))
    }

    /// Writes one body and removes any older extension for the same side.
    pub async fn put(
        &self,
        day: NaiveDate,
        request_id: &str,
        side: UsageBodyField,
        encoding: BodyEncoding,
        bytes: &[u8],
    ) -> Result<(), BodyFileStoreError> {
        let dir = self.request_dir(day, request_id);
        tokio::fs::create_dir_all(&dir).await?;
        set_dir_mode_private(&dir).await;

        let final_path = dir.join(format!("{}.{}", side.file_stem(), encoding.ext()));
        // Lifecycle and terminal writes can carry the same body more than once.
        // Length alone is not identity: the provider request body can change
        // between those writes while keeping its length, so compare the bytes
        // before skipping. The read hits the page cache because the previous
        // event just wrote the file, and it compares in chunks so a large body
        // is never held twice.
        if let Ok(metadata) = tokio::fs::metadata(&final_path).await {
            if metadata.is_file() && metadata.len() == bytes.len() as u64 {
                if let Ok(true) = file_equals(&final_path, bytes).await {
                    remove_other_extensions_best_effort(&dir, side, encoding).await;
                    return Ok(());
                }
            }
        }
        let tmp_path = dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));

        let write_result = write_private_file(&tmp_path, bytes).await;
        if let Err(error) = write_result {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(error.into());
        }
        if let Err(error) = tokio::fs::rename(&tmp_path, &final_path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(error.into());
        }

        // The extension is the encoding. A rewrite with a different encoding
        // must not leave the previous file behind, otherwise the read path sees
        // two candidates for one side. The new file is already in place, so a
        // cleanup failure must not turn a successful write into an error.
        remove_other_extensions_best_effort(&dir, side, encoding).await;
        Ok(())
    }

    /// Reads every side present in one request directory.
    ///
    /// The directory is listed once; each side is matched by file stem and the
    /// extension is parsed as its encoding. If a side has more than one file
    /// (e.g. a crash between the write and the old-extension cleanup), the
    /// newest file wins.
    pub async fn read_request(
        &self,
        day: NaiveDate,
        request_id: &str,
    ) -> Result<HashMap<UsageBodyField, StoredBodyFile>, BodyFileStoreError> {
        scan_sides(&self.request_dir(day, request_id), None).await
    }

    /// Reads one side's file, if present.
    ///
    /// The file extension is the encoding, so the directory is listed and only
    /// the requested side's bytes are read.
    pub async fn read_side(
        &self,
        day: NaiveDate,
        request_id: &str,
        side: UsageBodyField,
    ) -> Result<Option<StoredBodyFile>, BodyFileStoreError> {
        Ok(scan_sides(&self.request_dir(day, request_id), Some(side))
            .await?
            .remove(&side))
    }

    /// Removes every file for one side, regardless of extension.
    pub async fn delete_side(
        &self,
        day: NaiveDate,
        request_id: &str,
        side: UsageBodyField,
    ) -> Result<(), BodyFileStoreError> {
        remove_side_files(&self.request_dir(day, request_id), side, None).await
    }

    /// Removes one request directory (all four sides).
    pub async fn delete_request(
        &self,
        day: NaiveDate,
        request_id: &str,
    ) -> Result<(), BodyFileStoreError> {
        match tokio::fs::remove_dir_all(self.request_dir(day, request_id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Removes every date directory, leaving the configured root in place.
    ///
    /// This is [`Self::cleanup_before`] with a cutoff past every representable
    /// date, so it only ever removes `yyyy/mm/dd`-shaped directories. A root
    /// that also holds unrelated directories is not wiped.
    pub async fn purge_all(&self) -> Result<BodyFileCleanupStats, BodyFileStoreError> {
        self.cleanup_before(NaiveDate::MAX).await
    }

    /// Removes whole date directories older than `cutoff`.
    ///
    /// The cutoff is inclusive: a directory whose date is before `cutoff` is
    /// removed. Empty month/year directories are removed afterwards.
    pub async fn cleanup_before(
        &self,
        cutoff: NaiveDate,
    ) -> Result<BodyFileCleanupStats, BodyFileStoreError> {
        let mut stats = BodyFileCleanupStats::default();
        let mut years = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(stats),
            Err(error) => return Err(error.into()),
        };

        while let Some(year_entry) = years.next_entry().await? {
            let Some(year) = parse_component(year_entry.file_name().to_str(), 4) else {
                continue;
            };
            let year_path = year_entry.path();
            if !is_directory(&year_entry).await? {
                continue;
            }
            let Some(mut months) = read_dir_if_present(&year_path).await? else {
                continue;
            };
            while let Some(month_entry) = months.next_entry().await? {
                let Some(month) = parse_component(month_entry.file_name().to_str(), 2) else {
                    continue;
                };
                let month_path = month_entry.path();
                if !is_directory(&month_entry).await? {
                    continue;
                }
                let Some(mut days) = read_dir_if_present(&month_path).await? else {
                    continue;
                };
                while let Some(day_entry) = days.next_entry().await? {
                    let Some(day) = parse_component(day_entry.file_name().to_str(), 2) else {
                        continue;
                    };
                    let day_path = day_entry.path();
                    if !is_directory(&day_entry).await? {
                        continue;
                    }
                    let Some(year) = i32::try_from(year).ok() else {
                        continue;
                    };
                    let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
                        continue;
                    };
                    if date < cutoff {
                        // A concurrent purge or a second cleanup pass may have
                        // removed the day already; that is not a failure.
                        match tokio::fs::remove_dir_all(&day_path).await {
                            Ok(()) => stats.removed_days += 1,
                            Err(error) if is_not_found(&error) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                let _ = tokio::fs::remove_dir(&month_path).await;
            }
            let _ = tokio::fs::remove_dir(&year_path).await;
        }
        Ok(stats)
    }
}

/// The process-wide store. The body store is a single-node local resource, so
/// one process has at most one root. `None` means the path was not configured.
static GLOBAL: LazyLock<Mutex<Option<Arc<BodyFileStore>>>> = LazyLock::new(|| Mutex::new(None));

pub fn configure_global(store: Option<Arc<BodyFileStore>>) {
    if let Ok(mut guard) = GLOBAL.lock() {
        *guard = store;
    }
}

pub fn global() -> Option<Arc<BodyFileStore>> {
    GLOBAL.lock().ok().and_then(|guard| guard.clone())
}

pub fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub fn day_from_unix_ms(unix_ms: u64) -> Option<NaiveDate> {
    let unix_ms = i64::try_from(unix_ms).ok()?;
    DateTime::<Utc>::from_timestamp_millis(unix_ms).map(|timestamp| timestamp.date_naive())
}

/// The UTC day of a whole-second timestamp.
///
/// The usage read model stores `created_at`/`updated_at` as `EXTRACT(EPOCH ...)`
/// seconds even where the field name says `_unix_ms`, so callers that hold a
/// read-model timestamp must not feed it to [`day_from_unix_ms`].
pub fn day_from_unix_secs(unix_secs: u64) -> Option<NaiveDate> {
    let unix_secs = i64::try_from(unix_secs).ok()?;
    DateTime::<Utc>::from_timestamp(unix_secs, 0).map(|timestamp| timestamp.date_naive())
}

/// Whether an existing file holds exactly `bytes`.
///
/// The comparison streams the file in chunks so a large body is never held
/// twice in memory.
async fn file_equals(path: &Path, bytes: &[u8]) -> std::io::Result<bool> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = tokio::io::BufReader::new(file);
    let mut chunk = vec![0_u8; 64 * 1024];
    let mut offset = 0_usize;
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(offset == bytes.len());
        }
        let end = offset.saturating_add(read);
        if end > bytes.len() || chunk[..read] != bytes[offset..end] {
            return Ok(false);
        }
        offset = end;
    }
}

/// Lists one request directory and returns the newest file per side.
///
/// `only` restricts the scan to one side; the other sides are skipped before
/// their bytes are read.
async fn scan_sides(
    dir: &Path,
    only: Option<UsageBodyField>,
) -> Result<HashMap<UsageBodyField, StoredBodyFile>, BodyFileStoreError> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(HashMap::new());
        }
        Err(error) => return Err(error.into()),
    };

    let mut newest: HashMap<UsageBodyField, (std::time::SystemTime, StoredBodyFile)> =
        HashMap::new();
    while let Some(entry) = entries.next_entry().await? {
        match entry.file_type().await {
            Ok(file_type) if file_type.is_file() => {}
            Ok(_) => continue,
            // The file was removed by a concurrent delete or cleanup pass.
            Err(error) if is_not_found(&error) => continue,
            Err(error) => return Err(error.into()),
        }
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let Some((stem, ext)) = name.rsplit_once('.') else {
            continue;
        };
        let Some(side) = side_from_file_stem(stem) else {
            continue;
        };
        if only.is_some_and(|only| only != side) {
            continue;
        }
        let Some(encoding) = BodyEncoding::from_ext(ext) else {
            continue;
        };
        let path = entry.path();
        let Some((bytes, modified)) = read_body_file(&path, MAX_USAGE_BODY_BYTES).await? else {
            // A body above the read limit is unreadable, not fatal: skip this
            // side so the other sides of the same request still load.
            continue;
        };
        let candidate = StoredBodyFile { encoding, bytes };
        match newest.get(&side) {
            Some((existing_modified, _)) if *existing_modified >= modified => {}
            _ => {
                newest.insert(side, (modified, candidate));
            }
        }
    }

    Ok(newest
        .into_iter()
        .map(|(side, (_, file))| (side, file))
        .collect())
}

/// Reads one body file, returning `None` when it exceeds `limit`.
///
/// The size is taken from the opened inode, not from the path: a concurrent
/// `put` replaces the path with a new inode, so a stat on the path could
/// describe a different file than the one that is read. The inode behind an
/// open fd is never modified in place.
async fn read_body_file(
    path: &Path,
    limit: usize,
) -> Result<Option<(Vec<u8>, std::time::SystemTime)>, BodyFileStoreError> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        // A concurrent delete or cleanup pass removed the file between the
        // directory listing and the open; the side is simply absent.
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata().await?;
    if metadata.len() > limit as u64 {
        return Ok(None);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    // The extra byte detects a file that grew between the stat and the read
    // without ever allocating more than the limit.
    file.take(limit as u64 + 1).read_to_end(&mut bytes).await?;
    if bytes.len() > limit {
        return Ok(None);
    }
    let modified = metadata
        .modified()
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    Ok(Some((bytes, modified)))
}

fn side_from_file_stem(stem: &str) -> Option<UsageBodyField> {
    match stem {
        "client_request" => Some(UsageBodyField::RequestBody),
        "provider_request" => Some(UsageBodyField::ProviderRequestBody),
        "provider_response" => Some(UsageBodyField::ResponseBody),
        "client_response" => Some(UsageBodyField::ClientResponseBody),
        _ => None,
    }
}

fn parse_component(value: Option<&str>, width: usize) -> Option<u32> {
    let value = value?;
    if value.len() != width || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

async fn remove_side_files(
    dir: &Path,
    side: UsageBodyField,
    keep: Option<BodyEncoding>,
) -> Result<(), BodyFileStoreError> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let Some((stem, ext)) = name.rsplit_once('.') else {
            continue;
        };
        if stem != side.file_stem() {
            continue;
        }
        if keep.is_some_and(|keep| BodyEncoding::from_ext(ext) == Some(keep)) {
            continue;
        }
        // Only regular files are body copies; a same-named directory is not
        // ours to remove.
        match entry.file_type().await {
            Ok(file_type) if file_type.is_file() => {}
            Ok(_) => continue,
            Err(error) if is_not_found(&error) => continue,
            Err(error) => return Err(error.into()),
        }
        match tokio::fs::remove_file(entry.path()).await {
            Ok(()) => {}
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Removes the other encodings of one side without failing the write.
///
/// The new file is already in place when this runs, so a cleanup failure only
/// leaves an extra candidate behind; the read path still resolves the side.
async fn remove_other_extensions_best_effort(dir: &Path, side: UsageBodyField, keep: BodyEncoding) {
    if let Err(error) = remove_side_files(dir, side, Some(keep)).await {
        tracing::warn!(
            path = %dir.display(),
            side = side.file_stem(),
            error = %error,
            "body file encoding cleanup failed"
        );
    }
}

fn is_not_found(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
}

async fn read_dir_if_present(
    path: &Path,
) -> Result<Option<tokio::fs::ReadDir>, BodyFileStoreError> {
    match tokio::fs::read_dir(path).await {
        Ok(entries) => Ok(Some(entries)),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn is_directory(entry: &tokio::fs::DirEntry) -> Result<bool, BodyFileStoreError> {
    match entry.file_type().await {
        Ok(file_type) => Ok(file_type.is_dir()),
        Err(error) if is_not_found(&error) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(path).await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    Ok(())
}

async fn set_dir_mode_private(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (Arc<BodyFileStore>, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("aether-body-store-test-{}", uuid::Uuid::new_v4()));
        (Arc::new(BodyFileStore::new(root.clone())), root)
    }

    fn day(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
    }

    #[test]
    fn seconds_and_milliseconds_resolve_to_the_same_day() {
        let event_ms = 1_763_000_000_000_u64;
        assert_eq!(
            day_from_unix_secs(event_ms / 1_000),
            day_from_unix_ms(event_ms)
        );
        // A read-model second value must not be interpreted as milliseconds.
        assert_eq!(day_from_unix_secs(1_763_000_000), Some(day(2025, 11, 13)));
        assert_ne!(day_from_unix_ms(1_763_000_000), Some(day(2025, 11, 13)));
    }

    #[test]
    fn path_hashes_request_id_and_uses_encoding_extension() {
        let (store, _root) = temp_store();
        let path = store.path(
            day(2026, 11, 6),
            "trace/../../escape",
            UsageBodyField::RequestBody,
            BodyEncoding::Json,
        );
        let text = path.to_string_lossy();
        assert!(text.ends_with(&format!(
            "/{}/client_request.json",
            sha256_hex("trace/../../escape")
        )));
        assert!(!text.contains(".."));
    }

    #[tokio::test]
    async fn put_read_delete_roundtrip() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                br#"{"a":1}"#,
            )
            .await
            .expect("put");
        let bodies = store.read_request(date, "req-1").await.expect("read");
        let body = bodies
            .get(&UsageBodyField::RequestBody)
            .expect("request body");
        assert_eq!(body.encoding, BodyEncoding::Json);
        assert_eq!(body.bytes, br#"{"a":1}"#);

        store
            .delete_side(date, "req-1", UsageBodyField::RequestBody)
            .await
            .expect("delete side");
        assert!(store
            .read_request(date, "req-1")
            .await
            .expect("read")
            .is_empty());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn put_replaces_old_extension() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                b"{}",
            )
            .await
            .expect("put json");
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Sse,
                b"data: x\n\n",
            )
            .await
            .expect("put sse");

        let dir = store.request_dir(date, "req-1");
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(&dir).await.expect("read dir");
        while let Some(entry) = entries.next_entry().await.expect("entry") {
            names.push(entry.file_name().to_string_lossy().to_string());
        }
        names.sort();
        assert_eq!(names, vec!["client_request.sse"]);

        let bodies = store.read_request(date, "req-1").await.expect("read");
        assert_eq!(
            bodies
                .get(&UsageBodyField::RequestBody)
                .expect("body")
                .encoding,
            BodyEncoding::Sse
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn cleanup_removes_old_days_only() {
        let (store, root) = temp_store();
        let old = day(2026, 11, 1);
        let keep = day(2026, 11, 6);
        store
            .put(
                old,
                "req-old",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                b"{}",
            )
            .await
            .expect("put old");
        store
            .put(
                keep,
                "req-keep",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                b"{}",
            )
            .await
            .expect("put keep");

        let stats = store.cleanup_before(keep).await.expect("cleanup");
        assert_eq!(stats.removed_days, 1);
        assert!(store
            .read_request(old, "req-old")
            .await
            .expect("old")
            .is_empty());
        assert!(store
            .read_request(keep, "req-keep")
            .await
            .expect("keep")
            .contains_key(&UsageBodyField::RequestBody));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn read_body_file_skips_a_file_above_the_limit() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                b"0123456789",
            )
            .await
            .expect("put");
        let path = store.path(
            date,
            "req-1",
            UsageBodyField::RequestBody,
            BodyEncoding::Json,
        );
        assert!(read_body_file(&path, 10).await.expect("read").is_some());
        assert!(read_body_file(&path, 9).await.expect("read").is_none());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn file_equals_compares_content_not_length() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                b"abcd",
            )
            .await
            .expect("put");
        let path = store.path(
            date,
            "req-1",
            UsageBodyField::RequestBody,
            BodyEncoding::Json,
        );
        assert!(file_equals(&path, b"abcd").await.expect("equals"));
        assert!(!file_equals(&path, b"abce").await.expect("equals"));
        assert!(!file_equals(&path, b"abc").await.expect("equals"));
        assert!(!file_equals(&path, b"abcde").await.expect("equals"));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn read_side_reads_only_the_requested_side() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                br#"{"a":1}"#,
            )
            .await
            .expect("put request");
        store
            .put(
                date,
                "req-1",
                UsageBodyField::ResponseBody,
                BodyEncoding::Sse,
                b"data: x\n\n",
            )
            .await
            .expect("put response");

        let request = store
            .read_side(date, "req-1", UsageBodyField::RequestBody)
            .await
            .expect("read request")
            .expect("request body");
        assert_eq!(request.bytes, br#"{"a":1}"#.to_vec());
        assert_eq!(request.encoding, BodyEncoding::Json);

        let response = store
            .read_side(date, "req-1", UsageBodyField::ResponseBody)
            .await
            .expect("read response")
            .expect("response body");
        assert_eq!(response.encoding, BodyEncoding::Sse);

        assert!(store
            .read_side(date, "req-1", UsageBodyField::ProviderRequestBody)
            .await
            .expect("read missing")
            .is_none());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn put_rewrites_a_same_size_body_with_different_content() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::ProviderRequestBody,
                BodyEncoding::Json,
                br#"{"a":1}"#,
            )
            .await
            .expect("first put");
        store
            .put(
                date,
                "req-1",
                UsageBodyField::ProviderRequestBody,
                BodyEncoding::Json,
                br#"{"a":2}"#,
            )
            .await
            .expect("second put");

        let bodies = store.read_request(date, "req-1").await.expect("read");
        assert_eq!(
            bodies[&UsageBodyField::ProviderRequestBody].bytes,
            br#"{"a":2}"#.to_vec()
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn purge_all_only_removes_date_directories() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                b"{}",
            )
            .await
            .expect("put");
        let unrelated = root.join("unrelated");
        tokio::fs::create_dir_all(&unrelated).await.expect("mkdir");

        let stats = store.purge_all().await.expect("purge");
        assert_eq!(stats.removed_days, 1);
        assert!(store
            .read_request(date, "req-1")
            .await
            .expect("read")
            .is_empty());
        assert!(unrelated.is_dir(), "unrelated directory must survive");
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn read_missing_request_is_empty() {
        let (store, root) = temp_store();
        assert!(store
            .read_request(day(2026, 11, 6), "missing")
            .await
            .expect("read")
            .is_empty());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn delete_request_removes_all_sides() {
        let (store, root) = temp_store();
        let date = day(2026, 11, 6);
        store
            .put(
                date,
                "req-1",
                UsageBodyField::RequestBody,
                BodyEncoding::Json,
                b"{}",
            )
            .await
            .expect("put request");
        store
            .put(
                date,
                "req-1",
                UsageBodyField::ResponseBody,
                BodyEncoding::Sse,
                b"data: x\n\n",
            )
            .await
            .expect("put response");
        store
            .delete_request(date, "req-1")
            .await
            .expect("delete request");
        assert!(store
            .read_request(date, "req-1")
            .await
            .expect("read")
            .is_empty());
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
