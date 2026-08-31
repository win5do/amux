use std::{
    collections::{BTreeMap, VecDeque},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Lines, Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use base64::Engine;
use chrono::Utc;
use serde::Serialize;
use tracing::{debug, warn};

use crate::{
    logging::LogConfig,
    model::{EncodedData, OutputEvent},
};

pub const LIVE_EVENT_LIMIT: usize = 4096;

#[derive(Debug, Clone, Serialize)]
pub struct GapEvent {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub requested_after: u64,
    pub oldest_available: u64,
}

pub enum LogRecord {
    Gap(GapEvent),
    Output(OutputEvent),
}

pub struct RecordReader {
    generations: VecDeque<GenerationReader>,
    after: u64,
    through: Option<u64>,
    gap: Option<GapEvent>,
}

struct GenerationReader {
    lines: Lines<BufReader<File>>,
    raw: BTreeMap<String, File>,
    oldest: Option<u64>,
}

impl RecordReader {
    pub fn open(process_dir: &Path, after: u64, through: Option<u64>) -> Result<Self> {
        Self::open_internal(process_dir, after, through, true)
    }

    fn open_internal(
        process_dir: &Path,
        after: u64,
        through: Option<u64>,
        report_gap: bool,
    ) -> Result<Self> {
        let generations = open_generation_snapshot(process_dir)?;
        let oldest = generations.iter().find_map(|generation| generation.oldest);
        let gap = report_gap
            .then_some(oldest)
            .flatten()
            .filter(|oldest| after.saturating_add(1) < *oldest)
            .map(|oldest_available| GapEvent {
                kind: "gap",
                requested_after: after,
                oldest_available,
            });
        if let Some(gap) = &gap {
            warn!(
                requested_after = gap.requested_after,
                oldest_available = gap.oldest_available,
                "output history gap"
            );
        }
        Ok(Self {
            generations: generations.into(),
            after,
            through,
            gap,
        })
    }
}

impl Iterator for RecordReader {
    type Item = Result<LogRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(gap) = self.gap.take() {
            return Some(Ok(LogRecord::Gap(gap)));
        }
        loop {
            let generation = self.generations.front_mut()?;
            match generation.lines.next() {
                Some(Ok(line)) => {
                    let Ok(event) = serde_json::from_str::<OutputEvent>(&line) else {
                        continue;
                    };
                    if event.sequence <= self.after
                        || self.through.is_some_and(|end| event.sequence > end)
                    {
                        continue;
                    }
                    return Some(generation.event_with_data(event).map(LogRecord::Output));
                }
                Some(Err(error)) => return Some(Err(error.into())),
                None => {
                    self.generations.pop_front();
                }
            }
        }
    }
}

impl GenerationReader {
    fn open(dir: &Path) -> Result<Option<Self>> {
        let directory = File::open(dir)?;
        let Some(index) = open_at(&directory, "events.jsonl")? else {
            return Ok(None);
        };
        let oldest = open_at(&directory, "events.jsonl")?.and_then(|file| {
            BufReader::new(file)
                .lines()
                .map_while(std::io::Result::ok)
                .find_map(|line| serde_json::from_str::<OutputEvent>(&line).ok())
                .map(|event| event.sequence)
        });
        let mut raw = BTreeMap::new();
        for stream in ["stdout", "stderr", "tty"] {
            if let Some(file) = open_at(&directory, &format!("{stream}.log"))? {
                raw.insert(stream.to_owned(), file);
            }
        }
        Ok(Some(Self {
            lines: BufReader::new(index).lines(),
            raw,
            oldest,
        }))
    }

    fn event_with_data(&mut self, mut event: OutputEvent) -> Result<OutputEvent> {
        let file = self
            .raw
            .get_mut(&event.stream)
            .with_context(|| format!("missing raw stream {}", event.stream))?;
        file.seek(SeekFrom::Start(event.offset as u64))?;
        let mut bytes = vec![0; event.length];
        file.read_exact(&mut bytes)?;
        event.data = Some(encode_data(bytes));
        Ok(event)
    }
}

fn open_generation_snapshot(process_dir: &Path) -> Result<Vec<GenerationReader>> {
    for _ in 0..8 {
        let paths = generation_dirs(process_dir)?;
        let before = generation_identities(&paths);
        let mut readers = Vec::new();
        let mut stable = true;
        for path in &paths {
            match GenerationReader::open(path) {
                Ok(Some(reader)) => readers.push(reader),
                Ok(None) => {}
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                {
                    stable = false;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        let after_paths = generation_dirs(process_dir)?;
        if stable && before == generation_identities(&after_paths) {
            return Ok(readers);
        }
    }
    anyhow::bail!("output generations changed repeatedly while opening logs")
}

fn generation_identities(paths: &[PathBuf]) -> Vec<(PathBuf, u64, u64)> {
    paths
        .iter()
        .filter_map(|path| {
            let metadata = fs::metadata(path).ok()?;
            Some((path.clone(), metadata.dev(), metadata.ino()))
        })
        .collect()
}

fn open_at(directory: &File, name: &str) -> Result<Option<File>> {
    let name = CString::new(name)?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC,
        )
    };
    if fd >= 0 {
        return Ok(Some(unsafe { File::from_raw_fd(fd) }));
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(None)
    } else {
        Err(error.into())
    }
}

impl LogRecord {
    pub fn to_json_line(&self) -> Result<Vec<u8>> {
        let mut line = match self {
            Self::Gap(value) => serde_json::to_vec(value)?,
            Self::Output(value) => serde_json::to_vec(value)?,
        };
        line.push(b'\n');
        Ok(line)
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Gap(_) => None,
            Self::Output(value) => Some(value.sequence),
        }
    }
}

#[derive(Clone)]
pub struct OutputLog {
    process_dir: PathBuf,
    config: LogConfig,
}

pub struct OutputState {
    pub sequence: u64,
    pub offsets: BTreeMap<String, i64>,
    pub active_size: u64,
    pub recent: VecDeque<OutputEvent>,
}

pub struct OutputFiles {
    current_dir: PathBuf,
    streams: BTreeMap<String, File>,
    index: File,
    dirty: bool,
}

impl OutputFiles {
    fn open(current_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&current_dir)?;
        let index = open_append(&current_dir.join("events.jsonl"))?;
        Ok(Self {
            current_dir,
            streams: BTreeMap::new(),
            index,
            dirty: false,
        })
    }

    fn append(&mut self, stream: &str, offset: i64, bytes: &[u8], line: &[u8]) -> Result<()> {
        let path = self.current_dir.join(format!("{stream}.log"));
        let raw = match self.streams.entry(stream.to_owned()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(open_append(&path)?),
        };
        let raw_start = raw.metadata()?.len();
        if offset < 0 || raw_start != offset as u64 {
            bail!(
                "output stream offset mismatch for {stream}: expected {offset}, found {raw_start}"
            );
        }
        let index_start = self.index.metadata()?.len();
        if let Err(error) = raw
            .write_all(bytes)
            .and_then(|_| self.index.write_all(line))
            .and_then(|_| self.index.write_all(b"\n"))
        {
            let _ = raw.set_len(raw_start);
            let _ = self.index.set_len(index_start);
            return Err(error.into());
        }
        self.dirty = true;
        Ok(())
    }

    pub fn sync_data(&mut self) -> Result<bool> {
        if !self.dirty {
            return Ok(false);
        }
        for file in self.streams.values() {
            file.sync_data()?;
        }
        self.index.sync_data()?;
        self.dirty = false;
        Ok(true)
    }

    fn reopen(&mut self, current_dir: PathBuf) -> Result<()> {
        *self = Self::open(current_dir)?;
        Ok(())
    }
}

impl OutputState {
    fn remember(&mut self, event: OutputEvent) {
        self.recent.push_back(event);
        while self.recent.len() > LIVE_EVENT_LIMIT {
            self.recent.pop_front();
        }
    }
}

impl OutputLog {
    pub fn open(
        process_dir: impl Into<PathBuf>,
        config: LogConfig,
    ) -> Result<(Self, OutputState, OutputFiles)> {
        let this = Self {
            process_dir: process_dir.into(),
            config,
        };
        this.migrate_legacy()?;
        fs::create_dir_all(this.current_dir())?;
        this.repair_current_generation()?;
        let state = this.scan_state()?;
        let files = OutputFiles::open(this.current_dir())?;
        Ok((this, state, files))
    }

    pub fn append(
        &self,
        state: &mut OutputState,
        files: &mut OutputFiles,
        stream: &str,
        bytes: &[u8],
        execution: u64,
    ) -> Result<OutputEvent> {
        let mut offset = *state.offsets.get(stream).unwrap_or(&0);
        let mut event = OutputEvent {
            sequence: state.sequence + 1,
            execution,
            timestamp: Utc::now(),
            stream: stream.to_owned(),
            offset,
            length: bytes.len(),
            data: None,
        };
        let mut line = serde_json::to_vec(&event)?;
        let addition = bytes.len() as u64 + line.len() as u64 + 1;
        if state.active_size > 0
            && state.active_size.saturating_add(addition) > self.config.rotate_size
        {
            files.sync_data()?;
            self.rotate()?;
            files.reopen(self.current_dir())?;
            state.offsets.clear();
            state.active_size = 0;
            offset = 0;
            event.offset = 0;
            line = serde_json::to_vec(&event)?;
        }
        files.append(stream, offset, bytes, &line)?;
        state.sequence = event.sequence;
        state
            .offsets
            .insert(stream.to_owned(), offset + bytes.len() as i64);
        state.active_size = state
            .active_size
            .saturating_add(bytes.len() as u64 + line.len() as u64 + 1);
        state.remember(event.clone());
        Ok(event)
    }

    pub fn process_dir(&self) -> &Path {
        &self.process_dir
    }

    pub fn replay_tail(&self, stream: &str, limit: usize) -> Result<VecDeque<u8>> {
        let mut output = VecDeque::new();
        for generation in generation_dirs(&self.process_dir)?.into_iter().rev() {
            if output.len() >= limit {
                break;
            }
            let path = generation.join(format!("{stream}.log"));
            let Ok(mut file) = File::open(path) else {
                continue;
            };
            let length = file.metadata()?.len() as usize;
            let count = (limit - output.len()).min(length);
            file.seek(SeekFrom::End(-(count as i64)))?;
            let mut bytes = vec![0; count];
            file.read_exact(&mut bytes)?;
            for byte in bytes.into_iter().rev() {
                output.push_front(byte);
            }
        }
        Ok(output)
    }

    pub fn replay_event_tail(
        &self,
        stream: &str,
        limit: usize,
        through: u64,
    ) -> Result<VecDeque<OutputEvent>> {
        let mut output = VecDeque::new();
        if limit == 0 {
            return Ok(output);
        }
        let mut size = 0usize;
        for record in RecordReader::open_internal(&self.process_dir, 0, Some(through), false)? {
            let LogRecord::Output(event) = record? else {
                continue;
            };
            if event.stream != stream {
                continue;
            }
            size = size.saturating_add(event.length);
            output.push_back(event);
            while output.len() > 1 && size > limit {
                if let Some(removed) = output.pop_front() {
                    size = size.saturating_sub(removed.length);
                }
            }
        }
        Ok(output)
    }

    fn current_dir(&self) -> PathBuf {
        self.process_dir.join("output/current")
    }

    fn repair_current_generation(&self) -> Result<()> {
        let current = self.current_dir();
        fs::create_dir_all(&current)?;
        let index_path = current.join("events.jsonl");
        let index = fs::read(&index_path).unwrap_or_default();
        let mut valid_index_len = 0usize;
        let mut expected_offsets = BTreeMap::<String, u64>::new();
        let mut previous_sequence = None;

        for encoded_line in index.split_inclusive(|byte| *byte == b'\n') {
            if encoded_line.last() != Some(&b'\n') {
                break;
            }
            let Ok(event) = serde_json::from_slice::<OutputEvent>(
                &encoded_line[..encoded_line.len().saturating_sub(1)],
            ) else {
                break;
            };
            if !matches!(event.stream.as_str(), "stdout" | "stderr" | "tty")
                || event.offset < 0
                || previous_sequence.is_some_and(|sequence| event.sequence != sequence + 1)
            {
                break;
            }
            let expected = *expected_offsets.get(&event.stream).unwrap_or(&0);
            let Some(end) = (event.offset as u64).checked_add(event.length as u64) else {
                break;
            };
            let raw_length = fs::metadata(current.join(format!("{}.log", event.stream)))
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            if event.offset as u64 != expected || end > raw_length {
                break;
            }
            expected_offsets.insert(event.stream.clone(), end);
            previous_sequence = Some(event.sequence);
            valid_index_len += encoded_line.len();
        }

        let mut repaired = valid_index_len != index.len();
        if repaired {
            open_append(&index_path)?.set_len(valid_index_len as u64)?;
        }
        for stream in ["stdout", "stderr", "tty"] {
            let path = current.join(format!("{stream}.log"));
            let Ok(metadata) = fs::metadata(&path) else {
                continue;
            };
            let expected = *expected_offsets.get(stream).unwrap_or(&0);
            if metadata.len() > expected {
                open_append(&path)?.set_len(expected)?;
                repaired = true;
            }
        }
        if repaired {
            warn!(process_dir = %self.process_dir.display(), "repaired incomplete output generation tail");
        }
        Ok(())
    }

    fn scan_state(&self) -> Result<OutputState> {
        let mut sequence = 0;
        let mut recent = VecDeque::new();
        for generation in generation_dirs(&self.process_dir)? {
            visit_index(&generation, |event| {
                sequence = sequence.max(event.sequence);
                recent.push_back(event);
                while recent.len() > LIVE_EVENT_LIMIT {
                    recent.pop_front();
                }
                Ok(())
            })?;
        }
        let mut offsets = BTreeMap::new();
        visit_index(&self.current_dir(), |event| {
            let end = event.offset + event.length as i64;
            offsets
                .entry(event.stream)
                .and_modify(|value: &mut i64| *value = (*value).max(end))
                .or_insert(end);
            Ok(())
        })?;
        Ok(OutputState {
            sequence,
            offsets,
            active_size: directory_file_size(&self.current_dir())?,
            recent,
        })
    }

    fn rotate(&self) -> Result<()> {
        let current = self.current_dir();
        if self.config.backup_count == 0 {
            if current.exists() {
                fs::remove_dir_all(&current)?;
            }
            fs::create_dir_all(&current)?;
            debug!(process_dir = %self.process_dir.display(), "output generation discarded and recreated");
            return Ok(());
        }
        let backup = unique_generation_path(&self.process_dir.join("output"));
        fs::rename(&current, backup)?;
        fs::create_dir_all(&current)?;
        debug!(process_dir = %self.process_dir.display(), "output generation rotated");
        prune_generations(&self.process_dir.join("output"), self.config.backup_count)
    }

    fn migrate_legacy(&self) -> Result<()> {
        let output = self.process_dir.join("output");
        fs::create_dir_all(&output)?;
        let mut partials = fs::read_dir(&output)?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".migrate-"))
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        partials.sort();
        for partial in partials {
            let destination = if !self.current_dir().exists() {
                self.current_dir()
            } else {
                unique_generation_path(&output)
            };
            fs::rename(partial, destination)?;
        }
        if !self.process_dir.join("events.jsonl").exists() {
            fs::create_dir_all(self.current_dir())?;
            return Ok(());
        }
        debug!(process_dir = %self.process_dir.display(), "migrating legacy output layout");
        let staging = output.join(format!(".migrate-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir(&staging)?;
        for entry in fs::read_dir(&self.process_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name_text = name.to_string_lossy();
            if name_text == "events.jsonl" || name_text.ends_with(".log") {
                fs::rename(entry.path(), staging.join(name))?;
            }
        }
        let destination = if directory_file_size(&self.current_dir()).unwrap_or(0) == 0 {
            if self.current_dir().exists() {
                fs::remove_dir(self.current_dir())?;
            }
            self.current_dir()
        } else {
            unique_generation_path(&output)
        };
        fs::rename(staging, destination)?;
        prune_generations(&output, self.config.backup_count)?;
        fs::create_dir_all(self.current_dir())?;
        Ok(())
    }
}

fn open_append(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?)
}

pub fn visit_records(
    process_dir: &Path,
    after: u64,
    through: Option<u64>,
    mut visitor: impl FnMut(LogRecord) -> Result<()>,
) -> Result<u64> {
    let mut cursor = after;
    for record in RecordReader::open(process_dir, after, through)? {
        let record = record?;
        if let Some(sequence) = record.sequence() {
            cursor = cursor.max(sequence);
        }
        visitor(record)?;
    }
    Ok(cursor)
}

pub fn generation_dirs(process_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    if process_dir.join("events.jsonl").exists() {
        dirs.push(process_dir.to_owned());
    }
    let output = process_dir.join("output");
    if let Ok(entries) = fs::read_dir(&output) {
        let mut backups = entries
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && entry.file_name().to_string_lossy().ends_with(".bak")
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        backups.sort();
        dirs.extend(backups);
    }
    if output.join("current").exists() {
        dirs.push(output.join("current"));
    }
    Ok(dirs)
}

fn visit_index(dir: &Path, mut visitor: impl FnMut(OutputEvent) -> Result<()>) -> Result<()> {
    let Ok(file) = File::open(dir.join("events.jsonl")) else {
        return Ok(());
    };
    for line in BufReader::new(file).lines().map_while(std::io::Result::ok) {
        if let Ok(event) = serde_json::from_str::<OutputEvent>(&line) {
            visitor(event)?;
        }
    }
    Ok(())
}

fn encode_data(bytes: Vec<u8>) -> EncodedData {
    match String::from_utf8(bytes.clone()) {
        Ok(text) => EncodedData {
            encoding: "utf8".into(),
            text,
            base64: String::new(),
        },
        Err(_) => EncodedData {
            encoding: "base64".into(),
            text: String::new(),
            base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        },
    }
}

fn unique_generation_path(output: &Path) -> PathBuf {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%S%.9fZ");
    for suffix in 0_u32.. {
        let extra = if suffix == 0 {
            String::new()
        } else {
            format!("-{suffix}")
        };
        let candidate = output.join(format!("{timestamp}{extra}.bak"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!()
}

fn prune_generations(output: &Path, keep: usize) -> Result<()> {
    let mut backups = fs::read_dir(output)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_dir())
                && entry.file_name().to_string_lossy().ends_with(".bak")
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    backups.sort();
    let remove = backups.len().saturating_sub(keep);
    for backup in backups.into_iter().take(remove) {
        fs::remove_dir_all(backup)?;
    }
    Ok(())
}

fn directory_file_size(dir: &Path) -> Result<u64> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{append_line, append_raw};

    #[test]
    fn rotates_generations_and_reports_gap() {
        let temp = tempfile::tempdir().unwrap();
        let (log, mut state, mut files) = OutputLog::open(
            temp.path(),
            LogConfig {
                rotate_size: 220,
                backup_count: 2,
            },
        )
        .unwrap();
        for _ in 0..8 {
            log.append(&mut state, &mut files, "stdout", b"abcdefghij", 1)
                .unwrap();
        }
        let backups = fs::read_dir(temp.path().join("output"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".bak"))
            .count();
        assert_eq!(backups, 2);
        let mut records = Vec::new();
        visit_records(temp.path(), 0, None, |record| {
            records.push(record.to_json_line().unwrap());
            Ok(())
        })
        .unwrap();
        assert!(String::from_utf8_lossy(&records[0]).contains("\"type\":\"gap\""));
        assert!(
            records
                .iter()
                .any(|line| String::from_utf8_lossy(line).contains("abcdefghij"))
        );
    }

    #[test]
    fn migrates_legacy_layout() {
        let temp = tempfile::tempdir().unwrap();
        append_raw(&temp.path().join("stdout.log"), b"old").unwrap();
        let event = OutputEvent {
            sequence: 4,
            execution: 1,
            timestamp: Utc::now(),
            stream: "stdout".into(),
            offset: 0,
            length: 3,
            data: None,
        };
        append_line(
            &temp.path().join("events.jsonl"),
            &serde_json::to_vec(&event).unwrap(),
        )
        .unwrap();
        let (_, state, _) = OutputLog::open(
            temp.path(),
            LogConfig {
                rotate_size: 1024,
                backup_count: 2,
            },
        )
        .unwrap();
        assert_eq!(state.sequence, 4);
        assert!(temp.path().join("output/current/events.jsonl").exists());
    }

    #[test]
    fn live_event_cache_is_bounded() {
        let mut state = OutputState {
            sequence: 0,
            offsets: BTreeMap::new(),
            active_size: 0,
            recent: VecDeque::new(),
        };
        for sequence in 1..=(LIVE_EVENT_LIMIT as u64 + 32) {
            state.remember(OutputEvent {
                sequence,
                execution: 1,
                timestamp: Utc::now(),
                stream: "stdout".into(),
                offset: 0,
                length: 1,
                data: None,
            });
        }
        assert_eq!(state.recent.len(), LIVE_EVENT_LIMIT);
        assert_eq!(state.recent.front().unwrap().sequence, 33);
    }

    #[test]
    fn appends_are_readable_before_batched_sync() {
        let temp = tempfile::tempdir().unwrap();
        let (log, mut state, mut files) = OutputLog::open(
            temp.path(),
            LogConfig {
                rotate_size: 1024,
                backup_count: 2,
            },
        )
        .unwrap();
        log.append(&mut state, &mut files, "tty", b"a", 1).unwrap();
        log.append(&mut state, &mut files, "tty", b"b", 1).unwrap();

        assert!(files.dirty);
        assert_eq!(fs::read(log.current_dir().join("tty.log")).unwrap(), b"ab");
        let records = RecordReader::open(temp.path(), 0, None)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(records.len(), 2);
        assert!(files.sync_data().unwrap());
        assert!(!files.sync_data().unwrap());
    }

    #[test]
    fn repairs_incomplete_index_and_orphan_raw_tail() {
        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join("output/current");
        fs::create_dir_all(&current).unwrap();
        fs::write(current.join("tty.log"), b"ok-orphan").unwrap();
        let event = OutputEvent {
            sequence: 7,
            execution: 1,
            timestamp: Utc::now(),
            stream: "tty".into(),
            offset: 0,
            length: 2,
            data: None,
        };
        let mut index = serde_json::to_vec(&event).unwrap();
        index.push(b'\n');
        index.extend_from_slice(b"{\"sequence\":8");
        fs::write(current.join("events.jsonl"), &index).unwrap();

        let (_, state, _) = OutputLog::open(
            temp.path(),
            LogConfig {
                rotate_size: 1024,
                backup_count: 2,
            },
        )
        .unwrap();

        assert_eq!(state.sequence, 7);
        assert_eq!(fs::read(current.join("tty.log")).unwrap(), b"ok");
        assert_eq!(
            fs::read(current.join("events.jsonl")).unwrap(),
            serde_json::to_vec(&event)
                .unwrap()
                .into_iter()
                .chain([b'\n'])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn repairs_index_event_that_extends_past_raw_data() {
        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join("output/current");
        fs::create_dir_all(&current).unwrap();
        fs::write(current.join("stdout.log"), b"ok").unwrap();
        let valid = OutputEvent {
            sequence: 4,
            execution: 1,
            timestamp: Utc::now(),
            stream: "stdout".into(),
            offset: 0,
            length: 2,
            data: None,
        };
        let invalid = OutputEvent {
            sequence: 5,
            execution: 1,
            timestamp: Utc::now(),
            stream: "stdout".into(),
            offset: 2,
            length: 1,
            data: None,
        };
        let mut index = serde_json::to_vec(&valid).unwrap();
        index.push(b'\n');
        index.extend(serde_json::to_vec(&invalid).unwrap());
        index.push(b'\n');
        fs::write(current.join("events.jsonl"), index).unwrap();

        let (_, state, _) = OutputLog::open(
            temp.path(),
            LogConfig {
                rotate_size: 1024,
                backup_count: 2,
            },
        )
        .unwrap();

        assert_eq!(state.sequence, 4);
        assert_eq!(
            fs::read(current.join("events.jsonl")).unwrap(),
            serde_json::to_vec(&valid)
                .unwrap()
                .into_iter()
                .chain([b'\n'])
                .collect::<Vec<_>>()
        );
    }
}
