use std::{
    collections::{BTreeMap, VecDeque},
    ffi::CString,
    fs::{self, File},
    io::{BufRead, BufReader, Lines, Read, Seek, SeekFrom},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::MetadataExt,
    },
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::Engine;
use chrono::Utc;
use serde::Serialize;
use tracing::{debug, warn};

use crate::{
    logging::LogConfig,
    model::{EncodedData, OutputEvent},
    store::{append_line, append_raw},
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

impl OutputState {
    fn remember(&mut self, event: OutputEvent) {
        self.recent.push_back(event);
        while self.recent.len() > LIVE_EVENT_LIMIT {
            self.recent.pop_front();
        }
    }
}

impl OutputLog {
    pub fn open(process_dir: impl Into<PathBuf>, config: LogConfig) -> Result<(Self, OutputState)> {
        let this = Self {
            process_dir: process_dir.into(),
            config,
        };
        this.migrate_legacy()?;
        fs::create_dir_all(this.current_dir())?;
        let state = this.scan_state()?;
        Ok((this, state))
    }

    pub fn append(
        &self,
        state: &mut OutputState,
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
            self.rotate()?;
            state.offsets.clear();
            state.active_size = 0;
            offset = 0;
            event.offset = 0;
            line = serde_json::to_vec(&event)?;
        }
        let current = self.current_dir();
        append_raw(&current.join(format!("{stream}.log")), bytes)?;
        append_line(&current.join("events.jsonl"), &line)?;
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

    #[test]
    fn rotates_generations_and_reports_gap() {
        let temp = tempfile::tempdir().unwrap();
        let (log, mut state) = OutputLog::open(
            temp.path(),
            LogConfig {
                rotate_size: 220,
                backup_count: 2,
            },
        )
        .unwrap();
        for _ in 0..8 {
            log.append(&mut state, "stdout", b"abcdefghij", 1).unwrap();
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
        let (_, state) = OutputLog::open(
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
}
