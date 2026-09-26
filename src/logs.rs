//! Daemon-side log ring: the last N formatted log lines, plus a live
//! broadcast so `logs.follow` can stream them to any connected client
//! (the relay path included — the frames are ordinary event pushes).
//!
//! The ring is process-global and lazily built: `damond` installs the
//! tee writer at boot so console output is unchanged while the ring
//! fills; RPC handlers read whatever ring exists (an empty one when
//! the binary never logs through it — e.g. tests or the channel
//! bridges).

use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, LazyLock};

/// Ring capacity — a chatty daemon's last ~2000 lines, a few hundred
/// KB at worst.
pub const DEFAULT_CAP: usize = 2000;

pub struct LogRing {
    lines: parking_lot::Mutex<VecDeque<String>>,
    tx: tokio::sync::broadcast::Sender<String>,
    cap: usize,
}

impl LogRing {
    fn new(cap: usize) -> Self {
        Self {
            lines: parking_lot::Mutex::new(VecDeque::with_capacity(cap.min(1024))),
            tx: tokio::sync::broadcast::Sender::new(64),
            cap,
        }
    }

    /// Record one complete line. Oldest is evicted at capacity; live
    /// followers get it on the broadcast (no followers → ignored).
    pub fn push(&self, line: String) {
        if self.cap == 0 {
            return;
        }
        let mut q = self.lines.lock();
        if q.len() >= self.cap {
            q.pop_front();
        }
        q.push_back(line.clone());
        drop(q);
        let _ = self.tx.send(line);
    }

    /// The most recent `n` lines, oldest first.
    pub fn recent(&self, n: usize) -> Vec<String> {
        let q = self.lines.lock();
        let skip = q.len().saturating_sub(n);
        q.iter().skip(skip).cloned().collect()
    }

    /// Live follower feed — see `logs.follow`.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

static RING: LazyLock<Arc<LogRing>> = LazyLock::new(|| Arc::new(LogRing::new(DEFAULT_CAP)));

/// The process-wide ring — the tee writer and the RPC handlers share
/// it. A binary that never writes through the tee simply serves an
/// empty ring.
pub fn ring() -> Arc<LogRing> {
    RING.clone()
}

/// `MakeWriter` that tees formatted events to stdout AND the ring —
/// `damond`'s console output is unchanged, `logs.tail`/`logs.follow`
/// see the same lines. One writer instance per event: lines are
/// emitted as they complete (`\n`), so a single event never tears.
pub struct MakeTeeWriter {
    ring: Arc<LogRing>,
}

impl MakeTeeWriter {
    pub fn new(ring: Arc<LogRing>) -> Self {
        Self { ring }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeTeeWriter {
    type Writer = TeeWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TeeWriter {
            ring: self.ring.clone(),
            buf: Vec::new(),
            out: std::io::stdout(),
        }
    }
}

pub struct TeeWriter {
    ring: Arc<LogRing>,
    buf: Vec<u8>,
    out: std::io::Stdout,
}

impl Write for TeeWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.out.write_all(bytes)?;
        self.buf.extend_from_slice(bytes);
        while let Some(pos) = self.buf.iter().position(|&c| c == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=pos).collect();
            // Drop the trailing newline; carriage returns too.
            let mut end = line.len();
            while end > 0 && (line[end - 1] == b'\n' || line[end - 1] == b'\r') {
                end -= 1;
            }
            let s = String::from_utf8_lossy(&line[..end]).to_string();
            self.ring.push(s);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_evicts_at_capacity_and_reports_recent() {
        let ring = LogRing::new(3);
        for i in 0..5 {
            ring.push(format!("l{i}"));
        }
        assert_eq!(ring.recent(10), vec!["l2", "l3", "l4"]);
        assert_eq!(ring.recent(1), vec!["l4"]);
        assert!(ring.recent(0).is_empty());
    }

    #[test]
    fn tee_writer_emits_complete_lines_only() {
        let ring = Arc::new(LogRing::new(10));
        let mut w = TeeWriter {
            ring: ring.clone(),
            buf: Vec::new(),
            out: std::io::stdout(),
        };
        w.write_all(b"partial").unwrap();
        assert!(ring.recent(10).is_empty(), "no newline yet");
        w.write_all(b" line\nnext\n").unwrap();
        assert_eq!(ring.recent(10), vec!["partial line", "next"]);
    }
}
