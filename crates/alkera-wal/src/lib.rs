use alkera_schema::{MarketDataEnvelope, SchemaError};
use crc32c::crc32c;
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::path::Path;
use thiserror::Error;

const WAL_HEADER_MAGIC: u64 = 0x414C_4B45_5241_574C; // "ALKERAWL"
const WAL_VERSION: u32 = 1;
const CONTROL_PAGE_BYTES: usize = 64;
const FRAME_MAGIC: u32 = 0x4652_4D31; // "FRM1"
const WRAP_MAGIC: u32 = 0x5752_4150; // "WRAP"
const FRAME_HEADER_BYTES: usize = 32; // [magic:4][crc32c:4][recv_ts_ns:8][seq:8][payload_len:4][pad:4] (8-byte aligned)

#[inline]
const fn align_up_8(n: usize) -> usize {
    (n + 7) & !7
}

#[derive(Debug, Error)]
pub enum WalError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("schema serialization error: {0}")]
    Schema(#[from] SchemaError),
    #[error("invalid WAL capacity (minimum 4096 bytes): {0}")]
    InvalidCapacity(usize),
    #[error("corrupted WAL header magic")]
    CorruptedHeader,
    #[error("frame payload ({0} bytes) exceeds ring data capacity ({1} bytes)")]
    FrameTooLarge(usize, usize),
    #[error("WAL ring buffer full: uncommitted bytes ({0}) would be overwritten")]
    RingBufferFull(u64),
    #[error("CRC32C checksum mismatch at seq {seq}: expected {expected:#x}, observed {observed:#x}")]
    CrcMismatch {
        seq: u64,
        expected: u32,
        observed: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalFrame {
    pub seq: u64,
    pub recv_ts_ns: u64,
    pub envelope: MarketDataEnvelope,
}

/// Memory-mapped circular NVMe Write-Ahead Log (`MmapRingWal`).
/// Buffers incoming WebSocket envelopes on local disk prior to Redpanda acknowledgement,
/// surviving downstream broker maintenance or network partitions with zero dropped frames.
pub struct MmapRingWal {
    mmap: MmapMut,
    data_capacity: u64,
    write_pos: u64,
    committed_pos: u64,
    next_seq: u64,
}

impl MmapRingWal {
    /// Opens or initializes a memory-mapped circular WAL file at `path` with total size `total_bytes`.
    pub fn open<P: AsRef<Path>>(path: P, total_bytes: usize) -> Result<Self, WalError> {
        if total_bytes < 4096 {
            return Err(WalError::InvalidCapacity(total_bytes));
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let metadata = file.metadata()?;
        let is_new = metadata.len() == 0;
        if metadata.len() != total_bytes as u64 {
            file.set_len(total_bytes as u64)?;
        }

        let mut mmap = unsafe { MmapOptions::new().len(total_bytes).map_mut(&file)? };
        let data_capacity = (total_bytes - CONTROL_PAGE_BYTES) as u64;

        if is_new {
            Self::write_u64(&mut mmap, 0, WAL_HEADER_MAGIC);
            Self::write_u32(&mut mmap, 8, WAL_VERSION);
            Self::write_u64(&mut mmap, 16, data_capacity);
            Self::write_u64(&mut mmap, 24, 0); // write_pos
            Self::write_u64(&mut mmap, 32, 0); // committed_pos
            Self::write_u64(&mut mmap, 40, 1); // next_seq
            mmap.flush()?;
            Ok(Self {
                mmap,
                data_capacity,
                write_pos: 0,
                committed_pos: 0,
                next_seq: 1,
            })
        } else {
            let magic = Self::read_u64(&mmap, 0);
            if magic != WAL_HEADER_MAGIC {
                return Err(WalError::CorruptedHeader);
            }
            let write_pos = Self::read_u64(&mmap, 24);
            let committed_pos = Self::read_u64(&mmap, 32);
            let next_seq = Self::read_u64(&mmap, 40);
            Ok(Self {
                mmap,
                data_capacity,
                write_pos,
                committed_pos,
                next_seq,
            })
        }
    }

    /// Appends a `MarketDataEnvelope` to the circular WAL with a CRC32C checksum header.
    /// Returns the monotonic WAL sequence number assigned to the frame.
    pub fn append(&mut self, envelope: &MarketDataEnvelope) -> Result<u64, WalError> {
        let payload = envelope.to_rkyv_bytes()?;
        let aligned_payload_len = align_up_8(payload.len());
        let frame_len = (FRAME_HEADER_BYTES + aligned_payload_len) as u64;
        if frame_len + 8 > self.data_capacity {
            return Err(WalError::FrameTooLarge(
                frame_len as usize,
                self.data_capacity as usize,
            ));
        }

        let uncommitted_bytes = self.write_pos.saturating_sub(self.committed_pos);
        if uncommitted_bytes + frame_len + 8 > self.data_capacity {
            return Err(WalError::RingBufferFull(uncommitted_bytes));
        }

        let mut phys_offset = (self.write_pos % self.data_capacity) as usize;
        let remaining_at_tail = (self.data_capacity as usize) - phys_offset;

        // If the frame does not fit contiguously before the end of the file, write a WRAP marker
        // and advance `write_pos` to the start of the data region.
        if remaining_at_tail < frame_len as usize {
            if remaining_at_tail >= 4 {
                Self::write_u32(
                    &mut self.mmap,
                    CONTROL_PAGE_BYTES + phys_offset,
                    WRAP_MAGIC,
                );
            }
            let wrap_pad = remaining_at_tail as u64;
            if uncommitted_bytes + wrap_pad + frame_len > self.data_capacity {
                return Err(WalError::RingBufferFull(uncommitted_bytes));
            }
            self.write_pos += wrap_pad;
            phys_offset = 0;
        }

        let seq = self.next_seq;
        let recv_ts_ns = envelope.recv_ts_ns();
        let checksum = crc32c(&payload);

        let base = CONTROL_PAGE_BYTES + phys_offset;
        Self::write_u32(&mut self.mmap, base, FRAME_MAGIC);
        Self::write_u32(&mut self.mmap, base + 4, checksum);
        Self::write_u64(&mut self.mmap, base + 8, recv_ts_ns);
        Self::write_u64(&mut self.mmap, base + 16, seq);
        Self::write_u32(&mut self.mmap, base + 24, payload.len() as u32);
        Self::write_u32(&mut self.mmap, base + 28, 0);
        self.mmap[base + FRAME_HEADER_BYTES..base + FRAME_HEADER_BYTES + payload.len()]
            .copy_from_slice(&payload);

        self.write_pos += frame_len;
        self.next_seq += 1;

        Self::write_u64(&mut self.mmap, 24, self.write_pos);
        Self::write_u64(&mut self.mmap, 40, self.next_seq);
        Ok(seq)
    }

    /// Marks all frames up to and including `acked_seq` as durably committed to Redpanda,
    /// freeing their ring space for reuse.
    pub fn commit_up_to_seq(&mut self, acked_seq: u64) -> Result<usize, WalError> {
        let mut cursor = self.committed_pos;
        let mut freed_frames = 0;

        while cursor < self.write_pos {
            let phys_offset = (cursor % self.data_capacity) as usize;
            let remaining_at_tail = (self.data_capacity as usize) - phys_offset;
            if remaining_at_tail < FRAME_HEADER_BYTES {
                cursor += remaining_at_tail as u64;
                continue;
            }
            let base = CONTROL_PAGE_BYTES + phys_offset;
            let magic = Self::read_u32(&self.mmap, base);
            if magic == WRAP_MAGIC {
                cursor += remaining_at_tail as u64;
                continue;
            }
            if magic != FRAME_MAGIC {
                break;
            }
            let seq = Self::read_u64(&self.mmap, base + 16);
            let payload_len = Self::read_u32(&self.mmap, base + 24) as usize;
            let frame_len = (FRAME_HEADER_BYTES + align_up_8(payload_len)) as u64;
            if seq <= acked_seq {
                cursor += frame_len;
                freed_frames += 1;
            } else {
                break;
            }
        }

        self.committed_pos = cursor;
        Self::write_u64(&mut self.mmap, 32, self.committed_pos);
        Ok(freed_frames)
    }

    /// Replays all uncommitted frames currently buffered in the WAL, verifying every CRC32C checksum.
    pub fn replay_uncommitted(&self) -> Result<Vec<WalFrame>, WalError> {
        let mut cursor = self.committed_pos;
        let mut frames = Vec::new();

        while cursor < self.write_pos {
            let phys_offset = (cursor % self.data_capacity) as usize;
            let remaining_at_tail = (self.data_capacity as usize) - phys_offset;
            if remaining_at_tail < FRAME_HEADER_BYTES {
                cursor += remaining_at_tail as u64;
                continue;
            }
            let base = CONTROL_PAGE_BYTES + phys_offset;
            let magic = Self::read_u32(&self.mmap, base);
            if magic == WRAP_MAGIC {
                cursor += remaining_at_tail as u64;
                continue;
            }
            if magic != FRAME_MAGIC {
                return Err(WalError::CorruptedHeader);
            }

            let expected_crc = Self::read_u32(&self.mmap, base + 4);
            let recv_ts_ns = Self::read_u64(&self.mmap, base + 8);
            let seq = Self::read_u64(&self.mmap, base + 16);
            let payload_len = Self::read_u32(&self.mmap, base + 24) as usize;

            let payload_slice =
                &self.mmap[base + FRAME_HEADER_BYTES..base + FRAME_HEADER_BYTES + payload_len];
            let observed_crc = crc32c(payload_slice);
            if expected_crc != observed_crc {
                return Err(WalError::CrcMismatch {
                    seq,
                    expected: expected_crc,
                    observed: observed_crc,
                });
            }

            let envelope = MarketDataEnvelope::from_rkyv_bytes(payload_slice)?;
            frames.push(WalFrame {
                seq,
                recv_ts_ns,
                envelope,
            });
            cursor += (FRAME_HEADER_BYTES + align_up_8(payload_len)) as u64;
        }

        Ok(frames)
    }

    #[inline]
    pub fn uncommitted_bytes(&self) -> u64 {
        self.write_pos.saturating_sub(self.committed_pos)
    }

    pub fn flush(&self) -> Result<(), WalError> {
        self.mmap.flush()?;
        Ok(())
    }

    #[inline]
    fn write_u32(buf: &mut [u8], offset: usize, val: u32) {
        buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
    }

    #[inline]
    fn write_u64(buf: &mut [u8], offset: usize, val: u64) {
        buf[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
    }

    #[inline]
    fn read_u32(buf: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap())
    }

    #[inline]
    fn read_u64(buf: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alkera_schema::{PriceMicros, TakerSide, TradeEvent, VenueId};
    use tempfile::NamedTempFile;

    fn sample_trade(seq_idx: u64) -> MarketDataEnvelope {
        let trade_id = format!("trade-{seq_idx}");
        let dedup = TradeEvent::compute_dedup_key(
            VenueId::Kalshi,
            101,
            1_792_273_000_000 + seq_idx,
            &trade_id,
            "",
            PriceMicros(650_000),
            10_000_000,
            TakerSide::Yes,
        );
        MarketDataEnvelope::Trade(TradeEvent {
            venue_id: VenueId::Kalshi,
            instrument_iid: 101,
            market_uid: 50,
            exchange_ts_ms: 1_792_273_000_000 + seq_idx,
            sending_ts_ms: 1_792_273_000_000 + seq_idx,
            recv_ts_ns: 1_792_273_000_000_000_000 + seq_idx * 1_000,
            leg_a_recv_ns: 1_792_273_000_000_000_000 + seq_idx * 1_000,
            leg_b_recv_ns: 0,
            trade_id,
            tx_hash: String::new(),
            price_micros: PriceMicros(650_000),
            complement_price_micros: PriceMicros(350_000),
            size_micros: 10_000_000,
            taker_side: TakerSide::Yes,
            fee_rate_bps: 0,
            is_block_trade: false,
            dedup_key: dedup,
        })
    }

    #[test]
    fn test_wal_circular_wrap_and_crash_recovery() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut wal = MmapRingWal::open(tmp.path(), 8192).unwrap();
            for i in 1..=30 {
                let seq = wal.append(&sample_trade(i)).unwrap();
                assert_eq!(seq, i);
            }
            // Commit first 20 frames so ring space can wrap around
            let freed = wal.commit_up_to_seq(20).unwrap();
            assert_eq!(freed, 20);

            // Append 25 more frames, forcing wrap-around across the 8KB boundary
            for i in 31..=55 {
                let seq = wal.append(&sample_trade(i)).unwrap();
                assert_eq!(seq, i);
            }
            wal.flush().unwrap();
        }

        // Reopen WAL after simulated collector restart and verify uncommitted frames 21..=55
        let wal_reopened = MmapRingWal::open(tmp.path(), 8192).unwrap();
        let replayed = wal_reopened.replay_uncommitted().unwrap();
        assert_eq!(replayed.len(), 35);
        assert_eq!(replayed.first().unwrap().seq, 21);
        assert_eq!(replayed.last().unwrap().seq, 55);
        assert_eq!(replayed.first().unwrap().envelope, sample_trade(21));
        assert_eq!(replayed.last().unwrap().envelope, sample_trade(55));
    }

    #[test]
    fn test_wal_ring_buffer_full_and_crc32c_corruption_detection() {
        let tmp = NamedTempFile::new().unwrap();
        let mut wal = MmapRingWal::open(tmp.path(), 4096).unwrap();

        // Fill WAL until RingBufferFull is returned without overwriting uncommitted frames
        let mut appended = 0;
        loop {
            match wal.append(&sample_trade(appended + 1)) {
                Ok(_) => appended += 1,
                Err(WalError::RingBufferFull(_)) => break,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        assert!(appended > 5);
        assert_eq!(wal.replay_uncommitted().unwrap().len() as u64, appended);

        // Corrupt a payload byte inside the first frame and verify CRC32C mismatch detection
        wal.mmap[CONTROL_PAGE_BYTES + FRAME_HEADER_BYTES + 4] ^= 0xFF;
        let err = wal.replay_uncommitted().unwrap_err();
        assert!(matches!(err, WalError::CrcMismatch { seq: 1, .. }));
    }
}
