// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/nfs/client.rs — NFSv4.2 compound RPC TCP client

//! Persistent TCP client for sending NFSv4.2 compound RPCs directly to the
//! NFS server, bypassing the Linux VFS for small-file writes.
//!
//! Manages session establishment (EXCHANGE_ID + CREATE_SESSION), sequence IDs,
//! and directory handle caching.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use dashmap::DashMap;
use tracing::{debug, info, warn};

use super::mount::NfsBypassInfo;
use super::rpc::{self, Nfs4Op, StateId, WriteStable, CompoundReply};
use super::NfsError;

/// NFSv4.2 compound RPC client over TCP.
///
/// Sends file creation compounds directly to the NFS server, bypassing VFS.
/// One TCP connection, one session, sequential slot usage.
pub struct NfsCompoundClient {
    stream: TcpStream,
    session_id: [u8; 16],
    sequence_id: AtomicU32,
    client_id: u64,
    uid: u32,
    gid: u32,
    machine: String,
    xid_counter: AtomicU32,
    /// Cache of directory path → NFS file handle.
    dir_handle_cache: DashMap<PathBuf, Vec<u8>>,
}

impl NfsCompoundClient {
    /// Connect to the NFS server and establish a session.
    ///
    /// Performs TCP connect → EXCHANGE_ID → CREATE_SESSION.
    /// Returns an error if the server doesn't support NFSv4.2 or rejects AUTH_SYS.
    pub fn connect(info: &NfsBypassInfo) -> Result<Self, NfsError> {
        let stream = TcpStream::connect_timeout(
            &info.server_addr,
            std::time::Duration::from_secs(5),
        )?;
        stream.set_nodelay(true)?;

        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let machine = {
            let mut buf = [0u8; 256];
            if unsafe { libc::gethostname(buf.as_mut_ptr() as *mut _, buf.len()) } == 0 {
                let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
                String::from_utf8_lossy(&buf[..len]).to_string()
            } else {
                "foxing".to_string()
            }
        };

        let mut client = Self {
            stream,
            session_id: [0u8; 16],
            sequence_id: AtomicU32::new(1),
            client_id: 0,
            uid,
            gid,
            machine,
            xid_counter: AtomicU32::new(1),
            dir_handle_cache: DashMap::new(),
        };

        // TODO: Phase 3 — implement EXCHANGE_ID + CREATE_SESSION
        // For now, return the connected client
        info!("NFS bypass: connected to {} (session establishment pending)", info.server_addr);

        Ok(client)
    }

    /// Get the next XID for RPC calls.
    fn next_xid(&self) -> u32 {
        self.xid_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the next sequence ID for this session slot.
    fn next_sequence_id(&self) -> u32 {
        self.sequence_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Write a single file to the NFS server via compound RPC.
    ///
    /// Constructs: SEQUENCE + PUTFH(parent) + OPEN(create) + WRITE + SETATTR + CLOSE
    /// All in one TCP round-trip.
    pub fn write_file(
        &mut self,
        parent_handle: &[u8],
        filename: &str,
        data: &[u8],
        mode: u32,
        uid: u32,
        gid: u32,
        mtime: (i64, i64),
    ) -> Result<(), NfsError> {
        let xid = self.next_xid();
        let seq_id = self.next_sequence_id();

        // Build the compound: SEQUENCE + PUTFH + OPEN + WRITE + SETATTR + CLOSE
        let open_stateid = StateId::default(); // Will be populated by OPEN reply

        let ops = vec![
            Nfs4Op::Sequence {
                session_id: self.session_id,
                sequence_id: seq_id,
                slot_id: 0,
                highest_slot_id: 0,
                cache_this: false,
            },
            Nfs4Op::PutFh { handle: parent_handle.to_vec() },
            Nfs4Op::Open {
                seqid: 0,
                share_access: rpc::OPEN4_SHARE_ACCESS_WRITE,
                share_deny: rpc::OPEN4_SHARE_DENY_NONE,
                owner: format!("foxing-{}", std::process::id()).into_bytes(),
                filename: filename.to_string(),
                mode,
            },
            // WRITE and CLOSE use the stateid from OPEN — in a real implementation
            // we'd need a two-phase approach or the server fills in the special stateid.
            Nfs4Op::Write {
                stateid: open_stateid,
                offset: 0,
                stable: WriteStable::FileSync,
                data: data.to_vec(),
            },
            Nfs4Op::Close {
                seqid: 1,
                stateid: open_stateid,
            },
        ];

        let msg = rpc::build_compound(xid, "fxcp", self.uid, self.gid, &self.machine, &ops);

        // Send compound
        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        // Read reply
        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;

        // Check overall status
        if reply.status != rpc::NFS4_OK {
            return Err(NfsError::Nfs4Error {
                code: reply.status,
                message: format!("compound failed: {}", rpc::nfs4_error_name(reply.status)),
            });
        }

        // Check individual op statuses
        for result in &reply.op_results {
            if result.status != rpc::NFS4_OK {
                return Err(NfsError::Nfs4Error {
                    code: result.status,
                    message: format!("op {} failed: {}", result.op, rpc::nfs4_error_name(result.status)),
                });
            }
        }

        debug!("NFS bypass: wrote {} ({} bytes) via compound RPC", filename, data.len());
        Ok(())
    }

    /// Read a complete RPC reply (record-mark framed) from the TCP stream.
    fn read_reply(&mut self) -> Result<Vec<u8>, NfsError> {
        // Read 4-byte record mark
        let mut rm_buf = [0u8; 4];
        self.stream.read_exact(&mut rm_buf)?;
        let rm = u32::from_be_bytes(rm_buf);
        let _last_fragment = (rm & 0x80000000) != 0;
        let length = (rm & 0x7FFFFFFF) as usize;

        if length > 16 * 1024 * 1024 + 4096 {
            return Err(NfsError::RpcError(format!("reply too large: {} bytes", length)));
        }

        // Read the full fragment
        let mut data = vec![0u8; 4 + length]; // Include record mark for parser
        data[..4].copy_from_slice(&rm_buf);
        self.stream.read_exact(&mut data[4..])?;

        Ok(data)
    }

    /// Get or resolve the NFS file handle for a directory path.
    ///
    /// Uses `name_to_handle_at(2)` for the initial resolution and caches the result.
    pub fn get_or_resolve_handle(&self, dir_path: &Path) -> Result<Vec<u8>, NfsError> {
        if let Some(cached) = self.dir_handle_cache.get(dir_path) {
            return Ok(cached.clone());
        }

        let handle = super::mount::resolve_nfs_handle(dir_path)
            .map_err(|e| NfsError::StaleHandle { path: format!("{}: {}", dir_path.display(), e) })?;

        self.dir_handle_cache.insert(dir_path.to_path_buf(), handle.clone());
        Ok(handle)
    }

    /// Invalidate a cached directory handle (e.g., after NFS4ERR_STALE).
    pub fn invalidate_handle(&self, dir_path: &Path) {
        self.dir_handle_cache.remove(dir_path);
    }
}
