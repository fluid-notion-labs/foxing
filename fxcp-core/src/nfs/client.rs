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
use super::rpc::{self, Nfs4Op, StateId, WriteStable};
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
    server_info: NfsBypassInfo,
    /// Cache of directory path → NFS file handle.
    dir_handle_cache: DashMap<PathBuf, Vec<u8>>,
}

impl NfsCompoundClient {
    /// Connect to the NFS server and establish a session.
    ///
    /// Performs TCP connect → EXCHANGE_ID → CREATE_SESSION.
    pub fn connect(info: &NfsBypassInfo) -> Result<Self, NfsError> {
        let stream = TcpStream::connect_timeout(
            &info.server_addr,
            std::time::Duration::from_secs(5),
        )?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;

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
            server_info: info.clone(),
            dir_handle_cache: DashMap::new(),
        };

        // Session bootstrap: EXCHANGE_ID → CREATE_SESSION
        client.establish_session()?;

        info!("NFS bypass: session established with {} (client_id={:#x})",
              info.server_addr, client.client_id);

        Ok(client)
    }

    /// Establish NFSv4.1+ session via EXCHANGE_ID + CREATE_SESSION.
    fn establish_session(&mut self) -> Result<(), NfsError> {
        // Step 1: EXCHANGE_ID — register client identity with server
        let client_id = self.do_exchange_id()?;
        self.client_id = client_id;
        debug!("NFS bypass: EXCHANGE_ID ok, client_id={:#x}", client_id);

        // Step 2: CREATE_SESSION — get session_id and slot table
        let session_id = self.do_create_session(client_id)?;
        self.session_id = session_id;
        self.sequence_id.store(1, Ordering::Relaxed);
        debug!("NFS bypass: CREATE_SESSION ok, session={:02x?}", &session_id[..4]);

        Ok(())
    }

    /// Send EXCHANGE_ID compound to get a client_id from the server.
    fn do_exchange_id(&mut self) -> Result<u64, NfsError> {
        let xid = self.next_xid();

        // Build EXCHANGE_ID compound (no SEQUENCE — it's a session-creating op)
        let mut body = super::xdr::XdrEncoder::new(512);

        // RPC header
        body.encode_u32(xid);
        body.encode_u32(0); // CALL
        body.encode_u32(rpc::RPC_VERSION);
        body.encode_u32(rpc::NFS_PROGRAM);
        body.encode_u32(rpc::NFS_V4);
        body.encode_u32(rpc::NFSPROC4_COMPOUND);

        // AUTH_SYS
        encode_auth_sys(&mut body, self.uid, self.gid, &self.machine);
        // Verifier AUTH_NONE
        body.encode_u32(rpc::AUTH_NONE);
        body.encode_u32(0);

        // COMPOUND args: tag, minorversion=2, ops=[EXCHANGE_ID]
        body.encode_string("exid");
        body.encode_u32(2); // minorversion
        body.encode_u32(1); // 1 operation

        // EXCHANGE_ID operation
        body.encode_u32(rpc::OP_EXCHANGE_ID);
        // eia_clientowner: co_verifier(8) + co_ownerid(opaque)
        let verifier = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        body.encode_u64(verifier); // co_verifier
        let owner_id = format!("foxing.{}.{}", self.machine, std::process::id());
        body.encode_opaque(owner_id.as_bytes()); // co_ownerid
        // eia_flags
        body.encode_u32(0x00000001); // EXCHGID4_FLAG_SUPP_MOVED_REFER
        // eia_state_protect: SP4_NONE
        body.encode_u32(0); // SP4_NONE
        // eia_client_impl_id: empty array
        body.encode_u32(0); // 0 elements

        let body_bytes = body.into_bytes();
        let mut msg = Vec::with_capacity(4 + body_bytes.len());
        let rm = 0x80000000u32 | (body_bytes.len() as u32);
        msg.extend_from_slice(&rm.to_be_bytes());
        msg.extend_from_slice(&body_bytes);

        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;

        if reply.status != rpc::NFS4_OK {
            return Err(NfsError::SessionFailed(format!(
                "EXCHANGE_ID failed: {} ({})", rpc::nfs4_error_name(reply.status), reply.status
            )));
        }

        // Parse EXCHANGE_ID result: skip to client_id (u64)
        // The reply parsing is minimal — we need the client_id from the EXCHANGE_ID result
        // It comes after: status(4) + eir_clientid(8) + eir_sequenceid(4) + eir_flags(4) + ...
        // Since our generic parser stopped after seeing the op status, we need to
        // extract client_id from the raw reply data.
        let client_id = self.extract_exchange_id_client_id(&reply_data)?;
        Ok(client_id)
    }

    /// Extract client_id from EXCHANGE_ID reply by scanning for it in the raw data.
    fn extract_exchange_id_client_id(&self, data: &[u8]) -> Result<u64, NfsError> {
        // After the RPC reply header and compound header, find EXCHANGE_ID result.
        // The EXCHANGE_ID result starts after: op(4) + status(4), then:
        //   eir_clientid(8), eir_sequenceid(4), eir_flags(4), ...
        // We scan backwards from known structure. Simplified: look for the op code
        // OP_EXCHANGE_ID (42) followed by NFS4_OK (0), then read the next 8 bytes.
        let needle = [
            0, 0, 0, rpc::OP_EXCHANGE_ID as u8,  // op = 42
            0, 0, 0, 0,                            // status = 0 (NFS4_OK)
        ];
        if let Some(pos) = data.windows(8).position(|w| w == needle) {
            let cid_start = pos + 8;
            if cid_start + 8 <= data.len() {
                let client_id = u64::from_be_bytes([
                    data[cid_start], data[cid_start+1], data[cid_start+2], data[cid_start+3],
                    data[cid_start+4], data[cid_start+5], data[cid_start+6], data[cid_start+7],
                ]);
                return Ok(client_id);
            }
        }
        Err(NfsError::SessionFailed("cannot parse client_id from EXCHANGE_ID reply".into()))
    }

    /// Send CREATE_SESSION compound to get a session_id.
    fn do_create_session(&mut self, client_id: u64) -> Result<[u8; 16], NfsError> {
        let xid = self.next_xid();

        let mut body = super::xdr::XdrEncoder::new(512);

        // RPC header
        body.encode_u32(xid);
        body.encode_u32(0); // CALL
        body.encode_u32(rpc::RPC_VERSION);
        body.encode_u32(rpc::NFS_PROGRAM);
        body.encode_u32(rpc::NFS_V4);
        body.encode_u32(rpc::NFSPROC4_COMPOUND);

        // AUTH_SYS
        encode_auth_sys(&mut body, self.uid, self.gid, &self.machine);
        body.encode_u32(rpc::AUTH_NONE);
        body.encode_u32(0);

        // COMPOUND args
        body.encode_string("csess");
        body.encode_u32(2); // minorversion
        body.encode_u32(1); // 1 operation

        // CREATE_SESSION operation (RFC 8881 §18.36)
        body.encode_u32(rpc::OP_CREATE_SESSION);
        body.encode_u64(client_id);          // csa_clientid
        body.encode_u32(1);                   // csa_sequence (from EXCHANGE_ID eir_sequenceid)
        body.encode_u32(0);                   // csa_flags: 0 (no persist, no back channel)
        // csa_fore_chan_attrs (channel_attrs4):
        //   headerpadsize, maxrequestsize, maxresponsesize,
        //   maxresponsesize_cached, maxoperations, maxrequests, rdma_ird[]
        body.encode_u32(0);                   // ca_headerpadsize
        body.encode_u32(16 * 1024 * 1024 + 4096); // ca_maxrequestsize
        body.encode_u32(1024 * 1024);         // ca_maxresponsesize
        body.encode_u32(4096);                // ca_maxresponsesize_cached
        body.encode_u32(16);                  // ca_maxoperations
        body.encode_u32(1);                   // ca_maxrequests (1 slot)
        body.encode_u32(0);                   // ca_rdma_ird count (empty array)
        // csa_back_chan_attrs (minimal — no back channel)
        body.encode_u32(0);                   // ca_headerpadsize
        body.encode_u32(4096);                // ca_maxrequestsize
        body.encode_u32(4096);                // ca_maxresponsesize
        body.encode_u32(0);                   // ca_maxresponsesize_cached
        body.encode_u32(2);                   // ca_maxoperations
        body.encode_u32(0);                   // ca_maxrequests (0 = no back channel)
        body.encode_u32(0);                   // ca_rdma_ird count
        // csa_cb_program
        body.encode_u32(0x40000000);          // callback program number (unused)
        // csa_sec_parms: callback_sec_parms4[]
        // For AUTH_SYS: secflavor(4) + authsys_parms
        body.encode_u32(1);                   // array count: 1 element
        body.encode_u32(rpc::AUTH_SYS);       // cb_secflavor
        // authsys_parms (cbsp_sys_cred):
        body.encode_u32(0);                   // stamp
        body.encode_string(&self.machine);    // machinename
        body.encode_u32(self.uid);            // uid
        body.encode_u32(self.gid);            // gid
        body.encode_u32(1);                   // gids count
        body.encode_u32(self.gid);            // gids[0]

        let body_bytes = body.into_bytes();
        let mut msg = Vec::with_capacity(4 + body_bytes.len());
        let rm = 0x80000000u32 | (body_bytes.len() as u32);
        msg.extend_from_slice(&rm.to_be_bytes());
        msg.extend_from_slice(&body_bytes);

        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;

        if reply.status != rpc::NFS4_OK {
            return Err(NfsError::SessionFailed(format!(
                "CREATE_SESSION failed: {} ({})", rpc::nfs4_error_name(reply.status), reply.status
            )));
        }

        // Extract session_id from CREATE_SESSION reply
        let session_id = self.extract_session_id(&reply_data)?;
        Ok(session_id)
    }

    /// Extract session_id (16 bytes) from CREATE_SESSION reply.
    fn extract_session_id(&self, data: &[u8]) -> Result<[u8; 16], NfsError> {
        // CREATE_SESSION result starts after op(4) + status(4), then session_id(16)
        let needle = [
            0, 0, 0, rpc::OP_CREATE_SESSION as u8,
            0, 0, 0, 0,
        ];
        if let Some(pos) = data.windows(8).position(|w| w == needle) {
            let sid_start = pos + 8;
            if sid_start + 16 <= data.len() {
                let mut session_id = [0u8; 16];
                session_id.copy_from_slice(&data[sid_start..sid_start + 16]);
                return Ok(session_id);
            }
        }
        Err(NfsError::SessionFailed("cannot parse session_id from CREATE_SESSION reply".into()))
    }

    /// Re-establish session after NFS4ERR_BADSESSION or connection loss.
    pub fn recover_session(&mut self) -> Result<(), NfsError> {
        warn!("NFS bypass: recovering session...");
        // Reconnect TCP
        self.stream = TcpStream::connect_timeout(
            &self.server_info.server_addr,
            std::time::Duration::from_secs(5),
        )?;
        self.stream.set_nodelay(true)?;
        self.stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        self.stream.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;

        // Re-establish session
        self.establish_session()?;

        // Clear handle cache (handles may be stale after session loss)
        self.dir_handle_cache.clear();

        info!("NFS bypass: session recovered (client_id={:#x})", self.client_id);
        Ok(())
    }

    fn next_xid(&self) -> u32 {
        self.xid_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn next_sequence_id(&self) -> u32 {
        self.sequence_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Write a single file to the NFS server via compound RPC.
    ///
    /// SEQUENCE + PUTFH(parent) + OPEN(create) + WRITE(FILE_SYNC) + CLOSE
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
            Nfs4Op::Write {
                stateid: StateId::default(), // Server uses current stateid from OPEN
                offset: 0,
                stable: WriteStable::FileSync,
                data: data.to_vec(),
            },
            Nfs4Op::Close {
                seqid: 1,
                stateid: StateId::default(),
            },
        ];

        let msg = rpc::build_compound(xid, "fxcp", self.uid, self.gid, &self.machine, &ops);

        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;

        if reply.status != rpc::NFS4_OK {
            let code = reply.status;
            // Check for recoverable errors
            if code == rpc::NFS4ERR_BADSESSION || code == rpc::NFS4ERR_BADSEQ {
                return Err(NfsError::Nfs4Error {
                    code,
                    message: format!("session error: {} — needs recovery", rpc::nfs4_error_name(code)),
                });
            }
            if code == rpc::NFS4ERR_STALE {
                return Err(NfsError::StaleHandle {
                    path: filename.to_string(),
                });
            }
            return Err(NfsError::Nfs4Error {
                code,
                message: format!("compound failed: {}", rpc::nfs4_error_name(code)),
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
        let mut rm_buf = [0u8; 4];
        self.stream.read_exact(&mut rm_buf)?;
        let rm = u32::from_be_bytes(rm_buf);
        let length = (rm & 0x7FFFFFFF) as usize;

        if length > 16 * 1024 * 1024 + 4096 {
            return Err(NfsError::RpcError(format!("reply too large: {} bytes", length)));
        }

        let mut data = vec![0u8; 4 + length];
        data[..4].copy_from_slice(&rm_buf);
        self.stream.read_exact(&mut data[4..])?;

        Ok(data)
    }

    /// Get or resolve the NFS file handle for a directory path.
    pub fn get_or_resolve_handle(&self, dir_path: &Path) -> Result<Vec<u8>, NfsError> {
        if let Some(cached) = self.dir_handle_cache.get(dir_path) {
            return Ok(cached.clone());
        }

        let handle = super::mount::resolve_nfs_handle(dir_path)
            .map_err(|e| NfsError::StaleHandle { path: format!("{}: {}", dir_path.display(), e) })?;

        self.dir_handle_cache.insert(dir_path.to_path_buf(), handle.clone());
        Ok(handle)
    }

    /// Invalidate a cached directory handle.
    pub fn invalidate_handle(&self, dir_path: &Path) {
        self.dir_handle_cache.remove(dir_path);
    }
}

/// Encode AUTH_SYS credentials.
fn encode_auth_sys(enc: &mut super::xdr::XdrEncoder, uid: u32, gid: u32, machine: &str) {
    enc.encode_u32(rpc::AUTH_SYS);
    let mut body = super::xdr::XdrEncoder::new(64);
    body.encode_u32(0);          // stamp
    body.encode_string(machine);
    body.encode_u32(uid);
    body.encode_u32(gid);
    body.encode_u32(1);          // gids count
    body.encode_u32(gid);        // gids[0]
    let body_bytes = body.into_bytes();
    enc.encode_opaque(&body_bytes);
}
