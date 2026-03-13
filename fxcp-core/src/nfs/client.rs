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
        // NFS servers default to `secure` — require source port <1024.
        // Bind to a privileged port before connecting (requires root/CAP_NET_BIND_SERVICE).
        let stream = Self::connect_privileged(&info.server_addr)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;

        // Use effective UID/GID directly. The NFS server handles root_squash
        // mapping — we send our real credentials and let the server decide.
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

        // Step 3: RECLAIM_COMPLETE — tell server we have no state to reclaim.
        // This ends the grace period for our session so OPEN works immediately.
        self.do_reclaim_complete()?;
        debug!("NFS bypass: RECLAIM_COMPLETE ok");

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

    /// Send RECLAIM_COMPLETE to end the grace period for our session.
    fn do_reclaim_complete(&mut self) -> Result<(), NfsError> {
        let seq_id = self.next_sequence_id();
        let ops = vec![
            Nfs4Op::Sequence {
                session_id: self.session_id,
                sequence_id: seq_id,
                slot_id: 0,
                highest_slot_id: 0,
                cache_this: false,
            },
            Nfs4Op::ReclaimComplete,
        ];
        let msg = rpc::build_compound(self.next_xid(), "rclm", self.uid, self.gid, &self.machine, &ops);
        self.stream.write_all(&msg)?;
        self.stream.flush()?;
        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;
        if reply.status != rpc::NFS4_OK {
            return Err(NfsError::SessionFailed(format!(
                "RECLAIM_COMPLETE failed: {} ({})", rpc::nfs4_error_name(reply.status), reply.status
            )));
        }
        Ok(())
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

    /// Connect to an NFS server from a privileged source port (<1024).
    /// NFS servers with `secure` (default) reject connections from ports ≥1024.
    fn connect_privileged(server: &std::net::SocketAddr) -> Result<TcpStream, NfsError> {
        use std::os::unix::io::FromRawFd;

        let domain = if server.is_ipv4() { libc::AF_INET } else { libc::AF_INET6 };
        let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(NfsError::ConnectionFailed(std::io::Error::last_os_error()));
        }

        // Try binding to ports 900-1023 (privileged range, avoids well-known services)
        let mut bound = false;
        for port in (900..1024).rev() {
            let ret = if server.is_ipv4() {
                let addr = libc::sockaddr_in {
                    sin_family: libc::AF_INET as u16,
                    sin_port: (port as u16).to_be(),
                    sin_addr: libc::in_addr { s_addr: 0 },
                    sin_zero: [0; 8],
                };
                unsafe { libc::bind(fd, &addr as *const _ as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in>() as u32) }
            } else {
                let addr = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as u16,
                    sin6_port: (port as u16).to_be(),
                    sin6_flowinfo: 0,
                    sin6_addr: libc::in6_addr { s6_addr: [0; 16] },
                    sin6_scope_id: 0,
                };
                unsafe { libc::bind(fd, &addr as *const _ as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in6>() as u32) }
            };
            if ret == 0 {
                bound = true;
                debug!("NFS bypass: bound to privileged port {}", port);
                break;
            }
        }

        if !bound {
            unsafe { libc::close(fd); }
            // Fall back to ephemeral port (works if server has `insecure` export option)
            debug!("NFS bypass: no privileged port available, using ephemeral");
            return Ok(TcpStream::connect_timeout(server, std::time::Duration::from_secs(5))?);
        }

        // Connect to server
        let connect_result = match server {
            std::net::SocketAddr::V4(v4) => {
                let addr = libc::sockaddr_in {
                    sin_family: libc::AF_INET as u16,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(v4.ip().octets()) },
                    sin_zero: [0; 8],
                };
                unsafe { libc::connect(fd, &addr as *const _ as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in>() as u32) }
            }
            std::net::SocketAddr::V6(v6) => {
                let addr = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as u16,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr { s6_addr: v6.ip().octets() },
                    sin6_scope_id: v6.scope_id(),
                };
                unsafe { libc::connect(fd, &addr as *const _ as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in6>() as u32) }
            }
        };

        if connect_result != 0 {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd); }
            return Err(NfsError::ConnectionFailed(err));
        }

        Ok(unsafe { TcpStream::from_raw_fd(fd) })
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
    /// Retries on NFS4ERR_GRACE (server grace period after session creation).
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
        for attempt in 0..3 {
            match self.write_file_inner(parent_handle, filename, data, mode, uid, gid, mtime) {
                Ok(()) => return Ok(()),
                Err(NfsError::Nfs4Error { code: 10013, .. }) => {
                    // NFS4ERR_GRACE — server in grace period, retry after delay
                    if attempt < 2 {
                        debug!("NFS bypass: server in grace period, retry {} in 500ms", attempt + 1);
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        continue;
                    }
                    return Err(NfsError::Nfs4Error {
                        code: 10013,
                        message: "server grace period persists after retries".into(),
                    });
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    fn write_file_inner(
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
                clientid: self.client_id,
                owner: format!("foxing-{}", std::process::id()).into_bytes(),
                filename: filename.to_string(),
                mode,
            },
            Nfs4Op::Write {
                stateid: StateId::current(),
                offset: 0,
                stable: WriteStable::FileSync,
                data: data.to_vec(),
            },
            // SETATTR in same compound — sets uid/gid/mtime without extra round-trip
            Nfs4Op::SetAttr {
                stateid: StateId::current(),
                mode: None, // already set by OPEN
                uid: Some(uid),
                gid: Some(gid),
                mtime: Some(mtime),
            },
            Nfs4Op::Close {
                seqid: 1,
                stateid: StateId::current(),
            },
        ];

        let msg = rpc::build_compound(xid, "fxcp", self.uid, self.gid, &self.machine, &ops);

        debug!("NFS write compound: {} bytes, {} ops, handle={} bytes, data={} bytes",
               msg.len(), ops.len(), parent_handle.len(), data.len());
        // Hex dump the compound starting after RPC header to show op boundaries
        // Skip 4 (record mark) + ~120 (RPC+AUTH+COMPOUND header)
        // The ops start after the compound header
        if msg.len() > 140 {
            let ops_start = &msg[4..]; // skip record mark
            // Find the ops by looking for the number of ops u32
            debug!("NFS compound hex (first 200 bytes after record mark): {:02x?}", &ops_start[..ops_start.len().min(200)]);
        }

        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;

        // Log detailed reply info
        debug!("NFS compound reply: overall_status={} ({}) ops={}",
               reply.status, rpc::nfs4_error_name(reply.status), reply.op_results.len());
        for (i, r) in reply.op_results.iter().enumerate() {
            debug!("  op[{}]: op={} status={} ({})", i, r.op, r.status, rpc::nfs4_error_name(r.status));
        }

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
    /// Handles multi-fragment replies by reading until the last-fragment bit is set.
    fn read_reply(&mut self) -> Result<Vec<u8>, NfsError> {
        let mut result = Vec::with_capacity(4096);

        loop {
            let mut rm_buf = [0u8; 4];
            self.stream.read_exact(&mut rm_buf)?;
            let rm = u32::from_be_bytes(rm_buf);
            let last_fragment = (rm & 0x80000000) != 0;
            let length = (rm & 0x7FFFFFFF) as usize;

            if length > 16 * 1024 * 1024 + 4096 {
                return Err(NfsError::RpcError(format!("reply fragment too large: {} bytes", length)));
            }

            if result.is_empty() {
                // First fragment — include the record mark for the parser
                result.extend_from_slice(&rm_buf);
            }

            let offset = result.len();
            result.resize(offset + length, 0);
            self.stream.read_exact(&mut result[offset..])?;

            if last_fragment {
                break;
            }
        }

        Ok(result)
    }

    /// Get or resolve the NFS file handle for a directory path.
    ///
    /// Uses PUTROOTFH + LOOKUP compounds to walk from the export root to
    /// the target directory, caching intermediate handles.
    pub fn get_or_resolve_handle(&mut self, dir_path: &Path) -> Result<Vec<u8>, NfsError> {
        if let Some(cached) = self.dir_handle_cache.get(dir_path) {
            return Ok(cached.clone());
        }

        // Compute relative path from mount point
        let rel = dir_path.strip_prefix(&self.server_info.mount_point)
            .unwrap_or(std::path::Path::new(""));

        // Walk from export root via PUTROOTFH + LOOKUP chain + GETFH
        let handle = self.resolve_via_lookup(rel)?;

        self.dir_handle_cache.insert(dir_path.to_path_buf(), handle.clone());
        Ok(handle)
    }

    /// Resolve a relative path from the export root using PUTFH + LOOKUP + GETFH.
    fn resolve_via_lookup(&mut self, rel_path: &Path) -> Result<Vec<u8>, NfsError> {
        let xid = self.next_xid();
        let seq_id = self.next_sequence_id();

        // Get the mount point's NFS wire filehandle via name_to_handle_at.
        // The kernel stores the server's opaque handle; we extract the wire
        // portion (skipping the kernel's internal 14-byte header).
        let mount_fh = super::mount::resolve_nfs_handle(&self.server_info.mount_point)
            .map_err(|e| NfsError::StaleHandle { path: format!("mount: {}", e) })?;
        debug!("NFS PUTFH: mount handle {} bytes: {:02x?}", mount_fh.len(), &mount_fh[..mount_fh.len().min(16)]);

        let mut ops = vec![
            Nfs4Op::Sequence {
                session_id: self.session_id,
                sequence_id: seq_id,
                slot_id: 0,
                highest_slot_id: 0,
                cache_this: false,
            },
            Nfs4Op::PutFh { handle: mount_fh },
        ];

        // LOOKUP only the relative path within the export
        for component in rel_path.components() {
            if let std::path::Component::Normal(name) = component {
                ops.push(Nfs4Op::Lookup { name: name.to_string_lossy().to_string() });
            }
        }

        // GETFH to retrieve the actual server-side filehandle
        ops.push(Nfs4Op::GetFh);

        let msg = rpc::build_compound(xid, "lkup", self.uid, self.gid, &self.machine, &ops);
        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;

        debug!("NFS LOOKUP reply: overall_status={} ({}) ops={}",
               reply.status, rpc::nfs4_error_name(reply.status), reply.op_results.len());
        for (i, r) in reply.op_results.iter().enumerate() {
            debug!("  LOOKUP op[{}]: op={} status={} ({})", i, r.op, r.status, rpc::nfs4_error_name(r.status));
        }

        if reply.status != rpc::NFS4_OK {
            return Err(NfsError::Nfs4Error {
                code: reply.status,
                message: format!("LOOKUP for {:?} failed: {}", rel_path, rpc::nfs4_error_name(reply.status)),
            });
        }

        // Extract the filehandle from GETFH result
        for result in &reply.op_results {
            if result.op == rpc::OP_GETFH {
                if let Some(fh) = &result.filehandle {
                    debug!("NFS GETFH: resolved {:?} → {} bytes", rel_path, fh.len());
                    return Ok(fh.clone());
                }
            }
        }

        Err(NfsError::SessionFailed(format!("GETFH not found in reply for {:?}", rel_path)))
    }

    /// Invalidate a cached directory handle.
    pub fn invalidate_handle(&self, dir_path: &Path) {
        self.dir_handle_cache.remove(dir_path);
    }

    /// Batch-fetch size+mtime for multiple files in a directory.
    ///
    /// Sends a single compound: SEQUENCE + PUTFH(dir) + [LOOKUP(file) + GETATTR]×N
    /// Returns (filename, size, mtime_sec, mtime_nsec) for each file found.
    /// Files that don't exist (LOOKUP returns NFS4ERR_NOENT) are silently skipped.
    /// Max 7 files per compound (SEQUENCE + PUTFH + 7×(LOOKUP+GETATTR) = 16 ops).
    pub fn batch_stat(
        &mut self,
        dir_handle: &[u8],
        filenames: &[&str],
    ) -> Result<Vec<(String, u64, i64, i64)>, NfsError> {
        let xid = self.next_xid();
        let seq_id = self.next_sequence_id();

        let attr_request = [1 << rpc::FATTR4_SIZE, 1 << (rpc::FATTR4_TIME_MODIFY - 32)];

        let mut ops = vec![
            rpc::Nfs4Op::Sequence {
                session_id: self.session_id,
                sequence_id: seq_id,
                slot_id: 0,
                highest_slot_id: 0,
                cache_this: false,
            },
            rpc::Nfs4Op::PutFh { handle: dir_handle.to_vec() },
        ];

        for name in filenames {
            ops.push(rpc::Nfs4Op::Lookup { name: name.to_string() });
            ops.push(rpc::Nfs4Op::GetAttr { attr_request });
        }

        let msg = rpc::build_compound(xid, "bstat", self.uid, self.gid, &self.machine, &ops);
        self.stream.write_all(&msg)?;
        self.stream.flush()?;

        let reply_data = self.read_reply()?;
        let reply = rpc::parse_compound_reply(&reply_data)?;

        // Extract results: for each LOOKUP+GETATTR pair, check status
        let mut results = Vec::new();
        for (i, name) in filenames.iter().enumerate() {
            // Find the LOOKUP result for this file (skip SEQUENCE + PUTFH = first 2 ops)
            let lookup_idx = 2 + i * 2;
            let getattr_idx = lookup_idx + 1;

            // Check LOOKUP status
            if let Some(lookup_result) = reply.op_results.get(lookup_idx) {
                if lookup_result.status != rpc::NFS4_OK {
                    continue; // file doesn't exist or LOOKUP failed
                }
            } else {
                break; // compound stopped processing (prior error)
            }

            // Check GETATTR result
            if let Some(getattr_result) = reply.op_results.get(getattr_idx) {
                if getattr_result.status == rpc::NFS4_OK {
                    if let (Some(size), Some((mtime_s, mtime_ns))) = (getattr_result.size, getattr_result.mtime) {
                        results.push((name.to_string(), size, mtime_s, mtime_ns));
                    }
                }
            } else {
                break;
            }
        }

        Ok(results)
    }
}

/// Fast NFS server liveness check via NULL RPC.
///
/// Opens a fresh TCP connection to the NFS server and sends a NULL procedure
/// call. Returns true if the server responds within 2 seconds. Does not
/// touch any session or cached state — safe to call from the health probe.
pub fn probe_server_alive(server_addr: &std::net::SocketAddr) -> bool {
    use std::io::{Read, Write};
    let mut stream = match std::net::TcpStream::connect_timeout(server_addr, std::time::Duration::from_secs(2)) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(2)));
    let msg = rpc::build_null_call(1);
    if stream.write_all(&msg).is_err() { return false; }
    if stream.flush().is_err() { return false; }
    // NULL reply is just an RPC reply header — reading any bytes means success
    let mut rm_buf = [0u8; 4];
    stream.read_exact(&mut rm_buf).is_ok()
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
