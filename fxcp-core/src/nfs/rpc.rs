// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/nfs/rpc.rs — NFSv4.2 compound RPC builder and reply parser

//! Constructs NFSv4.2 compound RPC payloads and parses server replies.
//!
//! Implements the minimal ONC RPC + NFSv4.2 wire format needed for
//! atomic file creation via compound operations.

use super::xdr::{XdrEncoder, XdrDecoder};
use super::NfsError;

// -----------------------------------------------------------------------
// NFSv4 operation codes (RFC 8881)
// -----------------------------------------------------------------------
pub const OP_SEQUENCE: u32 = 53;
pub const OP_PUTFH: u32 = 22;
pub const OP_OPEN: u32 = 18;
pub const OP_WRITE: u32 = 38;
pub const OP_SETATTR: u32 = 34;
pub const OP_CLOSE: u32 = 4;
pub const OP_GETATTR: u32 = 9;
pub const OP_EXCHANGE_ID: u32 = 42;
pub const OP_CREATE_SESSION: u32 = 43;
pub const OP_DESTROY_SESSION: u32 = 44;
pub const OP_PUTROOTFH: u32 = 24;
pub const OP_LOOKUP: u32 = 15;
pub const OP_GETFH: u32 = 10;
pub const OP_RECLAIM_COMPLETE: u32 = 58;

// NFS4 status codes
pub const NFS4_OK: u32 = 0;
pub const NFS4ERR_STALE: u32 = 70;
pub const NFS4ERR_NOENT: u32 = 2;
pub const NFS4ERR_EXIST: u32 = 17;
pub const NFS4ERR_ACCESS: u32 = 13;
pub const NFS4ERR_DELAY: u32 = 10008;
pub const NFS4ERR_BADSESSION: u32 = 10052;
pub const NFS4ERR_BADSEQ: u32 = 10026;
pub const NFS4ERR_SEQ_MISORDERED: u32 = 10063;

// ONC RPC constants
pub const RPC_VERSION: u32 = 2;
pub const NFS_PROGRAM: u32 = 100003;
pub const NFS_V4: u32 = 4;
pub const NFSPROC4_COMPOUND: u32 = 1;
pub const NFSPROC4_NULL: u32 = 0;
pub const AUTH_SYS: u32 = 1;
pub const AUTH_NONE: u32 = 0;

// OPEN constants
pub const OPEN4_SHARE_ACCESS_READ: u32 = 0x00000001;
pub const OPEN4_SHARE_ACCESS_WRITE: u32 = 0x00000002;
pub const OPEN4_SHARE_ACCESS_BOTH: u32 = 0x00000003;
pub const OPEN4_SHARE_DENY_NONE: u32 = 0x00000000;
pub const CLAIM_NULL: u32 = 0;
pub const OPEN4_CREATE: u32 = 1;
pub const OPEN4_NOCREATE: u32 = 0;
pub const CREATEMODE4_UNCHECKED: u32 = 0;

// WRITE constants
pub const UNSTABLE4: u32 = 0;
pub const DATA_SYNC4: u32 = 1;
pub const FILE_SYNC4: u32 = 2;

// FATTR4 attribute bitmap positions (word 1)
pub const FATTR4_MODE: u32 = 33;       // bit 33 = word 1, bit 1
pub const FATTR4_OWNER: u32 = 36;      // bit 36 = word 1, bit 4
pub const FATTR4_OWNER_GROUP: u32 = 37; // bit 37 = word 1, bit 5
pub const FATTR4_TIME_MODIFY_SET: u32 = 52; // bit 52 = word 1, bit 20

// RPC reply status
pub const MSG_ACCEPTED: u32 = 0;
pub const SUCCESS: u32 = 0;

// -----------------------------------------------------------------------
// NFS4 compound operation types
// -----------------------------------------------------------------------

/// Represents an NFSv4 state identifier (returned by OPEN, used by WRITE/SETATTR/CLOSE).
#[derive(Debug, Clone, Copy)]
pub struct StateId {
    pub seqid: u32,
    pub other: [u8; 12],
}

impl Default for StateId {
    fn default() -> Self {
        Self { seqid: 0, other: [0u8; 12] }
    }
}

impl StateId {
    /// The "current stateid" special value — tells the server to use the stateid
    /// from the most recent stateful operation (OPEN) in this compound.
    /// RFC 8881 §16.2.3.1.2
    pub fn current() -> Self {
        Self { seqid: 0xFFFFFFFF, other: [0xFF; 12] }
    }
}

/// Write stability level.
#[derive(Debug, Clone, Copy)]
pub enum WriteStable {
    Unstable,
    DataSync,
    FileSync,
}

/// A single NFSv4.2 compound operation.
#[derive(Debug, Clone)]
pub enum Nfs4Op {
    Sequence {
        session_id: [u8; 16],
        sequence_id: u32,
        slot_id: u32,
        highest_slot_id: u32,
        cache_this: bool,
    },
    PutFh {
        handle: Vec<u8>,
    },
    PutRootFh,
    Lookup {
        name: String,
    },
    Open {
        seqid: u32,
        share_access: u32,
        share_deny: u32,
        clientid: u64,
        owner: Vec<u8>,
        filename: String,
        mode: u32,
    },
    Write {
        stateid: StateId,
        offset: u64,
        stable: WriteStable,
        data: Vec<u8>,
    },
    SetAttr {
        stateid: StateId,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        mtime: Option<(i64, i64)>,
    },
    Close {
        seqid: u32,
        stateid: StateId,
    },
    GetAttr {
        attr_request: [u32; 2],
    },
    GetFh,
    /// Tell server we have no state to reclaim (ends grace period for this session).
    ReclaimComplete,
}

/// Parsed result of a single operation in a compound reply.
#[derive(Debug)]
pub struct OpResult {
    pub op: u32,
    pub status: u32,
    /// For OPEN: the returned stateid.
    pub stateid: Option<StateId>,
    /// For GETFH: the returned filehandle.
    pub filehandle: Option<Vec<u8>>,
}

/// Parsed NFSv4.2 compound reply.
#[derive(Debug)]
pub struct CompoundReply {
    pub xid: u32,
    pub status: u32,
    pub tag: Vec<u8>,
    pub op_results: Vec<OpResult>,
}

// -----------------------------------------------------------------------
// Encoding
// -----------------------------------------------------------------------

/// Encode AUTH_SYS credentials into XDR.
fn encode_auth_sys(enc: &mut XdrEncoder, uid: u32, gid: u32, machine: &str) {
    enc.encode_u32(AUTH_SYS);  // flavor
    // Auth body (length-prefixed)
    let mut body = XdrEncoder::new(64);
    body.encode_u32(0);         // stamp
    body.encode_string(machine); // machinename
    body.encode_u32(uid);        // uid
    body.encode_u32(gid);        // gid
    body.encode_u32(1);          // gids count
    body.encode_u32(gid);        // gids[0]
    let body_bytes = body.into_bytes();
    enc.encode_opaque(&body_bytes);
}

/// Encode a single NFSv4 operation into XDR.
fn encode_op(enc: &mut XdrEncoder, op: &Nfs4Op) {
    match op {
        Nfs4Op::Sequence { session_id, sequence_id, slot_id, highest_slot_id, cache_this } => {
            enc.encode_u32(OP_SEQUENCE);
            enc.encode_opaque_fixed(session_id);  // 16 bytes, no length prefix
            enc.encode_u32(*sequence_id);
            enc.encode_u32(*slot_id);
            enc.encode_u32(*highest_slot_id);
            enc.encode_bool(*cache_this);
        }
        Nfs4Op::PutFh { handle } => {
            enc.encode_u32(OP_PUTFH);
            enc.encode_opaque(handle);
        }
        Nfs4Op::PutRootFh => {
            enc.encode_u32(OP_PUTROOTFH);
        }
        Nfs4Op::Lookup { name } => {
            enc.encode_u32(OP_LOOKUP);
            enc.encode_string(name);
        }
        Nfs4Op::Open { seqid, share_access, share_deny, clientid, owner, filename, mode } => {
            enc.encode_u32(OP_OPEN);
            enc.encode_u32(*seqid);          // seqid
            enc.encode_u32(*share_access);   // share_access
            enc.encode_u32(*share_deny);     // share_deny
            // open_owner4: clientid(8) + owner(opaque)
            enc.encode_u64(*clientid);       // clientid from EXCHANGE_ID
            enc.encode_opaque(owner);        // owner
            // openhow4: opentype + createhow
            enc.encode_u32(OPEN4_CREATE);    // opentype = OPEN4_CREATE
            enc.encode_u32(CREATEMODE4_UNCHECKED); // createmode = UNCHECKED
            // createattrs (fattr4): bitmap + attr_vals for UNCHECKED
            // Minimal: just set mode
            enc.encode_u32(2);               // bitmap length (2 words)
            enc.encode_u32(0);               // bitmap word 0 (no attrs in word 0)
            enc.encode_u32(1 << (FATTR4_MODE - 32)); // bitmap word 1 (mode bit)
            // attr_vals: opaque containing the attribute values
            let mut attr = XdrEncoder::new(8);
            attr.encode_u32(*mode & 0o7777); // mode_masked4 (12 bits)
            let attr_bytes = attr.into_bytes();
            enc.encode_opaque(&attr_bytes);
            // open_claim4: claim_type + claim_data
            enc.encode_u32(CLAIM_NULL);      // claim_type = CLAIM_NULL
            enc.encode_string(filename);     // claim_file = component4
        }
        Nfs4Op::Write { stateid, offset, stable, data } => {
            enc.encode_u32(OP_WRITE);
            // stateid4
            enc.encode_u32(stateid.seqid);
            enc.encode_opaque_fixed(&stateid.other);
            enc.encode_u64(*offset);
            enc.encode_u32(match stable {
                WriteStable::Unstable => UNSTABLE4,
                WriteStable::DataSync => DATA_SYNC4,
                WriteStable::FileSync => FILE_SYNC4,
            });
            enc.encode_opaque(data);
        }
        Nfs4Op::SetAttr { stateid, mode, uid, gid, mtime } => {
            enc.encode_u32(OP_SETATTR);
            // stateid4
            enc.encode_u32(stateid.seqid);
            enc.encode_opaque_fixed(&stateid.other);
            // fattr4 bitmap + values
            let mut bitmap_w1: u32 = 0;
            if mode.is_some() { bitmap_w1 |= 1 << (FATTR4_MODE - 32); }
            // Encode bitmap
            enc.encode_u32(2);   // 2 bitmap words
            enc.encode_u32(0);   // word 0
            enc.encode_u32(bitmap_w1); // word 1
            // attr_vals
            let mut attr = XdrEncoder::new(32);
            if let Some(m) = mode {
                attr.encode_u32(*m);
            }
            let attr_bytes = attr.into_bytes();
            enc.encode_opaque(&attr_bytes);
        }
        Nfs4Op::Close { seqid, stateid } => {
            enc.encode_u32(OP_CLOSE);
            enc.encode_u32(*seqid);
            enc.encode_u32(stateid.seqid);
            enc.encode_opaque_fixed(&stateid.other);
        }
        Nfs4Op::GetAttr { attr_request } => {
            enc.encode_u32(OP_GETATTR);
            enc.encode_u32(2);  // bitmap length
            enc.encode_u32(attr_request[0]);
            enc.encode_u32(attr_request[1]);
        }
        Nfs4Op::GetFh => {
            enc.encode_u32(OP_GETFH);
        }
        Nfs4Op::ReclaimComplete => {
            enc.encode_u32(OP_RECLAIM_COMPLETE);
            enc.encode_bool(false); // rca_one_fs = false (complete for all filesystems)
        }
    }
}

/// Build a complete RPC message (record-mark framed) for an NFSv4.2 COMPOUND call.
///
/// The message is ready to send over TCP to the NFS server.
pub fn build_compound(
    xid: u32,
    tag: &str,
    uid: u32,
    gid: u32,
    machine: &str,
    ops: &[Nfs4Op],
) -> Vec<u8> {
    let mut body = XdrEncoder::new(4096);

    // ONC RPC header
    body.encode_u32(xid);            // XID
    body.encode_u32(0);              // CALL (not REPLY)
    body.encode_u32(RPC_VERSION);    // RPC version 2
    body.encode_u32(NFS_PROGRAM);    // program: NFS
    body.encode_u32(NFS_V4);         // version: 4
    body.encode_u32(NFSPROC4_COMPOUND); // procedure: COMPOUND

    // AUTH_SYS credentials
    encode_auth_sys(&mut body, uid, gid, machine);

    // Verifier (AUTH_NONE)
    body.encode_u32(AUTH_NONE);
    body.encode_u32(0); // verifier body length

    // COMPOUND args
    body.encode_string(tag);         // tag
    body.encode_u32(2);              // minor version (4.2)
    body.encode_u32(ops.len() as u32); // argarray count

    for op in ops {
        encode_op(&mut body, op);
    }

    let body_bytes = body.into_bytes();

    // Record mark: last fragment (MSB=1) + length
    let record_mark = 0x80000000u32 | (body_bytes.len() as u32);
    let mut msg = Vec::with_capacity(4 + body_bytes.len());
    msg.extend_from_slice(&record_mark.to_be_bytes());
    msg.extend_from_slice(&body_bytes);
    msg
}

/// Parse a compound reply, extracting status codes and the OPEN stateid.
///
/// This is a minimal parser — it extracts enough to determine success/failure
/// and to get the stateid from OPEN (needed for WRITE, SETATTR, CLOSE).
pub fn parse_compound_reply(data: &[u8]) -> Result<CompoundReply, NfsError> {
    // Skip record mark (4 bytes)
    if data.len() < 4 {
        return Err(NfsError::XdrDecode("reply too short".into()));
    }
    let mut dec = XdrDecoder::new(&data[4..]);

    // RPC reply header
    let xid = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
    let msg_type = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
    if msg_type != 1 { // REPLY
        return Err(NfsError::RpcError(format!("expected REPLY (1), got {}", msg_type)));
    }

    let reply_stat = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
    if reply_stat != MSG_ACCEPTED {
        return Err(NfsError::RpcError(format!("RPC rejected: {}", reply_stat)));
    }

    // Verifier
    let _verf_flavor = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
    let _verf_body = dec.decode_opaque().map_err(|e| NfsError::XdrDecode(e.to_string()))?;

    // Accept status
    let accept_stat = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
    if accept_stat != SUCCESS {
        return Err(NfsError::RpcError(format!("RPC accept error: {}", accept_stat)));
    }

    // COMPOUND reply
    let compound_status = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
    let tag = dec.decode_opaque().map_err(|e| NfsError::XdrDecode(e.to_string()))?.to_vec();
    let num_results = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;

    let mut op_results = Vec::with_capacity(num_results as usize);
    for _ in 0..num_results {
        let op = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
        let status = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;

        let mut stateid = None;
        let mut filehandle = None;

        // Parse enough of each op result to skip to the next
        if status == NFS4_OK {
            match op {
                OP_SEQUENCE => {
                    dec.skip_raw(16 + 4 + 4 + 4 + 4 + 4).map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                }
                OP_PUTFH | OP_PUTROOTFH | OP_LOOKUP | OP_RECLAIM_COMPLETE => {
                    // No result data
                }
                OP_GETFH => {
                    // GETFH result: opaque filehandle
                    let fh = dec.decode_opaque().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                    filehandle = Some(fh.to_vec());
                }
                OP_OPEN => {
                    let sid_seqid = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                    let sid_other = dec.decode_opaque_fixed(12).map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                    let mut other = [0u8; 12];
                    other.copy_from_slice(sid_other);
                    stateid = Some(StateId { seqid: sid_seqid, other });
                    break; // Stop parsing — OPEN result is complex
                }
                OP_WRITE => {
                    dec.skip_raw(4 + 4 + 8).map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                }
                OP_SETATTR => {
                    let bm_len = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                    dec.skip_raw(bm_len as usize * 4).map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                }
                OP_CLOSE => {
                    dec.skip_raw(4 + 12).map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                }
                OP_GETATTR => {
                    // bitmap + attr_vals — skip
                    let bm_len = dec.decode_u32().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                    dec.skip_raw(bm_len as usize * 4).map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                    let _ = dec.decode_opaque().map_err(|e| NfsError::XdrDecode(e.to_string()))?;
                }
                _ => {
                    break; // Unknown op — can't skip safely
                }
            }
        }

        op_results.push(OpResult { op, status, stateid, filehandle });
    }

    Ok(CompoundReply {
        xid,
        status: compound_status,
        tag,
        op_results,
    })
}

/// Get the human-readable name for an NFS4 error code.
pub fn nfs4_error_name(code: u32) -> &'static str {
    match code {
        NFS4_OK => "NFS4_OK",
        1 => "NFS4ERR_PERM",
        NFS4ERR_NOENT => "NFS4ERR_NOENT",
        NFS4ERR_STALE => "NFS4ERR_STALE",
        NFS4ERR_NOENT => "NFS4ERR_NOENT",
        NFS4ERR_EXIST => "NFS4ERR_EXIST",
        NFS4ERR_ACCESS => "NFS4ERR_ACCESS",
        NFS4ERR_DELAY => "NFS4ERR_DELAY",
        10013 => "NFS4ERR_GRACE",
        10015 => "NFS4ERR_SHARE_DENIED",
        10016 => "NFS4ERR_WRONGSEC",
        NFS4ERR_BADSESSION => "NFS4ERR_BADSESSION",
        NFS4ERR_BADSEQ => "NFS4ERR_BADSEQ",
        NFS4ERR_SEQ_MISORDERED => "NFS4ERR_SEQ_MISORDERED",
        _ => "NFS4ERR_UNKNOWN",
    }
}
