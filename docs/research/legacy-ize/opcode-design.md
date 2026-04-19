# Opcode Design for Filesystem Version Control

## Overview

This document defines the opcode system for capturing filesystem mutations in Ize. An **opcode** is a self-contained record of a single filesystem operation that can be:

1. **Stored** persistently for version history
2. **Replayed** to reconstruct filesystem state
3. **Transformed** into Pijul changes for distributed version control

The key insight: by the time an opcode is processed, the filesystem may have moved on. Therefore, each opcode must be **self-contained** with all data needed to understand and replay the operation.

## Design Principles

### 1. Self-Containment

Every opcode contains everything needed to understand and replay the operation:
- Full path (not inode - inodes are ephemeral)
- Complete data payload (not a reference to current filesystem state)
- Metadata at time of operation

### 2. Immutability

Opcodes are append-only. Once created, they never change. This enables:
- Safe concurrent processing
- Simple crash recovery
- Deterministic replay

### 3. Ordering

Opcodes have a monotonic sequence number. Operations on the same path must be applied in order. Operations on different paths are commutative (can be reordered).

### 4. Minimal but Complete

Capture the minimum information needed for:
- Replay (reconstruct the operation)
- Attribution (when, what, where)
- Optimization (deduplication, coalescing)

## Opcode Types

### File Operations

#### FileCreate

A new file is created.

```rust
FileCreate {
    path: PathBuf,         // Full relative path from working root
    mode: u32,             // Unix permissions (e.g., 0o644)
    content: Vec<u8>,      // Initial content (may be empty)
}
```

**When captured:** On `create()` FUSE call, or first `write()` to a new file.

**Notes:**
- If a file is created empty then written, we may see `FileCreate { content: [] }` followed by `FileWrite`. Coalescing can merge these.
- Mode includes file type bits but we only care about permission bits (lower 12 bits).

#### FileWrite

Data is written to an existing file.

```rust
FileWrite {
    path: PathBuf,         // Full relative path
    offset: u64,           // Byte offset where write begins
    data: Vec<u8>,         // Bytes written
}
```

**When captured:** On `write()` FUSE call.

**Notes:**
- Multiple writes may occur for a single user-level write (large writes are chunked).
- Offset + data.len() gives the end position.
- Sequential writes can be coalesced: if `write1.offset + write1.data.len() == write2.offset`, merge them.

#### FileTruncate

File size is changed (usually reduced, but can extend with zeros).

```rust
FileTruncate {
    path: PathBuf,         // Full relative path
    new_size: u64,         // New file size in bytes
}
```

**When captured:** On `setattr()` FUSE call with size change, or `truncate()` syscall.

**Notes:**
- Truncate to 0 + write is a common pattern for "overwrite file".
- Extending via truncate fills with zeros (sparse file behavior varies).

#### FileDelete

A file is removed (unlinked).

```rust
FileDelete {
    path: PathBuf,         // Full relative path (at time of deletion)
}
```

**When captured:** On `unlink()` FUSE call.

**Notes:**
- The file content is not stored in the opcode - it should already be in version history.
- If we need to support "undo delete", we'd restore from the last known state in Pijul.

#### FileRename

A file is moved or renamed.

```rust
FileRename {
    old_path: PathBuf,     // Original path
    new_path: PathBuf,     // New path
}
```

**When captured:** On `rename()` FUSE call where source is a file.

**Notes:**
- Can represent both rename-in-place (`dir/old.txt` → `dir/new.txt`) and move (`dir1/file.txt` → `dir2/file.txt`).
- If destination exists, it's an implicit delete + rename (atomic overwrite).

### Directory Operations

#### DirCreate

A new directory is created.

```rust
DirCreate {
    path: PathBuf,         // Full relative path
    mode: u32,             // Unix permissions (e.g., 0o755)
}
```

**When captured:** On `mkdir()` FUSE call.

**Notes:**
- `mkdir -p` generates multiple `DirCreate` opcodes (one per level).
- Empty directories are significant in Ize (unlike Git).

#### DirDelete

An empty directory is removed.

```rust
DirDelete {
    path: PathBuf,         // Full relative path
}
```

**When captured:** On `rmdir()` FUSE call.

**Notes:**
- Directory must be empty (FUSE enforces this).
- Recursive delete (`rm -rf`) generates `FileDelete` for each file, then `DirDelete` for each directory (bottom-up).

#### DirRename

A directory is moved or renamed.

```rust
DirRename {
    old_path: PathBuf,     // Original path
    new_path: PathBuf,     // New path
}
```

**When captured:** On `rename()` FUSE call where source is a directory.

**Notes:**
- All contents move with the directory (implicit path changes).
- This is a single opcode - we don't generate opcodes for each contained file.

### Metadata Operations

#### SetPermissions

File or directory permissions are changed.

```rust
SetPermissions {
    path: PathBuf,         // Full relative path
    mode: u32,             // New permission bits
}
```

**When captured:** On `setattr()` FUSE call with mode change, or `chmod()` syscall.

**Notes:**
- Only captures permission bits (lower 12 bits of mode).
- File type bits are immutable and not stored.

#### SetTimestamps

File or directory timestamps are explicitly modified.

```rust
SetTimestamps {
    path: PathBuf,         // Full relative path
    atime: Option<u64>,    // Access time (Unix timestamp, if changed)
    mtime: Option<u64>,    // Modification time (Unix timestamp, if changed)
}
```

**When captured:** On `setattr()` FUSE call with time changes, or `utimes()`/`touch` syscalls.

**Notes:**
- Only captures explicit timestamp changes, not implicit updates from writes.
- For versioning purposes, we may choose to not persist these (Git doesn't track timestamps).

#### SetOwnership

File or directory ownership is changed.

```rust
SetOwnership {
    path: PathBuf,         // Full relative path
    uid: Option<u32>,      // New owner UID (if changed)
    gid: Option<u32>,      // New group GID (if changed)
}
```

**When captured:** On `setattr()` FUSE call with uid/gid changes, or `chown()` syscall.

**Notes:**
- Ownership may not be meaningful for version control (different systems have different users).
- Consider making this optional/configurable.

### Symbolic Link Operations

#### SymlinkCreate

A symbolic link is created.

```rust
SymlinkCreate {
    path: PathBuf,         // Path of the symlink itself
    target: PathBuf,       // What the symlink points to (may be relative)
}
```

**When captured:** On `symlink()` FUSE call.

**Notes:**
- Target is stored as-is (may be relative or absolute).
- Absolute targets may cause portability issues.

#### SymlinkDelete

A symbolic link is removed.

```rust
SymlinkDelete {
    path: PathBuf,         // Path of the symlink
}
```

**When captured:** On `unlink()` FUSE call where target is a symlink.

**Notes:**
- Could potentially merge with `FileDelete` since both use `unlink()`.
- Keeping separate provides clearer semantics.

### Hard Link Operations

#### HardLinkCreate

A hard link is created.

```rust
HardLinkCreate {
    existing_path: PathBuf,  // Existing file to link to
    new_path: PathBuf,       // New link path
}
```

**When captured:** On `link()` FUSE call.

**Notes:**
- Hard links are tricky for version control (same content, multiple paths).
- Pijul handles this via its graph structure.
- May need special handling for path renames affecting hard links.

## Unified Opcode Enum

```rust
/// A single filesystem operation with all necessary context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opcode {
    /// Monotonic sequence number for ordering
    pub seq: u64,
    
    /// When the operation occurred (Unix timestamp in nanoseconds)
    pub timestamp: u64,
    
    /// The operation itself
    pub op: Operation,
}

/// The specific operation type and its data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    // File operations
    FileCreate { path: PathBuf, mode: u32, content: Vec<u8> },
    FileWrite { path: PathBuf, offset: u64, data: Vec<u8> },
    FileTruncate { path: PathBuf, new_size: u64 },
    FileDelete { path: PathBuf },
    FileRename { old_path: PathBuf, new_path: PathBuf },
    
    // Directory operations
    DirCreate { path: PathBuf, mode: u32 },
    DirDelete { path: PathBuf },
    DirRename { old_path: PathBuf, new_path: PathBuf },
    
    // Metadata operations
    SetPermissions { path: PathBuf, mode: u32 },
    SetTimestamps { path: PathBuf, atime: Option<u64>, mtime: Option<u64> },
    SetOwnership { path: PathBuf, uid: Option<u32>, gid: Option<u32> },
    
    // Symbolic links
    SymlinkCreate { path: PathBuf, target: PathBuf },
    SymlinkDelete { path: PathBuf },
    
    // Hard links
    HardLinkCreate { existing_path: PathBuf, new_path: PathBuf },
}
```

## Operation Matrix

| FUSE Method | Opcode(s) Generated | Data Captured |
|-------------|---------------------|---------------|
| `create` | `FileCreate` | path, mode, empty content |
| `write` | `FileWrite` | path, offset, data bytes |
| `setattr` (size) | `FileTruncate` | path, new_size |
| `setattr` (mode) | `SetPermissions` | path, mode |
| `setattr` (times) | `SetTimestamps` | path, atime, mtime |
| `setattr` (uid/gid) | `SetOwnership` | path, uid, gid |
| `unlink` (file) | `FileDelete` | path |
| `unlink` (symlink) | `SymlinkDelete` | path |
| `rename` (file) | `FileRename` | old_path, new_path |
| `rename` (dir) | `DirRename` | old_path, new_path |
| `mkdir` | `DirCreate` | path, mode |
| `rmdir` | `DirDelete` | path |
| `symlink` | `SymlinkCreate` | path, target |
| `link` | `HardLinkCreate` | existing_path, new_path |

## Coalescing Rules

To reduce storage and improve Pijul change efficiency, consecutive opcodes can be coalesced:

### Safe to Coalesce

1. **Sequential writes to same file:**
   ```
   FileWrite { path: "a.txt", offset: 0, data: "Hello" }
   FileWrite { path: "a.txt", offset: 5, data: " World" }
   → FileWrite { path: "a.txt", offset: 0, data: "Hello World" }
   ```

2. **Create + immediate writes:**
   ```
   FileCreate { path: "a.txt", mode: 0o644, content: [] }
   FileWrite { path: "a.txt", offset: 0, data: "Content" }
   → FileCreate { path: "a.txt", mode: 0o644, content: "Content" }
   ```

3. **Truncate to 0 + write (overwrite pattern):**
   ```
   FileTruncate { path: "a.txt", new_size: 0 }
   FileWrite { path: "a.txt", offset: 0, data: "New content" }
   → FileTruncate + FileWrite (or just consider it a full overwrite)
   ```

4. **Multiple permission changes:**
   ```
   SetPermissions { path: "a.txt", mode: 0o644 }
   SetPermissions { path: "a.txt", mode: 0o755 }
   → SetPermissions { path: "a.txt", mode: 0o755 }
   ```

### Never Coalesce

1. **Operations on different paths** - always keep separate
2. **Delete + Create (same path)** - this is a replacement, not an update
3. **Rename chains** - each rename is significant
4. **Operations separated by time threshold** (e.g., > 1 second)

## Path Resolution

Opcodes store **relative paths** from the working directory root. This requires translating FUSE inodes to paths at capture time.

### Inode → Path Strategy

1. **Maintain live mapping:** `PassthroughFS` already maintains `inode_to_path: Arc<RwLock<HashMap<u64, PathBuf>>>`
2. **Observers receive inodes:** Observer methods get `(parent_ino, name)` or just `ino`
3. **Resolve at notification time:** Observer uses shared `InodeMap` to resolve

### Path Handling Edge Cases

| Scenario | Resolution |
|----------|------------|
| File renamed after write queued | Store path at write time, not processing time |
| Parent directory renamed | Opcodes use path at capture time |
| File deleted then recreated | Separate opcodes, different file identities |
| Inode reused by OS | Path mapping is updated on new file creation |

## Serialization

Opcodes need efficient serialization for:
1. **Queue persistence** (crash recovery)
2. **Long-term storage** (version history)
3. **Network transfer** (future: sync)

### Format Options

| Format | Pros | Cons |
|--------|------|------|
| **bincode** | Fast, compact | Not human-readable |
| **MessagePack** | Compact, cross-language | Slightly slower than bincode |
| **JSON** | Human-readable, debuggable | Large, slow |
| **FlatBuffers** | Zero-copy reads | Complex schema management |

**Recommendation:** Use `bincode` for queue persistence (speed), with optional JSON export for debugging.

### Content Deduplication

For opcodes with large `data`/`content` fields:

1. **Hash the content:** SHA-256 or BLAKE3
2. **Store content once:** Content-addressed blob storage
3. **Reference by hash:** Opcode stores hash, not inline data

```rust
pub enum ContentRef {
    Inline(Vec<u8>),           // Small content (< 4KB)
    Hashed { hash: [u8; 32] }, // Large content stored separately
}
```

## Integration with FsObserver

The `FsObserver` trait maps directly to opcode generation:

```rust
impl FsObserver for OpcodeRecorder {
    fn on_create(&self, parent: u64, name: &OsStr, mode: u32, _result_ino: Option<u64>) {
        let path = self.resolve_path(parent, name);
        self.emit(Operation::FileCreate { 
            path, 
            mode, 
            content: Vec::new() 
        });
    }
    
    fn on_write(&self, ino: u64, _fh: u64, offset: i64, data: &[u8]) {
        let path = self.resolve_inode(ino);
        self.emit(Operation::FileWrite {
            path,
            offset: offset as u64,
            data: data.to_vec(),
        });
    }
    
    fn on_unlink(&self, parent: u64, name: &OsStr) {
        let path = self.resolve_path(parent, name);
        // Note: Would need to check if file vs symlink for proper opcode
        self.emit(Operation::FileDelete { path });
    }
    
    fn on_mkdir(&self, parent: u64, name: &OsStr, mode: u32, _result_ino: Option<u64>) {
        let path = self.resolve_path(parent, name);
        self.emit(Operation::DirCreate { path, mode });
    }
    
    fn on_rmdir(&self, parent: u64, name: &OsStr) {
        let path = self.resolve_path(parent, name);
        self.emit(Operation::DirDelete { path });
    }
    
    fn on_rename(&self, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr) {
        let old_path = self.resolve_path(parent, name);
        let new_path = self.resolve_path(newparent, newname);
        // Note: Would need to check if file vs dir for proper opcode
        self.emit(Operation::FileRename { old_path, new_path });
    }
    
    fn on_setattr(
        &self,
        ino: u64,
        size: Option<u64>,
        mode: Option<u32>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) {
        let path = self.resolve_inode(ino);
        
        if let Some(new_size) = size {
            self.emit(Operation::FileTruncate { path: path.clone(), new_size });
        }
        
        if let Some(mode) = mode {
            self.emit(Operation::SetPermissions { path: path.clone(), mode });
        }
        
        if atime.is_some() || mtime.is_some() {
            self.emit(Operation::SetTimestamps {
                path,
                atime: atime.map(|t| t.duration_since(UNIX_EPOCH).unwrap().as_secs()),
                mtime: mtime.map(|t| t.duration_since(UNIX_EPOCH).unwrap().as_secs()),
            });
        }
    }
}
```

## Pijul Change Mapping

Each opcode type maps to Pijul primitives:

| Opcode | Pijul Operation |
|--------|-----------------|
| `FileCreate` | Add file inode + content vertices |
| `FileWrite` | Modify content vertices (diff-based) |
| `FileTruncate` | Delete trailing content vertices |
| `FileDelete` | Delete file inode + content |
| `FileRename` | Rename file inode |
| `DirCreate` | Add directory inode |
| `DirDelete` | Delete directory inode |
| `DirRename` | Rename directory inode |
| `SetPermissions` | Update inode metadata |
| `SymlinkCreate` | Add symlink inode |
| `HardLinkCreate` | Add edge to existing content |

The mapping happens in the opcode processor, not in the opcode definition. This keeps opcodes simple and Pijul-agnostic.

## Testing Strategy

### Unit Tests

Each opcode type needs tests for:
1. **Serialization roundtrip:** `opcode == deserialize(serialize(opcode))`
2. **Path normalization:** Handles `.`, `..`, trailing slashes
3. **Edge cases:** Empty content, zero offset, max sizes

### Integration Tests (from existing test suite)

| Test | Opcodes Expected |
|------|------------------|
| `test_file_create_operation_tracked` | `FileCreate` or `FileCreate` + `FileWrite` |
| `test_file_write_operations_tracked` | Multiple `FileWrite`, possibly `FileTruncate` |
| `test_file_delete_operation_tracked` | `FileCreate` + `FileDelete` |
| `test_file_rename_operation_tracked` | `FileCreate` + `FileRename` |
| `test_mkdir_operation_tracked` | `DirCreate` |
| `test_nested_mkdir_operations_tracked` | Multiple `DirCreate` |
| `test_rmdir_operation_tracked` | `DirCreate` + `DirDelete` |
| `test_chmod_operation_tracked` | `FileCreate` + `SetPermissions` |
| `test_truncate_operation_tracked` | `FileCreate` + `FileWrite` + `FileTruncate` |
| `test_complex_file_operations_all_tracked` | Mix of all types |
| `test_concurrent_operations_all_tracked` | Concurrent opcodes, all captured |

### Property-Based Tests

```rust
proptest! {
    #[test]
    fn opcode_serialization_roundtrip(opcode in arbitrary_opcode()) {
        let serialized = serialize(&opcode);
        let deserialized = deserialize(&serialized);
        prop_assert_eq!(opcode, deserialized);
    }
    
    #[test]
    fn path_normalization_idempotent(path in "([a-z]+/)*[a-z]+") {
        let normalized = normalize_path(&path);
        let double_normalized = normalize_path(&normalized);
        prop_assert_eq!(normalized, double_normalized);
    }
}
```

## Future Considerations

### Extended Attributes (xattrs)

```rust
SetXattr { path: PathBuf, name: String, value: Vec<u8> },
RemoveXattr { path: PathBuf, name: String },
```

Not currently implemented in `PassthroughFS`, but the opcode system should be extensible.

### Access Control Lists (ACLs)

Could be modeled as special xattrs, or as dedicated opcodes if ACL support is added.

### Sparse Files

`FileWrite` with large offsets creates implicit zeros. May need:
```rust
FileSparseExtend { path: PathBuf, new_size: u64 },
```

### Batch Operations

For efficiency, may want:
```rust
Batch { opcodes: Vec<Operation> },
```

This would allow atomic multi-file operations and reduce overhead.

## Summary

The opcode system provides a clean abstraction layer between:
1. **FUSE operations** (low-level, inode-based, ephemeral)
2. **Version control** (path-based, content-addressed, persistent)

By capturing self-contained opcodes at the observer layer, we decouple the immediate filesystem response from the versioning backend. This enables:
- Async versioning without blocking filesystem operations
- Clean separation of concerns
- Testable components
- Future flexibility (different backends, network sync, etc.)