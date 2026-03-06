#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

#define MAX_FILENAME 256
#define EVENT_VERSION 5
#define MAX_AGGREGATE_BYTES 1048576

#ifndef ATTR_MODE
#define ATTR_MODE   1
#endif
#ifndef ATTR_UID
#define ATTR_UID    2
#endif
#ifndef ATTR_GID
#define ATTR_GID    4
#endif
#ifndef ATTR_SIZE
#define ATTR_SIZE   8
#endif
#ifndef ATTR_ATIME
#define ATTR_ATIME  16
#endif
#ifndef ATTR_MTIME
#define ATTR_MTIME  32
#endif
#ifndef ATTR_CTIME
#define ATTR_CTIME  64
#endif

// Forward declarations if missing in vmlinux
struct renamedata___p {
    struct user_namespace *old_mnt_userns;
    struct dentry *old_dentry;
    struct inode *old_dir;
    struct dentry *new_dentry;
    struct inode *new_dir;
    struct inode **delegated_inode;
    unsigned int flags;
} __attribute__((preserve_access_index));

struct fileattr___p {
    u32 flags;
    u32 fsx_flags;
} __attribute__((preserve_access_index));

struct file_lock___p {
    unsigned char fl_type;
    unsigned int fl_flags;
} __attribute__((preserve_access_index));

struct file_lock_core___p {
    unsigned char flc_type;
    unsigned int flc_flags;
} __attribute__((preserve_access_index));

struct file_lock___split {
    struct file_lock_core___p c;
} __attribute__((preserve_access_index));

struct kprojid_t___p { int val; };
struct inode___p { struct kprojid_t___p i_projid; } __attribute__((preserve_access_index));

// XFS Internal Structs (Local CO-RE definitions)
struct xfs_mount {
    struct super_block *m_super;
} __attribute__((preserve_access_index));

struct xfs_trans {
    struct xfs_mount *t_mountp;
} __attribute__((preserve_access_index));

enum event_type {
    EVENT_WRITE=1,
    EVENT_WRITE_RANGE=2,
    EVENT_SETXATTR=3,
    EVENT_REMOVEXATTR=4,
    EVENT_RMDIR=5,
    EVENT_FSYNC=6,
    EVENT_RENAME=7,
    EVENT_CREATE=8,
    EVENT_UNLINK=9,
    EVENT_MKDIR=10,
    EVENT_TRUNCATE=11,
    EVENT_LINK=12,
    EVENT_CHMOD=13,
    EVENT_CHOWN=14,
    EVENT_BARRIER=15,
    EVENT_MKNOD=16,
    EVENT_SYMLINK=17,
    EVENT_FALLOCATE=18,
    EVENT_UTIMES=19,
    EVENT_SETFLAGS=20,
    EVENT_LOCK=21,
    EVENT_FLOCK=22,
    EVENT_RENAME_INCOMPLETE=23,
    EVENT_CLONE=24,
    EVENT_SEQUENCE_GAP=255
};

struct event {
    __u8 type;
    __u8 version;
    __u8 interactive;
    __u8 _pad0[1];
    __u32 dev_id;
    __u64 seq_num;
    __u64 timestamp_ns;
    __u64 parent_inode;
    __u64 inode;
    __u64 new_parent_inode;
    __u32 generation;
    __u32 mode;
    __u64 offset;
    __u64 length;
    __u32 uid;
    __u32 gid;
    __u32 nlink;
    __u32 flags;
    __u64 file_size;
    __u32 projid;
    __u32 _pad1;
    char name[MAX_FILENAME];
    char new_name[MAX_FILENAME];
    char comm[16];
};

struct dirty_key {
    __u64 inode_id;
    __u64 page_idx;
};

struct pending_write {
    __u64 start_offset;
    __u64 length;
    __u64 last_ts;
    __u32 flags;
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} local_seq_map SEC(".maps");

struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 64); __type(key, __u32); __type(value, __u8); } watched_devs SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 16); __type(key, __u32); __type(value, __u8); } ignored_pids SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY); __uint(max_entries, 1); __type(key, __u32); __type(value, struct stats); } statistics SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_RINGBUF); __uint(max_entries, 33554432); } events SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 65536); __type(key, __u64); __type(value, __u64); } temp_dentries SEC(".maps");
// Separate stash for security_inode_create → d_instantiate flow to avoid
// stealing dentries stashed by vfs_mkdir/vfs_link/etc. entry probes.
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 65536); __type(key, __u64); __type(value, __u64); } create_dentries SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 65536);
    __type(key, struct dirty_key);
    __type(value, __u64);
} dirty_pages SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, 8192);
    __type(key, __u64);
    __type(value, struct pending_write);
} write_aggregator SEC(".maps");

struct rename_args {
    struct dentry *old_dentry;
    struct dentry *new_dentry;
    __u32 flags;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, __u64);
    __type(value, struct rename_args);
} pending_renames SEC(".maps");

struct stats {
    __u64 events_submitted;
    __u64 events_dropped;
    __u64 write_events;
    __u64 metadata_events;
    __u64 incomplete_rename;
};

static __always_inline __u64 get_next_seq() {
    __u32 key = 0;
    __u64 *seq = bpf_map_lookup_elem(&local_seq_map, &key);
    if (!seq) return 0;
    return __sync_fetch_and_add(seq, 1);
}

static __always_inline int is_ignored_pid() {
    __u64 id = bpf_get_current_pid_tgid();
    __u32 tgid = id >> 32;
    if (bpf_map_lookup_elem(&ignored_pids, &tgid)) return 1;
    return 0;
}

static __always_inline int stash_dentry(struct dentry *dentry) {
    if (is_ignored_pid()) return 0;
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u64 ptr = (__u64)dentry;
    return bpf_map_update_elem(&temp_dentries, &pid_tgid, &ptr, BPF_ANY);
}

static __always_inline __u32 normalize_dev_id(__u32 raw_dev) {
    return raw_dev;
}

static __always_inline void mark_dirty(struct inode *inode, __u64 offset) {
    if (!inode) return;
    if (is_ignored_pid()) return;
    struct dirty_key key;
    key.inode_id = BPF_CORE_READ(inode, i_ino);
    key.page_idx = offset >> 12;
    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&dirty_pages, &key, &ts, BPF_ANY);
}

static __always_inline int flush_pending_write(struct inode *inode, struct dentry *dentry) {
    if (!inode) return 0;
    struct super_block *sb = BPF_CORE_READ(inode, i_sb);
    __u32 raw_dev_id = BPF_CORE_READ(sb, s_dev);
    __u32 dev_id = normalize_dev_id(raw_dev_id);
    if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;

    __u64 key = BPF_CORE_READ(inode, i_ino);
    struct pending_write *pw = bpf_map_lookup_elem(&write_aggregator, &key);
    if (!pw) return 0;

    __u64 seq_num = get_next_seq();
    if (seq_num == 0) return 0;

    struct event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) {
        __u32 z=0; struct stats *s = bpf_map_lookup_elem(&statistics, &z);
        if (s) __sync_fetch_and_add(&s->events_dropped, 1);
        return 0;
    }

    __builtin_memset(e, 0, sizeof(*e));
    struct task_struct *task = (struct task_struct *)bpf_get_current_task();
    struct signal_struct *signal = BPF_CORE_READ(task, signal);
    struct tty_struct *tty = BPF_CORE_READ(signal, tty);
    e->interactive = (tty != NULL) ? 1 : 0;
    bpf_get_current_comm(&e->comm, sizeof(e->comm));

    e->type = EVENT_WRITE_RANGE;
    e->version = EVENT_VERSION;
    e->dev_id = dev_id;
    e->seq_num = seq_num;
    e->timestamp_ns = pw->last_ts;
    e->inode = key;
    e->generation = BPF_CORE_READ(inode, i_generation);
    e->mode = BPF_CORE_READ(inode, i_mode);
    e->nlink = BPF_CORE_READ(inode, i_nlink);
    e->file_size = BPF_CORE_READ(inode, i_size);
    e->uid = BPF_CORE_READ(inode, i_uid.val);
    e->gid = BPF_CORE_READ(inode, i_gid.val);
    e->offset = pw->start_offset;
    e->length = pw->length;
    e->flags = pw->flags;

    struct inode___p *ip = (struct inode___p *)inode;
    if (bpf_core_field_exists(ip->i_projid)) {
        e->projid = BPF_CORE_READ(ip, i_projid.val);
    } else {
        e->projid = 0;
    }

    if (dentry) {
        struct dentry *parent = BPF_CORE_READ(dentry, d_parent);
        if (parent) {
            struct inode *pi = BPF_CORE_READ(parent, d_inode);
            if (pi) e->parent_inode = BPF_CORE_READ(pi, i_ino);
        }
        const unsigned char *name_ptr = BPF_CORE_READ(dentry, d_name.name);
        bpf_core_read_str(&e->name, sizeof(e->name), (const char *)name_ptr);
    }

    bpf_ringbuf_submit(e, 0);
    bpf_map_delete_elem(&write_aggregator, &key);

    __u32 z=0; struct stats *s = bpf_map_lookup_elem(&statistics, &z);
    if (s) {
        __sync_fetch_and_add(&s->events_submitted, 1);
        __sync_fetch_and_add(&s->write_events, 1);
    }
    return 0;
}

static __always_inline int submit_event_raw(struct inode *inode, struct dentry *dentry, enum event_type type, __u64 offset, __u64 length, __u32 flags, int check_pid) {
    if (!inode) return 0;
    if (check_pid && is_ignored_pid()) return 0;
    if (!dentry && (type == EVENT_RENAME || type == EVENT_CREATE || type == EVENT_MKDIR)) return 0;

    struct super_block *sb = BPF_CORE_READ(inode, i_sb);
    __u32 raw_dev_id = BPF_CORE_READ(sb, s_dev);
    __u32 dev_id = normalize_dev_id(raw_dev_id);
    if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;

    __u64 final_offset = offset;
    __u64 final_length = length;
    __u64 final_ts = bpf_ktime_get_ns();

    if (type == EVENT_WRITE_RANGE) {
        __u64 key = BPF_CORE_READ(inode, i_ino);
        struct pending_write *pw = bpf_map_lookup_elem(&write_aggregator, &key);
        if (pw) {
            if (pw->start_offset + pw->length == offset && 
                pw->length + length <= MAX_AGGREGATE_BYTES &&
                pw->flags == flags) { 
                pw->length += length;
                pw->last_ts = final_ts;
                return 0;
            }
            final_offset = pw->start_offset;
            final_length = pw->length;
            final_ts = pw->last_ts;
            flags = pw->flags; 
            pw->start_offset = offset;
            pw->length = length;
            pw->last_ts = bpf_ktime_get_ns();
            pw->flags = flags;
        } else {
            struct pending_write new_pw;
            new_pw.start_offset = offset;
            new_pw.length = length;
            new_pw.last_ts = final_ts;
            new_pw.flags = flags;
            bpf_map_update_elem(&write_aggregator, &key, &new_pw, BPF_ANY);
            return 0;
        }
    }

    __u64 seq_num = get_next_seq();
    if (seq_num == 0) return 0;

    struct event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) {
        __u32 z=0; struct stats *s = bpf_map_lookup_elem(&statistics, &z);
        if (s) __sync_fetch_and_add(&s->events_dropped, 1);
        return 0;
    }

    __builtin_memset(e, 0, sizeof(*e));
    struct task_struct *task = (struct task_struct *)bpf_get_current_task();
    struct signal_struct *signal = BPF_CORE_READ(task, signal);
    struct tty_struct *tty = BPF_CORE_READ(signal, tty);
    e->interactive = (tty != NULL) ? 1 : 0;
    bpf_get_current_comm(&e->comm, sizeof(e->comm));

    e->type = type;
    e->version = EVENT_VERSION;
    e->dev_id = dev_id;
    e->seq_num = seq_num;
    e->timestamp_ns = final_ts;
    e->inode = BPF_CORE_READ(inode, i_ino);
    e->generation = BPF_CORE_READ(inode, i_generation);
    e->mode = BPF_CORE_READ(inode, i_mode);
    e->nlink = BPF_CORE_READ(inode, i_nlink);
    e->file_size = BPF_CORE_READ(inode, i_size);
    e->uid = BPF_CORE_READ(inode, i_uid.val);
    e->gid = BPF_CORE_READ(inode, i_gid.val);
    e->offset = final_offset;
    e->length = final_length;
    e->flags = flags;

    struct inode___p *ip = (struct inode___p *)inode;
    if (bpf_core_field_exists(ip->i_projid)) {
        e->projid = BPF_CORE_READ(ip, i_projid.val);
    } else {
        e->projid = 0;
    }

    if (dentry) {
        struct dentry *parent = BPF_CORE_READ(dentry, d_parent);
        if (parent) {
            struct inode *pi = BPF_CORE_READ(parent, d_inode);
            if (pi) e->parent_inode = BPF_CORE_READ(pi, i_ino);
        }
        const unsigned char *name_ptr = BPF_CORE_READ(dentry, d_name.name);
        bpf_core_read_str(&e->name, sizeof(e->name), (const char *)name_ptr);
    }

    bpf_ringbuf_submit(e, 0);

    __u32 z=0; struct stats *s = bpf_map_lookup_elem(&statistics, &z);
    if (s) {
        __sync_fetch_and_add(&s->events_submitted, 1);
        if (type==EVENT_WRITE||type==EVENT_WRITE_RANGE||type==EVENT_CLONE) __sync_fetch_and_add(&s->write_events, 1);
        else __sync_fetch_and_add(&s->metadata_events, 1);
    }
    return 0;
}

static __always_inline int submit_event(struct inode *inode, struct dentry *dentry, enum event_type type, __u64 offset, __u64 length, __u32 flags) {
    return submit_event_raw(inode, dentry, type, offset, length, flags, 1);
}

static __always_inline int process_stashed_dentry(int ret, enum event_type type) {
    if (ret != 0) return 0;
    if (is_ignored_pid()) return 0;
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u64 *ptr = bpf_map_lookup_elem(&temp_dentries, &pid_tgid);
    if (ptr) {
        struct dentry *dentry = (struct dentry *)(*ptr);
        struct inode *inode = BPF_CORE_READ(dentry, d_inode);
        if (inode) submit_event(inode, dentry, type, 0, 0, 0);
        bpf_map_delete_elem(&temp_dentries, &pid_tgid);
    }
    return 0;
}

SEC("kprobe/vfs_write_iter")
int BPF_KPROBE(trace_vfs_write_iter, struct kiocb *iocb, struct iov_iter *from) {
    struct inode *inode = BPF_CORE_READ(iocb, ki_filp, f_inode);
    __u64 pos = BPF_CORE_READ(iocb, ki_pos);
    size_t count = BPF_CORE_READ(from, count);
    u32 ki_flags = BPF_CORE_READ(iocb, ki_flags);
    mark_dirty(inode, pos);
    return submit_event(inode, BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, pos, count, ki_flags);
}

SEC("kprobe/xfs_file_write_iter")
int BPF_KPROBE(trace_xfs_write_iter, struct kiocb *iocb, struct iov_iter *from) {
    struct inode *inode = BPF_CORE_READ(iocb, ki_filp, f_inode);
    __u64 pos = BPF_CORE_READ(iocb, ki_pos);
    size_t count = BPF_CORE_READ(from, count);
    u32 ki_flags = BPF_CORE_READ(iocb, ki_flags);
    mark_dirty(inode, pos);
    return submit_event(inode, BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, pos, count, ki_flags);
}

SEC("kprobe/btrfs_file_write_iter")
int BPF_KPROBE(trace_btrfs_write_iter, struct kiocb *iocb, struct iov_iter *from) {
    struct inode *inode = BPF_CORE_READ(iocb, ki_filp, f_inode);
    __u64 pos = BPF_CORE_READ(iocb, ki_pos);
    size_t count = BPF_CORE_READ(from, count);
    u32 ki_flags = BPF_CORE_READ(iocb, ki_flags);
    mark_dirty(inode, pos);
    return submit_event(inode, BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, pos, count, ki_flags);
}

SEC("kprobe/ext4_file_write_iter")
int BPF_KPROBE(trace_ext4_write_iter, struct kiocb *iocb, struct iov_iter *from) {
    struct inode *inode = BPF_CORE_READ(iocb, ki_filp, f_inode);
    __u64 pos = BPF_CORE_READ(iocb, ki_pos);
    size_t count = BPF_CORE_READ(from, count);
    u32 ki_flags = BPF_CORE_READ(iocb, ki_flags);
    mark_dirty(inode, pos);
    return submit_event(inode, BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, pos, count, ki_flags);
}

SEC("kprobe/f2fs_file_write_iter")
int BPF_KPROBE(trace_f2fs_write_iter, struct kiocb *iocb, struct iov_iter *from) {
    struct inode *inode = BPF_CORE_READ(iocb, ki_filp, f_inode);
    __u64 pos = BPF_CORE_READ(iocb, ki_pos);
    size_t count = BPF_CORE_READ(from, count);
    u32 ki_flags = BPF_CORE_READ(iocb, ki_flags);
    mark_dirty(inode, pos);
    return submit_event(inode, BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, pos, count, ki_flags);
}

SEC("kprobe/nfs_file_write")
int BPF_KPROBE(trace_nfs_file_write, struct kiocb *iocb, struct iov_iter *from) {
    struct inode *inode = BPF_CORE_READ(iocb, ki_filp, f_inode);
    __u64 pos = BPF_CORE_READ(iocb, ki_pos);
    size_t count = BPF_CORE_READ(from, count);
    u32 ki_flags = BPF_CORE_READ(iocb, ki_flags);
    mark_dirty(inode, pos);
    return submit_event(inode, BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, pos, count, ki_flags);
}

SEC("kprobe/vfs_fsync")
int BPF_KPROBE(trace_vfs_fsync, struct file *file, loff_t start, loff_t end, int datasync) {
    struct inode *inode = BPF_CORE_READ(file, f_inode);
    struct dentry *dentry = BPF_CORE_READ(file, f_path.dentry);
    flush_pending_write(inode, dentry);
    return submit_event(inode, dentry, EVENT_FSYNC, 0, 0, 0);
}

SEC("kprobe/vfs_rename")
int BPF_KPROBE(trace_rename_entry, struct renamedata___p *rd) {
    if (is_ignored_pid()) return 0;
    struct dentry *old_dentry = BPF_CORE_READ(rd, old_dentry);
    if (!old_dentry) return 0;
    struct inode *inode = BPF_CORE_READ(old_dentry, d_inode);
    if (!inode) return 0;

    struct super_block *sb = BPF_CORE_READ(inode, i_sb);
    __u32 raw_dev_id = BPF_CORE_READ(sb, s_dev);
    __u32 dev_id = normalize_dev_id(raw_dev_id);
    if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;

    __u64 pid = bpf_get_current_pid_tgid();
    struct rename_args args;
    args.old_dentry = old_dentry;
    args.new_dentry = BPF_CORE_READ(rd, new_dentry);
    args.flags = 0;
    if (bpf_core_field_exists(rd->flags)) {
        args.flags = BPF_CORE_READ(rd, flags);
    }
    bpf_map_update_elem(&pending_renames, &pid, &args, BPF_ANY);
    return 0;
}

SEC("kretprobe/vfs_rename")
int BPF_KRETPROBE(trace_rename_exit, int ret) {
    __u64 pid = bpf_get_current_pid_tgid();
    struct rename_args *args = bpf_map_lookup_elem(&pending_renames, &pid);
    if (!args) return 0;

    // Only emit event if kernel operation succeeded
    if (ret != 0) {
        bpf_map_delete_elem(&pending_renames, &pid);
        return 0;
    }

    struct dentry *old_dentry = args->old_dentry;
    struct dentry *new_dentry = args->new_dentry;
    __u32 rename_flags = args->flags;
    bpf_map_delete_elem(&pending_renames, &pid);

    struct inode *inode = BPF_CORE_READ(old_dentry, d_inode);
    if (!inode) return 0;

    struct super_block *sb = BPF_CORE_READ(inode, i_sb);
    __u32 dev_id = normalize_dev_id(BPF_CORE_READ(sb, s_dev));

    __u64 seq_num = get_next_seq();
    if (seq_num == 0) return 0;

    struct event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) {
        __u32 z=0; struct stats *s = bpf_map_lookup_elem(&statistics, &z);
        if (s) __sync_fetch_and_add(&s->events_dropped, 1);
        return 0;
    }

    __builtin_memset(e, 0, sizeof(*e));
    struct task_struct *task = (struct task_struct *)bpf_get_current_task();
    struct signal_struct *signal = BPF_CORE_READ(task, signal);
    struct tty_struct *tty = BPF_CORE_READ(signal, tty);
    e->interactive = (tty != NULL) ? 1 : 0;
    bpf_get_current_comm(&e->comm, sizeof(e->comm));

    e->type = EVENT_RENAME;
    e->version = EVENT_VERSION;
    e->dev_id = dev_id;
    e->seq_num = seq_num;
    e->timestamp_ns = bpf_ktime_get_ns();
    e->inode = BPF_CORE_READ(inode, i_ino);
    e->generation = BPF_CORE_READ(inode, i_generation);
    e->flags = rename_flags;

    struct dentry *op = BPF_CORE_READ(old_dentry, d_parent);
    if (op) e->parent_inode = BPF_CORE_READ(op, d_inode, i_ino);

    const unsigned char *old_name_ptr = BPF_CORE_READ(old_dentry, d_name.name);
    bpf_core_read_str(&e->name, sizeof(e->name), (const char *)old_name_ptr);

    __u64 new_parent_ino = 0;
    if (new_dentry) {
        const unsigned char *new_name_ptr = BPF_CORE_READ(new_dentry, d_name.name);
        bpf_core_read_str(&e->new_name, sizeof(e->new_name), (const char *)new_name_ptr);
        struct dentry *new_p = BPF_CORE_READ(new_dentry, d_parent);
        struct inode *new_p_inode = BPF_CORE_READ(new_p, d_inode);
        if (new_p_inode) {
            new_parent_ino = BPF_CORE_READ(new_p_inode, i_ino);
        }
    }
    e->new_parent_inode = new_parent_ino;

    if (e->new_parent_inode == 0 || e->new_name[0] == 0) {
        __u32 z=0; struct stats *s = bpf_map_lookup_elem(&statistics, &z);
        if (s) __sync_fetch_and_add(&s->incomplete_rename, 1);
        e->type = EVENT_RENAME_INCOMPLETE;
    }

    bpf_ringbuf_submit(e, 0);
    return 0;
}

SEC("kprobe/vfs_create")
int BPF_KPROBE(trace_create_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_create")
int BPF_KRETPROBE(trace_create_exit, int ret) { return process_stashed_dentry(ret, EVENT_CREATE); }

// Fallback for kernel 6.12+ where file creation via openat(O_CREAT) bypasses vfs_create.
// Uses a SEPARATE stash map (create_dentries) to avoid stealing dentries stashed by
// vfs_mkdir/vfs_link/etc. entry probes in temp_dentries.
SEC("kprobe/security_inode_create")
int BPF_KPROBE(trace_security_create_entry, struct inode *dir, struct dentry *dentry, umode_t mode) {
    if (is_ignored_pid()) return 0;
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u64 ptr = (__u64)dentry;
    return bpf_map_update_elem(&create_dentries, &pid_tgid, &ptr, BPF_ANY);
}
SEC("kprobe/d_instantiate")
int BPF_KPROBE(trace_d_instantiate, struct dentry *dentry, struct inode *inode) {
    if (!inode) return 0;
    if (is_ignored_pid()) return 0;
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u64 *ptr = bpf_map_lookup_elem(&create_dentries, &pid_tgid);
    if (!ptr) return 0;
    struct dentry *stashed = (struct dentry *)(*ptr);
    bpf_map_delete_elem(&create_dentries, &pid_tgid);
    // Use the inode parameter directly — d_inode isn't set yet at kprobe entry.
    return submit_event(inode, stashed, EVENT_CREATE, 0, 0, 0);
}

SEC("kprobe/vfs_mkdir")
int BPF_KPROBE(trace_mkdir_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_mkdir")
int BPF_KRETPROBE(trace_mkdir_exit, int ret) { return process_stashed_dentry(ret, EVENT_MKDIR); }

SEC("kprobe/vfs_mknod")
int BPF_KPROBE(trace_mknod_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_mknod")
int BPF_KRETPROBE(trace_mknod_exit, int ret) { return process_stashed_dentry(ret, EVENT_MKNOD); }

SEC("kprobe/vfs_link")
int BPF_KPROBE(trace_link_entry, struct dentry *old, void *id, void *dir, struct dentry *new_dentry) { return stash_dentry(new_dentry); }
SEC("kretprobe/vfs_link")
int BPF_KRETPROBE(trace_link_exit, int ret) { return process_stashed_dentry(ret, EVENT_LINK); }

SEC("kprobe/vfs_symlink")
int BPF_KPROBE(trace_symlink_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_symlink")
int BPF_KRETPROBE(trace_symlink_exit, int ret) { return process_stashed_dentry(ret, EVENT_SYMLINK); }

SEC("kprobe/vfs_unlink")
int BPF_KPROBE(trace_unlink_entry, void *idmap, struct inode *dir, struct dentry *dentry) {
    return stash_dentry(dentry);
}
SEC("kretprobe/vfs_unlink")
int BPF_KRETPROBE(trace_unlink_exit, int ret) {
    return process_stashed_dentry(ret, EVENT_UNLINK);
}

SEC("kprobe/vfs_rmdir")
int BPF_KPROBE(trace_rmdir_entry, void *idmap, struct inode *dir, struct dentry *dentry) {
    return stash_dentry(dentry);
}
SEC("kretprobe/vfs_rmdir")
int BPF_KRETPROBE(trace_rmdir_exit, int ret) {
    return process_stashed_dentry(ret, EVENT_RMDIR);
}

SEC("kprobe/notify_change")
int BPF_KPROBE(trace_notify_change, struct user_namespace *mnt_userns, struct dentry *dentry, struct iattr *attr) {
    struct inode *inode = BPF_CORE_READ(dentry, d_inode);
    unsigned int ia_valid = BPF_CORE_READ(attr, ia_valid);
    if (ia_valid & ATTR_SIZE) return submit_event(inode, dentry, EVENT_TRUNCATE, 0, BPF_CORE_READ(attr, ia_size), 0);
    if (ia_valid & ATTR_MODE) return submit_event(inode, dentry, EVENT_CHMOD, 0, 0, 0);
    if ((ia_valid & ATTR_UID) || (ia_valid & ATTR_GID)) return submit_event(inode, dentry, EVENT_CHOWN, 0, 0, 0);
    if ((ia_valid & ATTR_ATIME) || (ia_valid & ATTR_MTIME)) return submit_event(inode, dentry, EVENT_UTIMES, 0, 0, 0);
    return 0;
}

SEC("kprobe/vfs_setxattr")
int BPF_KPROBE(trace_setxattr, struct dentry *dentry) {
    return submit_event(BPF_CORE_READ(dentry, d_inode), dentry, EVENT_SETXATTR, 0, 0, 0);
}

SEC("kprobe/vfs_removexattr")
int BPF_KPROBE(trace_removexattr, struct dentry *dentry) {
    return submit_event(BPF_CORE_READ(dentry, d_inode), dentry, EVENT_REMOVEXATTR, 0, 0, 0);
}

SEC("kprobe/vfs_fileattr_set")
int BPF_KPROBE(trace_fileattr, struct user_namespace *mnt_userns, struct dentry *dentry, struct fileattr___p *fa) {
    u32 flags = 0;
    if (bpf_core_field_exists(fa->flags)) {
        flags = BPF_CORE_READ(fa, flags);
    }
    return submit_event(BPF_CORE_READ(dentry, d_inode), dentry, EVENT_SETFLAGS, 0, 0, flags);
}

SEC("kprobe/vfs_lock_file")
int BPF_KPROBE(trace_vfs_lock_file, struct file *filp, unsigned int cmd, struct file_lock___p *fl) {
    struct inode *inode = BPF_CORE_READ(filp, f_inode);
    struct dentry *dentry = BPF_CORE_READ(filp, f_path.dentry);
    unsigned char fl_type = 0;
    unsigned int fl_flags = 0;
    if (bpf_core_type_exists(struct file_lock_core___p)) {
        struct file_lock___split *fl_split = (void *)fl;
        fl_type = BPF_CORE_READ(fl_split, c.flc_type);
        fl_flags = BPF_CORE_READ(fl_split, c.flc_flags);
    } else {
        fl_type = BPF_CORE_READ(fl, fl_type);
        fl_flags = BPF_CORE_READ(fl, fl_flags);
    }
    return submit_event(inode, dentry, EVENT_LOCK, 0, 0, fl_flags | (fl_type << 24));
}

SEC("kprobe/flock_lock_inode_wait")
int BPF_KPROBE(trace_flock_lock_inode_wait, struct inode *inode, struct file_lock___p *fl) {
    unsigned char fl_type = 0;
    unsigned int fl_flags = 0;
    if (bpf_core_type_exists(struct file_lock_core___p)) {
        struct file_lock___split *fl_split = (void *)fl;
        fl_type = BPF_CORE_READ(fl_split, c.flc_type);
        fl_flags = BPF_CORE_READ(fl_split, c.flc_flags);
    } else {
        fl_type = BPF_CORE_READ(fl, fl_type);
        fl_flags = BPF_CORE_READ(fl, fl_flags);
    }
    return submit_event(inode, NULL, EVENT_FLOCK, 0, 0, fl_flags | (fl_type << 24));
}

SEC("kprobe/flock_lock_inode")
int BPF_KPROBE(trace_flock_lock_inode, struct inode *inode, struct file_lock___p *fl) {
    unsigned char fl_type = 0;
    unsigned int fl_flags = 0;
    if (bpf_core_type_exists(struct file_lock_core___p)) {
        struct file_lock___split *fl_split = (void *)fl;
        fl_type = BPF_CORE_READ(fl_split, c.flc_type);
        fl_flags = BPF_CORE_READ(fl_split, c.flc_flags);
    } else {
        fl_type = BPF_CORE_READ(fl, fl_type);
        fl_flags = BPF_CORE_READ(fl, fl_flags);
    }
    return submit_event(inode, NULL, EVENT_FLOCK, 0, 0, fl_flags | (fl_type << 24));
}

SEC("kprobe/vfs_fallocate")
int BPF_KPROBE(trace_fallocate, struct file *file, int mode, loff_t offset, loff_t len) {
    return submit_event(BPF_CORE_READ(file, f_inode), BPF_CORE_READ(file, f_path.dentry), EVENT_FALLOCATE, offset, len, mode);
}

// XFS Commit: Use void* to avoid type conflict with vmlinux.h, then cast locally
SEC("kprobe/xfs_trans_commit")
int BPF_KPROBE(trace_xfs_commit, void *tp) {
    struct xfs_trans *trans = (struct xfs_trans *)tp;
    struct xfs_mount *mp = BPF_CORE_READ(trans, t_mountp);
    struct super_block *sb = BPF_CORE_READ(mp, m_super);
    __u32 raw_dev_id = BPF_CORE_READ(sb, s_dev);
    __u32 dev_id = normalize_dev_id(raw_dev_id);
    if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;

    __u64 seq_num = get_next_seq();
    if (seq_num == 0) return 0;

    struct event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) return 0;

    __builtin_memset(e, 0, sizeof(*e));
    e->type = EVENT_BARRIER;
    e->version = EVENT_VERSION;
    e->dev_id = dev_id;
    e->seq_num = seq_num;
    e->timestamp_ns = bpf_ktime_get_ns();
    bpf_ringbuf_submit(e, 0);
    return 0;
}

SEC("kprobe/__filemap_fdatawrite_range")
int BPF_KPROBE(trace_filemap_fdatawrite_range, struct address_space *mapping, loff_t start, loff_t end, int sync_mode) {
    struct inode *inode = BPF_CORE_READ(mapping, host);
    if (!inode) return 0;
    struct dirty_key key;
    key.inode_id = BPF_CORE_READ(inode, i_ino);
    key.page_idx = start >> 12;
    __u64 *ts = bpf_map_lookup_elem(&dirty_pages, &key);
    if (ts) {
        return 0;
    }
    __u64 len = 0;
    if (end >= start) {
        len = end - start + 1;
    }
    return submit_event_raw(inode, NULL, EVENT_WRITE_RANGE, start, len, 0, 0);
}

SEC("kprobe/vfs_copy_file_range")
int BPF_KPROBE(trace_copy_file_range, struct file *file_in, loff_t pos_in, struct file *file_out, loff_t pos_out, size_t len, unsigned int flags) {
    struct inode *inode = BPF_CORE_READ(file_out, f_inode);
    struct dentry *dentry = BPF_CORE_READ(file_out, f_path.dentry);
    return submit_event(inode, dentry, EVENT_WRITE_RANGE, pos_out, len, 0);
}

SEC("kprobe/vfs_clone_file_range")
int BPF_KPROBE(trace_clone_file_range, struct file *file_in, loff_t pos_in, struct file *file_out, loff_t pos_out, u64 len, unsigned int remap_flags) {
    struct inode *inode = BPF_CORE_READ(file_out, f_inode);
    struct dentry *dentry = BPF_CORE_READ(file_out, f_path.dentry);
    return submit_event(inode, dentry, EVENT_CLONE, pos_out, len, remap_flags);
}

char LICENSE[] SEC("license") = "GPL";
