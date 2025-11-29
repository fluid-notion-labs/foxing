#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

#define MAX_FILENAME 256
#define EVENT_VERSION 2

// Attributes flags from linux/fs.h
#define ATTR_MODE   1
#define ATTR_UID    2
#define ATTR_GID    4
#define ATTR_SIZE   8
#define ATTR_ATIME  16
#define ATTR_MTIME  32
#define ATTR_CTIME  64

struct kprojid_t___p { int val; };
struct inode___p { struct kprojid_t___p i_projid; } __attribute__((preserve_access_index));

struct xfs_mount { struct super_block *m_super; } __attribute__((preserve_access_index));
struct xfs_trans { struct xfs_mount *t_mountp; } __attribute__((preserve_access_index));

enum event_type {
    EVENT_WRITE=1, EVENT_WRITE_RANGE=2, EVENT_SETXATTR=3, EVENT_REMOVEXATTR=4,
    EVENT_RMDIR=5, EVENT_FSYNC=6, EVENT_RENAME=7, EVENT_CREATE=8, EVENT_UNLINK=9,
    EVENT_MKDIR=10, EVENT_TRUNCATE=11, EVENT_LINK=12, EVENT_CHMOD=13, EVENT_CHOWN=14,
    EVENT_BARRIER=15, EVENT_MKNOD=16, EVENT_SYMLINK=17, EVENT_FALLOCATE=18,
    EVENT_UTIMES=19
};

struct event {
    __u8 type; 
    __u8 version; 
    __u8 _pad0[2]; 
    __u32 dev_id; __u64 seq_num;
    __u64 timestamp_ns; __u64 parent_inode; __u64 inode; __u64 new_parent_inode;
    __u32 generation; __u32 mode; __u64 offset; __u64 length; __u32 uid;
    __u32 gid; 
    __u32 nlink; 
    __u32 flags; 
    __u64 file_size;
    __u32 projid; 
    __u32 _pad3; 
    char name[MAX_FILENAME]; 
    char new_name[MAX_FILENAME];
};

struct stats { __u64 events_submitted; __u64 events_dropped; __u64 write_events; __u64 metadata_events; };

// BPF Maps
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 64); __type(key, __u32); __type(value, __u64); } device_seq SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 64); __type(key, __u32); __type(value, __u8); } watched_devs SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 16); __type(key, __u32); __type(value, __u8); } ignored_pids SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY); __uint(max_entries, 1); __type(key, __u32); __type(value, struct stats); } statistics SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_RINGBUF); __uint(max_entries, 33554432); } events SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 1024); __type(key, __u64); __type(value, __u64); } temp_dentries SEC(".maps");

static __always_inline __u64 next_seq(__u32 dev_id) {
    __u64 *seq = bpf_map_lookup_elem(&device_seq, &dev_id);
    if (!seq) {
        __u64 init = 0; bpf_map_update_elem(&device_seq, &dev_id, &init, BPF_NOEXIST);
        seq = bpf_map_lookup_elem(&device_seq, &dev_id);
        if (!seq) return 0;
    }
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

// Helper to normalize device ID from kernel's dev_t format
// The kernel stores device ID as: (major << 20) | minor
// We need to extract and normalize this consistently
static __always_inline __u32 normalize_dev_id(__u32 raw_dev) {
    // The s_dev field from super_block is already in the correct format
    // Just return it as-is - userspace will handle byte order
    return raw_dev;
}

static __always_inline int submit_event(struct inode *inode, struct dentry *dentry, enum event_type type, __u64 offset, __u64 length, __u32 flags) {
    if (!inode) return 0;
    if (is_ignored_pid()) return 0;
    
    struct super_block *sb = BPF_CORE_READ(inode, i_sb);
    __u32 raw_dev_id = BPF_CORE_READ(sb, s_dev);
    __u32 dev_id = normalize_dev_id(raw_dev_id);
    
    bpf_printk("FOXING-DEBUG: Write detected on dev_id: %u (raw: %u)\n", dev_id, raw_dev_id);
    
    if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;

    struct event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) {
        __u32 z=0; struct stats *s = bpf_map_lookup_elem(&statistics, &z);
        if (s) __sync_fetch_and_add(&s->events_dropped, 1);
        return 0;
    }
    __builtin_memset(e, 0, sizeof(*e));
    e->type = type; e->version = EVENT_VERSION; e->dev_id = dev_id;
    e->seq_num = next_seq(dev_id); e->timestamp_ns = bpf_ktime_get_ns();
    e->inode = BPF_CORE_READ(inode, i_ino); 
    e->generation = BPF_CORE_READ(inode, i_generation);
    e->mode = BPF_CORE_READ(inode, i_mode); e->nlink = BPF_CORE_READ(inode, i_nlink);
    e->file_size = BPF_CORE_READ(inode, i_size); e->uid = BPF_CORE_READ(inode, i_uid.val);
    e->gid = BPF_CORE_READ(inode, i_gid.val); e->offset = offset; e->length = length; e->flags = flags;
    
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
        if (type==EVENT_WRITE||type==EVENT_WRITE_RANGE) __sync_fetch_and_add(&s->write_events, 1);
        else __sync_fetch_and_add(&s->metadata_events, 1);
    }
    return 0;
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

SEC("kprobe/xfs_file_write_iter") int BPF_KPROBE(trace_xfs_write, struct kiocb *iocb, struct iov_iter *from) {
    return submit_event(BPF_CORE_READ(iocb, ki_filp, f_inode), BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, BPF_CORE_READ(iocb, ki_pos), BPF_CORE_READ(from, count), 0);
}
SEC("kprobe/btrfs_file_write_iter") int BPF_KPROBE(trace_btrfs_write, struct kiocb *iocb, struct iov_iter *from) {
    return submit_event(BPF_CORE_READ(iocb, ki_filp, f_inode), BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, BPF_CORE_READ(iocb, ki_pos), BPF_CORE_READ(from, count), 0);
}
SEC("kprobe/f2fs_file_write_iter") int BPF_KPROBE(trace_f2fs_write, struct kiocb *iocb, struct iov_iter *from) {
    return submit_event(BPF_CORE_READ(iocb, ki_filp, f_inode), BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, BPF_CORE_READ(iocb, ki_pos), BPF_CORE_READ(from, count), 0);
}
SEC("kprobe/generic_file_write_iter") int BPF_KPROBE(trace_gen_write, struct kiocb *iocb, struct iov_iter *from) {
    return submit_event(BPF_CORE_READ(iocb, ki_filp, f_inode), BPF_CORE_READ(iocb, ki_filp, f_path.dentry), EVENT_WRITE_RANGE, BPF_CORE_READ(iocb, ki_pos), BPF_CORE_READ(from, count), 0);
}
SEC("kprobe/vfs_fsync") int BPF_KPROBE(trace_vfs_fsync, struct file *file, loff_t start, loff_t end, int datasync) {
    return submit_event(BPF_CORE_READ(file, f_inode), BPF_CORE_READ(file, f_path.dentry), EVENT_FSYNC, 0, 0, 0);
}
SEC("kprobe/vfs_create") int BPF_KPROBE(trace_create_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_create") int BPF_KRETPROBE(trace_create_exit, int ret) { return process_stashed_dentry(ret, EVENT_CREATE); }
SEC("kprobe/vfs_mkdir") int BPF_KPROBE(trace_mkdir_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_mkdir") int BPF_KRETPROBE(trace_mkdir_exit, int ret) { return process_stashed_dentry(ret, EVENT_MKDIR); }
SEC("kprobe/vfs_mknod") int BPF_KPROBE(trace_mknod_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_mknod") int BPF_KRETPROBE(trace_mknod_exit, int ret) { return process_stashed_dentry(ret, EVENT_MKNOD); }
SEC("kprobe/vfs_link") int BPF_KPROBE(trace_link_entry, struct dentry *old, void *id, void *dir, struct dentry *new_dentry) { return stash_dentry(new_dentry); }
SEC("kretprobe/vfs_link") int BPF_KRETPROBE(trace_link_exit, int ret) { return process_stashed_dentry(ret, EVENT_LINK); }
SEC("kprobe/vfs_symlink") int BPF_KPROBE(trace_symlink_entry, void *id, void *dir, struct dentry *dentry) { return stash_dentry(dentry); }
SEC("kretprobe/vfs_symlink") int BPF_KRETPROBE(trace_symlink_exit, int ret) { return process_stashed_dentry(ret, EVENT_SYMLINK); }
SEC("kprobe/vfs_unlink") int BPF_KPROBE(trace_unlink, void *idmap, struct inode *dir, struct dentry *dentry) {
    return submit_event(BPF_CORE_READ(dentry, d_inode), dentry, EVENT_UNLINK, 0, 0, 0);
}
SEC("kprobe/vfs_rmdir") int BPF_KPROBE(trace_rmdir, void *idmap, struct inode *dir, struct dentry *dentry) {
    return submit_event(BPF_CORE_READ(dentry, d_inode), dentry, EVENT_RMDIR, 0, 0, 0);
}
SEC("kprobe/vfs_rename") int BPF_KPROBE(trace_rename, void *idmap, struct renamedata *rd) {
    struct dentry *old = BPF_CORE_READ(rd, old_dentry);
    struct dentry *new = BPF_CORE_READ(rd, new_dentry);
    struct inode *inode = BPF_CORE_READ(old, d_inode);
    if(!inode) return 0;
    
    struct super_block *sb = BPF_CORE_READ(inode, i_sb);
    __u32 raw_dev_id = BPF_CORE_READ(sb, s_dev);
    __u32 dev_id = normalize_dev_id(raw_dev_id);
    
    bpf_printk("FOXING-DEBUG: Write detected on dev_id: %u (raw: %u)\n", dev_id, raw_dev_id);
    if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;
    
    struct event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) return 0;
    __builtin_memset(e, 0, sizeof(*e));
    e->type = EVENT_RENAME; e->version = EVENT_VERSION; e->dev_id = dev_id;
    e->seq_num = next_seq(dev_id); e->timestamp_ns = bpf_ktime_get_ns();
    e->inode = BPF_CORE_READ(inode, i_ino); e->generation = BPF_CORE_READ(inode, i_generation);
    
    struct dentry *op = BPF_CORE_READ(old, d_parent);
    if (op) e->parent_inode = BPF_CORE_READ(op, d_inode, i_ino);
    const unsigned char *old_name_ptr = BPF_CORE_READ(old, d_name.name);
    bpf_core_read_str(&e->name, sizeof(e->name), (const char *)old_name_ptr);
    
    struct dentry *np = BPF_CORE_READ(new, d_parent);
    if (np) e->new_parent_inode = BPF_CORE_READ(np, d_inode, i_ino);
    const unsigned char *new_name_ptr = BPF_CORE_READ(new, d_name.name);
    bpf_core_read_str(&e->new_name, sizeof(e->new_name), (const char *)new_name_ptr);
    
    bpf_ringbuf_submit(e, 0);
    return 0;
}
SEC("kprobe/notify_change") int BPF_KPROBE(trace_notify_change, struct user_namespace *mnt_userns, struct dentry *dentry, struct iattr *attr) {
    struct inode *inode = BPF_CORE_READ(dentry, d_inode);
    unsigned int ia_valid = BPF_CORE_READ(attr, ia_valid);
    
    if (ia_valid & ATTR_SIZE) {
        return submit_event(inode, dentry, EVENT_TRUNCATE, 0, BPF_CORE_READ(attr, ia_size), 0);
    }
    if (ia_valid & ATTR_MODE) {
        return submit_event(inode, dentry, EVENT_CHMOD, 0, 0, 0);
    }
    if ((ia_valid & ATTR_UID) || (ia_valid & ATTR_GID)) {
        return submit_event(inode, dentry, EVENT_CHOWN, 0, 0, 0);
    }
    if ((ia_valid & ATTR_ATIME) || (ia_valid & ATTR_MTIME)) {
        return submit_event(inode, dentry, EVENT_UTIMES, 0, 0, 0);
    }
    return 0;
}
SEC("kprobe/vfs_setxattr") int BPF_KPROBE(trace_setxattr, struct dentry *dentry) {
    return submit_event(BPF_CORE_READ(dentry, d_inode), dentry, EVENT_SETXATTR, 0, 0, 0);
}
SEC("kprobe/vfs_removexattr") int BPF_KPROBE(trace_removexattr, struct dentry *dentry) {
    return submit_event(BPF_CORE_READ(dentry, d_inode), dentry, EVENT_REMOVEXATTR, 0, 0, 0);
}
SEC("kprobe/vfs_fallocate") int BPF_KPROBE(trace_fallocate, struct file *file, int mode, loff_t offset, loff_t len) {
    return submit_event(BPF_CORE_READ(file, f_inode), BPF_CORE_READ(file, f_path.dentry), EVENT_FALLOCATE, offset, len, mode);
}
SEC("kprobe/xfs_trans_commit") int BPF_KPROBE(trace_xfs_commit, struct xfs_trans *tp) {
    struct xfs_mount *mp = BPF_CORE_READ(tp, t_mountp);
    struct super_block *sb = BPF_CORE_READ(mp, m_super);
    __u32 raw_dev_id = BPF_CORE_READ(sb, s_dev);
    __u32 dev_id = normalize_dev_id(raw_dev_id);
    
    bpf_printk("FOXING-DEBUG: Write detected on dev_id: %u (raw: %u)\n", dev_id, raw_dev_id);
    if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;
    struct event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) return 0;
    __builtin_memset(e, 0, sizeof(*e));
    e->type = EVENT_BARRIER; e->version = EVENT_VERSION; e->dev_id = dev_id;
    e->seq_num = next_seq(dev_id); e->timestamp_ns = bpf_ktime_get_ns();
    bpf_ringbuf_submit(e, 0);
    return 0;
}
char LICENSE[] SEC("license") = "GPL";
