# foxing

`foxing` (formerly xfs-mirror) aspires to be a production-grade, eBPF-powered replication engine for Linux filesystems (XFS, Btrfs, F2FS, Ext4). It captures filesystem events in the kernel and replays them asynchronously on a target directory, providing near real-time mirroring with robust consistency guarantees.
