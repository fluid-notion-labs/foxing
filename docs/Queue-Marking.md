# Queue-Marking: CoDel/FQ-CoDel/CAKE Applied to Event-Driven Replication

## 1. Problem Statement

foxingd's event pipeline uses single FIFO `tokio::sync::mpsc` channels per worker. During write storms (fio generating 4.1M WriteRange events in 10 seconds), structural events (Create, Rename, Mkdir) are buried behind millions of write events. Workers drain writes via NFS at ~100 ops/sec — a 16K channel backlog takes 160+ seconds to clear. The adversarial test's 30-second stall window expires before structural events are processed.

This is the **bufferbloat problem** applied to event dispatch: high throughput (write events) fills queues, starving latency-sensitive traffic (structural events).

## 2. Queue Theory Foundation

### Little's Law

```
L = lambda * W

L = average queue depth (events)
lambda = arrival rate (events/sec)
W = average sojourn time (seconds per event)
```

During fio write storm:
- lambda = 410,000 events/sec (4.1M in 10s)
- W = 10ms per NFS write (optimistic)
- L = 410,000 * 0.01 = 4,100 events steady-state

With 512-capacity channel: channel fills in ~1.2ms. All subsequent events are dropped or block.

### Laminar vs Turbulent Flow

- **Laminar**: Steady-state replication, events flow predictably, sojourn time stable
- **Turbulent**: Write storm, priority inversion, structural events blocked by bulk data
- **Reynolds number analog**: `Re = (arrival_rate * queue_depth) / (processing_rate * channel_capacity)`
  - Re < 1: laminar (queue drains faster than fills)
  - Re > 1: turbulent (queue grows, bufferbloat)

During Phase 2 fio: Re = (410000 * 512) / (100 * 512) = 4100 >> 1. Fully turbulent.

## 3. CoDel (Controlled Delay) for Event Dispatch

### Core Insight
CoDel distinguishes **good queues** (transient burst, drains quickly) from **bad queues** (persistent congestion, events wait too long). The key metric is **sojourn time** — how long an event waits in the queue — not queue depth.

### Algorithm Applied to Events

```
CONSTANTS:
  TARGET = 50ms     # structural events should process within 50ms
  INTERVAL = 1000ms # sustained sojourn above target triggers action

STATE:
  dropping: bool = false
  first_above_time: Option<Instant> = None
  drop_count: u64 = 0

ON EVENT DEQUEUE(event):
  sojourn = now - event.created_at

  if sojourn < TARGET:
    first_above_time = None  # queue is healthy
    dropping = false
  else:
    if first_above_time is None:
      first_above_time = Some(now)  # start timing

    if now - first_above_time > INTERVAL:
      if not dropping:
        dropping = true
        drop_count = 0

      drop_count += 1
      # ACTION: mark event as congested (ECN) or drop if bulk
      if event.is_droppable():
        drop(event)
      else:
        mark_congested(event)
```

### foxing-specific calibration

| Event Class | Sojourn Target | Drop Policy |
|---|---|---|
| Control (Barrier, Gap) | 10ms | Never drop |
| Structural (Create, Rename, Mkdir) | 50ms | Never drop, ECN mark only |
| Metadata (Chmod, Chown, Utimes) | 200ms | Mark at 200ms, drop at 5s |
| Bulk (Write, WriteRange, Clone) | 500ms | Mark at 500ms, drop at 2s |

## 4. CAKE Tin Classification

### Four-Tin Model

CAKE uses "tins" — priority classes with independent queues, each running its own CoDel instance. Applied to foxing event types:

```
Tin 0 — CONTROL (highest priority, always drains first)
  Barrier, SequenceGap, Fsync
  Capacity: unbounded (mpsc::unbounded_channel)
  Drop policy: NEVER
  Purpose: Ordering barriers and sequence integrity

Tin 1 — STRUCTURAL (high priority, drained before bulk)
  Create, Mkdir, Rename, Unlink, Rmdir, Link, Symlink, Mknod, RenameIncomplete
  Capacity: 4096
  Drop policy: NEVER (use blocking_send if full)
  Purpose: Filesystem tree structure must be replicated exactly

Tin 2 — METADATA (normal priority)
  Chmod, Chown, Utimes, SetXattr, RemoveXattr, SetFlags, Lock, Flock
  Capacity: 1024
  Drop policy: Drop after 5s sojourn (metadata can be re-synced)
  Purpose: File attributes, less urgent than structure

Tin 3 — BULK (lowest priority, droppable under pressure)
  Write, WriteRange, Clone, Truncate, Fallocate
  Capacity: 64
  Drop policy: Drop freely under pressure (hydration scan catches missed writes)
  Purpose: Data content, coalesced by worker, recoverable via rescan
```

### Deficit Round Robin (DRR) Between Tins

When multiple tins have events, use weighted round-robin:
```
Weights: Control=inf, Structural=8, Metadata=2, Bulk=1
Quantum: 1 event per round per tin (adjusted by weight)

Per round:
  1. Drain ALL control events (weight=inf)
  2. Process up to 8 structural events
  3. Process up to 2 metadata events
  4. Process 1 bulk event
```

This ensures structural events get 8x processing share vs bulk writes, while bulk writes still make progress (preventing starvation).

## 5. FQ-CoDel Per-Flow Fairness

### Flow Identification

Flow = `hash(event.parent_inode) % N_FLOWS`

Events for files in the same directory share a flow. This prevents one hot directory (e.g., `/var/log/` with 10K writes/sec) from starving other directories.

### New Flow Priority

When a new `parent_inode` is seen for the first time, its events get temporary priority boost (processed before established flows). This ensures new directory creation + file creation sequences complete quickly, even if existing directories have large backlogs.

```
struct FlowState {
    created_at: Instant,
    events_processed: u64,
    is_new: bool,  // true until events_processed > 10
}
```

### Per-Flow CoDel

Each flow maintains its own CoDel state. A slow NFS write for one directory doesn't trigger dropping for other directories' events.

## 6. ECN Marking for Backpressure

### Mark Instead of Drop

When sojourn time exceeds target but the event is non-droppable, **mark** it with a congestion flag. The mark propagates through the pipeline:

```rust
struct Event {
    // ... existing fields ...
    pub congestion_experienced: bool,  // ECN CE equivalent
}
```

### Feedback Loop to BBR Tuner

When workers process marked events, increment a congestion counter:
```rust
if event.congestion_experienced {
    metrics::EVENT_CONGESTION_MARKS.inc();
}
```

The BBR tuner reads this counter periodically. When marks/sec exceeds a threshold:
1. Increase `coalesce_bytes` (aggregate more writes before processing)
2. Reduce `batch_size` (process fewer events per tuner cycle)
3. Transition to `Drain` state (reduce pacing gain)

This creates a closed feedback loop: congestion marks → tuner adjustment → reduced event generation → marks decrease.

## 7. Implementation Architecture

### TinnedEventQueue (replaces EventQueue)

```rust
pub enum EventTin {
    Control = 0,
    Structural = 1,
    Metadata = 2,
    Bulk = 3,
}

impl EventType {
    pub fn tin(&self) -> EventTin {
        match self {
            Self::Barrier | Self::SequenceGap | Self::Fsync => EventTin::Control,
            Self::Create | Self::Mkdir | Self::Rename | Self::Unlink
            | Self::Rmdir | Self::Link | Self::Symlink | Self::Mknod
            | Self::RenameIncomplete => EventTin::Structural,
            Self::Chmod | Self::Chown | Self::Utimes | Self::SetXattr
            | Self::RemoveXattr | Self::SetFlags | Self::Lock | Self::Flock => EventTin::Metadata,
            _ => EventTin::Bulk,
        }
    }
}

pub struct TinnedEventQueue {
    control: Vec<mpsc::UnboundedSender<Arc<Event>>>,
    structural: Vec<mpsc::Sender<Arc<Event>>>,  // capacity: 4096
    metadata: Vec<mpsc::Sender<Arc<Event>>>,    // capacity: 1024
    bulk: Vec<mpsc::Sender<Arc<Event>>>,        // capacity: 64
}
```

### Worker Select Loop

```rust
tokio::select! {
    biased;  // Control > Structural > Metadata > Bulk

    _ = shutdown_rx.recv() => { break; }
    Some(evt) = control_rx.recv() => { process(evt).await; }
    Some(evt) = structural_rx.recv() => { process(evt).await; }
    Some(evt) = metadata_rx.recv() => { process(evt).await; }
    Some(evt) = bulk_rx.recv() => {
        // Bulk events go through coalescer
        coalescer.push(evt);
        if let Some(coalesced) = coalescer.pop_batch(...) {
            process(coalesced).await;
        }
    }
    _ = flush_interval.tick(), if !coalescer.is_empty() => {
        while let Some(evt) = coalescer.pop_batch(0, Duration::ZERO, false) {
            process(evt).await;
            if flush_count >= 256 { break; }
        }
    }
}
```

### Metrics

```rust
lazy_static! {
    pub static ref EVENT_SOJOURN_SECONDS: HistogramVec = register_histogram_vec!(
        "foxing_event_sojourn_seconds",
        "Time from event creation to worker dispatch",
        &["tin"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]
    ).unwrap();

    pub static ref EVENT_TIN_DEPTH: GaugeVec = register_gauge_vec!(
        "foxing_event_tin_depth",
        "Current queue depth per tin",
        &["tin", "worker"]
    ).unwrap();

    pub static ref EVENT_CONGESTION_MARKS: CounterVec = register_counter_vec!(
        "foxing_event_congestion_marks_total",
        "Events marked with congestion experienced",
        &["tin"]
    ).unwrap();

    pub static ref EVENT_TIN_DROPS: CounterVec = register_counter_vec!(
        "foxing_event_tin_drops_total",
        "Events dropped per tin",
        &["tin"]
    ).unwrap();
}
```

## 8. Expected Impact

| Metric | Before | After |
|---|---|---|
| Phase 3 structural event latency | 30+ seconds (blocked by writes) | <100ms (Tin 1 priority) |
| Write events dropped during storm | 4.1M overflow | ~4M dropped intentionally (Tin 3) |
| Structural events dropped | Silently lost | 0 (never drop policy) |
| Phase 3 test result | FAIL (missing=100) | PASS (finals=100/100) |
| Queue observability | 1 metric (events_dropped) | 8 metrics (per-tin sojourn, depth, marks, drops) |

## 9. References

- RFC 8289: CoDel Algorithm
- CAKE: Common Applications Kept Enhanced (Linux kernel qdisc)
- FQ-CoDel: Fair Queuing Controlled Delay (Linux kernel qdisc)
- BBR: Bottleneck Bandwidth and Round-trip propagation time (Google)
- Little's Law: J.D.C. Little (1961), "A Proof for the Queuing Formula: L = lambda * W"
