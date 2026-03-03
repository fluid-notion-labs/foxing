use std::alloc::{alloc, dealloc, Layout};
use std::{ops::{Deref, DerefMut}, slice};
use tracing::{debug, error, trace, info};
use crate::metrics::{GLOBAL_BUFFER_COUNT, GLOBAL_BUFFER_LIMIT, GLOBAL_MEMORY_USAGE_BYTES};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::cell::UnsafeCell;
use crossbeam::queue::ArrayQueue;
use crate::error::{FxcpError, Result};
use crate::constants;
use std::ptr;

pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    capacity: usize,
    len: AtomicUsize
}
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// NUMA-aware allocation stub for enterprise server configurations.
    /// Falls back to standard allocation if node < 0 or mbind unavailable.
    #[allow(unused_variables)]
    pub fn try_new_numa(capacity: usize, alignment: usize, numa_node: i32) -> Result<Self> {
        // TODO: On dual-socket servers, use mmap + mbind(MPOL_BIND) to pin
        // buffers to the same NUMA node as the NVMe controller to avoid
        // QPI cross-talk. Discover node via /sys/class/block/*/device/numa_node.
        // For now, fall back to standard allocation.
        Self::try_new(capacity, alignment)
    }

    pub fn try_new(capacity: usize, alignment: usize) -> Result<Self> {
        // Fix: Limit is already in bytes, don't multiply by 1024*1024 again
        let limit_bytes = GLOBAL_BUFFER_LIMIT.get() as u64;
        let align = alignment.max(constants::MINIMUM_ALIGNMENT_BYTES);
        let actual_capacity = capacity.max(align);
        
        let _prev = GLOBAL_BUFFER_COUNT.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |current| {
                if current + actual_capacity as u64 > limit_bytes {
                    None
                } else {
                    Some(current + actual_capacity as u64)
                }
            }
        ).map_err(|_| FxcpError::MemoryExhausted(format!(
            "Global memory limit exceeded. Refusing allocation of {} bytes.", capacity
        )))?;

        // Update Prometheus Gauge
        GLOBAL_MEMORY_USAGE_BYTES.add(actual_capacity as f64);

        let layout = Layout::from_size_align(actual_capacity, align)
            .map_err(|_e| {
                GLOBAL_BUFFER_COUNT.fetch_sub(actual_capacity as u64, Ordering::SeqCst);
                GLOBAL_MEMORY_USAGE_BYTES.sub(actual_capacity as f64);
                FxcpError::System(nix::Error::from(nix::errno::Errno::EINVAL))
            })?;
            
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            GLOBAL_BUFFER_COUNT.fetch_sub(actual_capacity as u64, Ordering::SeqCst);
            GLOBAL_MEMORY_USAGE_BYTES.sub(actual_capacity as f64);
            return Err(FxcpError::MemoryExhausted("Physical memory allocation failed".to_string()));
        }
        unsafe { ptr::write_bytes(ptr, 0, actual_capacity); }
        trace!("AlignedBuffer: Allocated {} bytes.", actual_capacity);
        Ok(Self {
            ptr,
            layout,
            capacity: actual_capacity,
            len: AtomicUsize::new(0)
        })
    }
    #[inline(always)]
    pub fn clear(&self) {
        self.len.store(0, Ordering::Release);
    }
    #[inline(always)]
    pub fn capacity(&self) -> usize { self.capacity }
    #[inline(always)]
    pub fn set_full_len(&self) {
        self.len.store(self.capacity, Ordering::Release);
    }
    #[inline(always)]
    pub fn set_len(&self, len: usize) {
        if len > self.capacity {
            panic!("AlignedBuffer::set_len: {} exceeds capacity {}", len, self.capacity);
        }
        self.len.store(len, Ordering::Release);
    }
    #[inline(always)]
    pub fn get_len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
    #[inline(always)]
    pub fn ptr(&self) -> *mut u8 { self.ptr }
    #[inline(always)]
    pub fn alignment(&self) -> usize { self.layout.align() }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let _prev = GLOBAL_BUFFER_COUNT.fetch_sub(self.capacity as u64, Ordering::SeqCst);
        GLOBAL_MEMORY_USAGE_BYTES.sub(self.capacity as f64);
        unsafe { dealloc(self.ptr, self.layout); }
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.ptr, self.get_len()) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.get_len()) }
    }
}

enum QueueStrategy {
    Shared(ArrayQueue<u16>),
    Local(UnsafeCell<Vec<u16>>),
}
unsafe impl Send for QueueStrategy {}
unsafe impl Sync for QueueStrategy {}

struct BufferPoolInner {
    buffers: Vec<UnsafeCell<AlignedBuffer>>,
    free_indices: QueueStrategy,
    capacity: usize,
    chunk_size: usize,
    alignment: usize,
}
unsafe impl Sync for BufferPoolInner {}
unsafe impl Send for BufferPoolInner {}

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

impl BufferPool {
    pub fn new(requested_buffers: usize, requested_chunk_size: usize, alignment: usize) -> Result<Self> {
        Self::create_pool(requested_buffers, requested_chunk_size, alignment, false)
    }
    pub fn new_local(requested_buffers: usize, requested_chunk_size: usize, alignment: usize) -> Result<Self> {
        Self::create_pool(requested_buffers, requested_chunk_size, alignment, true)
    }
    fn create_pool(requested_buffers: usize, requested_chunk_size: usize, alignment: usize, local_mode: bool) -> Result<Self> {
        if requested_buffers > 65535 {
            return Err(FxcpError::Config("BufferPool: max 65535 buffers allowed".to_string()));
        }
        let align = alignment.max(constants::MINIMUM_ALIGNMENT_BYTES);
        let min_chunk_size = 4096;
        let strategies = [
            (requested_buffers, requested_chunk_size),
            (requested_buffers / 2, requested_chunk_size),
            (requested_buffers / 4, requested_chunk_size),
            (requested_buffers, requested_chunk_size / 2),
            (requested_buffers / 2, requested_chunk_size / 2),
            (8, min_chunk_size),
        ];
        
        for (count, size) in strategies {
            if count < 2 || size < min_chunk_size { continue; }
            let mut buffers = Vec::with_capacity(count);
            let mut allocated_successfully = true;
            for i in 0..count {
                match AlignedBuffer::try_new(size, align) {
                    Ok(buf) => buffers.push(UnsafeCell::new(buf)),
                    Err(e) => {
                        debug!("BufferPool: Allocation failed at buffer {} of {}: {:?}", i, count, e);
                        allocated_successfully = false;
                        break;
                    }
                }
            }
            if !allocated_successfully { continue; }
            
            let actual_capacity = buffers.len();
            let queue = if local_mode {
                let mut v = Vec::with_capacity(actual_capacity);
                for i in 0..actual_capacity {
                    v.push(i as u16);
                }
                QueueStrategy::Local(UnsafeCell::new(v))
            } else {
                let q = ArrayQueue::new(actual_capacity);
                for i in 0..actual_capacity {
                    let _ = q.push(i as u16);
                }
                QueueStrategy::Shared(q)
            };
            
            let total_mb = (actual_capacity as u64 * size as u64) / 1024 / 1024;
            info!("BufferPool: Allocated {} x {}KB buffers ({}MB total, Mode: {})",
                  actual_capacity, size / 1024, total_mb, if local_mode { "Thread-Local" } else { "Shared" });
            crate::metrics::BUFFER_POOL_CAPACITY.set(actual_capacity as f64);
            crate::metrics::BUFFER_POOL_CHUNK_SIZE.set(size as f64);
            crate::metrics::BUFFER_POOL_TOTAL_BYTES.set((actual_capacity as u64 * size as u64) as f64);
            
            return Ok(Self {
                inner: Arc::new(BufferPoolInner {
                    buffers,
                    free_indices: queue,
                    capacity: actual_capacity,
                    chunk_size: size,
                    alignment: align,
                })
            });
        }
        Err(FxcpError::MemoryExhausted("BufferPool: All allocation strategies failed.".to_string()))
    }
    
    #[inline(always)]
    pub fn capacity(&self) -> usize { self.inner.capacity }
    #[inline(always)]
    pub fn chunk_size(&self) -> usize { self.inner.chunk_size }
    #[inline(always)]
    pub fn alignment(&self) -> usize { self.inner.alignment }
    #[inline(always)]
    pub fn get_ptr(&self, index: u16) -> Option<*mut u8> {
        self.inner.buffers.get(index as usize).map(|cell| {
            unsafe { (*cell.get()).ptr() }
        })
    }
    #[inline(always)]
    pub fn get_len(&self, index: u16) -> usize {
        if let Some(cell) = self.inner.buffers.get(index as usize) {
            unsafe { (*cell.get()).get_len() }
        } else {
            0
        }
    }
    #[inline(always)]
    pub fn set_len(&self, index: u16, len: usize) {
        if let Some(cell) = self.inner.buffers.get(index as usize) {
            unsafe {
                let buf = &*cell.get();
                buf.set_len(len);
            }
        }
    }
    pub fn as_io_vecs(&mut self) -> Vec<libc::iovec> {
        self.inner.buffers.iter().map(|cell| {
            unsafe {
                let buf = &*cell.get();
                libc::iovec { iov_base: buf.ptr() as _, iov_len: buf.capacity() }
            }
        }).collect()
    }
    #[inline]
    pub fn acquire(&self) -> Option<u16> {
        match &self.inner.free_indices {
            QueueStrategy::Shared(q) => q.pop(),
            QueueStrategy::Local(cell) => {
                let vec = unsafe { &mut *cell.get() };
                vec.pop()
            }
        }
    }
    #[inline]
    pub fn release(&self, index: u16) {
        if let Some(cell) = self.inner.buffers.get(index as usize) {
            unsafe { (*cell.get()).clear(); }
            match &self.inner.free_indices {
                QueueStrategy::Shared(q) => {
                    let _ = q.push(index);
                },
                QueueStrategy::Local(c) => {
                    let vec = unsafe { &mut *c.get() };
                    vec.push(index);
                }
            }
        } else {
            error!("BufferPool: Attempted to release invalid index {}", index);
        }
    }
    #[inline]
    pub fn free_count(&self) -> usize {
        match &self.inner.free_indices {
            QueueStrategy::Shared(q) => q.len(),
            QueueStrategy::Local(c) => unsafe { (*c.get()).len() },
        }
    }
}
