use std::alloc::{alloc, dealloc, Layout, handle_alloc_error};
use std::{ops::{Deref, DerefMut}, slice};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU16, Ordering};
use tracing::{debug, error};
pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    capacity: usize,
    len: usize
}
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}
impl AlignedBuffer {
    pub fn new(capacity: usize) -> Self {
        let layout = Layout::from_size_align(capacity, 4096).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Self { ptr, layout, capacity, len: 0 }
    }
    pub fn capacity(&self) -> usize { self.capacity }
    pub fn set_full_len(&mut self) { self.len = self.capacity; }
    pub fn clear(&mut self) { self.len = 0; }
    pub fn ptr(&self) -> *mut u8 { self.ptr }
    pub unsafe fn capacity_slice_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.ptr, self.capacity)
        }
    }
}
impl Drop for AlignedBuffer {
    fn drop(&mut self) { unsafe { dealloc(self.ptr, self.layout); } }
}
impl Deref for AlignedBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target { unsafe { slice::from_raw_parts(self.ptr, self.len) } }
}
impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target { unsafe { slice::from_raw_parts_mut(self.ptr, self.len) } }
}
pub struct BufferPool {
    // The actual memory buffers, held in a Box to ensure they are pinned/stable.
    // The Box<Vec<..>> ensures that the Vec itself doesn't move, which is critical
    // once the addresses are registered with io_uring.
    buffers: Box<Vec<AlignedBuffer>>,
    // A list of indices (u16) representing available buffers.
    free_list: VecDeque<u16>,
    // The current number of in-flight buffers (used in metrics, mostly).
    in_flight_count: AtomicU16,
    // The size of each chunk.
    chunk_size: usize,
}
impl BufferPool {
    pub fn new(num_buffers: usize, chunk_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(num_buffers);
        for _ in 0..num_buffers {
            buffers.push(AlignedBuffer::new(chunk_size));
        }
        let mut free_list = VecDeque::with_capacity(num_buffers);
        for i in 0..num_buffers {
            // io_uring 0.7.x uses u16 for buffer IDs
            free_list.push_back(i as u16);
        }
        debug!("BufferPool initialized with {} buffers of {} bytes each.", num_buffers, chunk_size);
        Self {
            buffers: Box::new(buffers),
            free_list,
            in_flight_count: AtomicU16::new(0),
            chunk_size,
        }
    }
    pub fn capacity(&self) -> usize { self.buffers.len() }
    pub fn chunk_size(&self) -> usize { self.chunk_size }
    // Get the raw pointer for the buffer at the given index.
    pub fn get_ptr(&self, index: u16) -> *mut u8 {
        self.buffers[index as usize].ptr()
    }
    // Get the size of the memory block
    pub fn get_len(&self, index: u16) -> usize {
        self.buffers[index as usize].len
    }
    pub fn set_len(&mut self, index: u16, len: usize) {
        self.buffers[index as usize].len = len;
    }
    // Get the address of the underlying buffer data for io_uring registration (iovec array)
    pub fn as_io_vecs(&mut self) -> Vec<libc::iovec> {
        self.buffers.iter_mut().map(|buf| {
            libc::iovec { iov_base: buf.ptr() as _, iov_len: buf.capacity() }
        }).collect()
    }
    // Attempts to acquire a free buffer index (u16).
    pub fn acquire(&mut self) -> Option<u16> {
        let index = self.free_list.pop_front();
        if index.is_some() {
            self.in_flight_count.fetch_add(1, Ordering::Relaxed);
        }
        index
    }
    // Releases a buffer index back to the pool.
    pub fn release(&mut self, index: u16) {
        if index as usize >= self.buffers.len() {
            error!("Attempted to release invalid buffer index: {}", index);
            return;
        }
        self.buffers[index as usize].clear();
        self.free_list.push_back(index);
        self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn is_empty(&self) -> bool {
        self.free_list.is_empty()
    }
    pub fn free_count(&self) -> usize {
        self.free_list.len()
    }
}
