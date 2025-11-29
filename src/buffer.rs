use std::alloc::{alloc, dealloc, Layout};
use std::{ops::{Deref, DerefMut}, slice};

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
        Self { ptr, layout, capacity, len: 0 }
    }
    pub fn capacity(&self) -> usize { self.capacity }
    pub fn set_full_len(&mut self) { self.len = self.capacity; }
    pub fn clear(&mut self) { self.len = 0; }
    pub unsafe fn capacity_slice_mut(&mut self) -> &mut [u8] { 
        slice::from_raw_parts_mut(self.ptr, self.capacity) 
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
    // Removed invalid `type Target` here
    fn deref_mut(&mut self) -> &mut Self::Target { unsafe { slice::from_raw_parts_mut(self.ptr, self.len) } } 
}
