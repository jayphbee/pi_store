use std::ffi::CString;
use std::mem;
use std::ptr;
use std::slice;

use libc::{self, c_char, c_void, size_t};

use ffi;



pub struct SliceTransform {
    pub inner: *mut ffi::rocksdb_transactiondb_t,
}

