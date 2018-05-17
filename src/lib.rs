#![feature(fnbox)]
#![crate_type = "rlib"]
#![feature(custom_derive,asm,box_syntax,box_patterns)]
#[allow(dead_code,unused_variables,non_snake_case,unused_parens,unused_assignments,unused_unsafe,unused_imports)]

extern crate bytes;

#[macro_use]
extern crate lazy_static;

extern crate pi_vm;

pub mod alloc;
pub mod rc;
pub mod ordmap;
pub mod sbtree;
pub mod file;
