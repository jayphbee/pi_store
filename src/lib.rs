#![crate_type = "rlib"]
#![feature(fnbox)]
#![feature(integer_atomics)]
#![feature(duration_extras)]
#![feature(custom_derive,asm,box_syntax,box_patterns)]
#![feature(pointer_methods)]
#![feature(core_intrinsics)]
#![feature(generators, generator_trait)]
#![feature(exclusive_range_pattern)]
#![feature(box_into_raw_non_null)]
#![feature(trait_alias)]
#![feature(nll)]

#[allow(dead_code,unused_variables,non_snake_case,unused_parens,unused_assignments,unused_unsafe,unused_imports)]

extern crate core;
extern crate fnv;
extern crate bytes;
extern crate rocksdb;

#[macro_use]
extern crate lazy_static;

extern crate pi_lib;
extern crate pi_base;
extern crate pi_db;


pub mod log;
pub mod kg;
