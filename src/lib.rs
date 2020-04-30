#![crate_type = "rlib"]
#![feature(integer_atomics)]
#![feature(generators, generator_trait)]
#![feature(box_into_raw_non_null)]
#![feature(trait_alias)]
#![feature(nll)]

#[allow(dead_code,unused_variables,non_snake_case,unused_parens,unused_assignments,unused_unsafe,unused_imports)]

extern crate crc;
extern crate core;
extern crate fnv;
extern crate lmdb;
extern crate tempdir;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

extern crate rand;

extern crate apm;
extern crate atom;
extern crate r#async;
extern crate async_file;
extern crate handler;
extern crate worker;
extern crate file;
extern crate gray;
extern crate guid;
extern crate util as lib_util;
extern crate sinfo;
extern crate hash_value;
extern crate timer;
extern crate ordmap;
extern crate pi_db;
extern crate crossbeam_channel;
extern crate nodec;
extern crate crc32fast;
extern crate fastcmp;
extern crate bon;
extern crate json;
extern crate hash;

//pub mod kg;
//pub mod kg_log;
//pub mod kg_record;
//pub mod kg_root;
//pub mod kg_subtab;
pub mod lmdb_file;
//pub mod log;
pub mod pool;
pub mod file_mem_db;
pub mod log_store;