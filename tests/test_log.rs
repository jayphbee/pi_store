// extern crate pi_store;
// extern crate pi_base;
// extern crate pi_lib;

// use std::thread;
// use std::sync::Arc;

// use pi_lib::atom::Atom;
// use pi_base::pi_base_impl::STORE_TASK_POOL;
// use pi_base::worker_pool::WorkerPool;
// use pi_lib::guid::Guid;

// use pi_store::log::{SResult, Config, Log};

// #[test]
// fn test_load_write() {
//     let worker_pool = Box::new(WorkerPool::new(10, 1024 * 1024, 10000));
//     worker_pool.run(STORE_TASK_POOL.clone());

//     let cb = Arc::new(|result: SResult<Log>| {
//         println!("!!!!!!new log ok");
//         assert!(result.is_ok());
//         // let log = result.ok().unwrap();
//         // let cb = Arc::new(|result: SResult<()>| {
//         //     assert!(result.is_ok());
//         // });
//         // assert!(log.write(Guid(0xffffffffu128), Arc::new("Hello World!".to_string().into_bytes()), 0, cb).is_ok());
//         // let cb = Arc::new(|result: SResult<()>| {
//         //     assert!(result.is_ok());
//         // });
//         // assert!(log.write(Guid(0xffffffffu128), Arc::new("AAAAAAAAAAAAAAAAAAAAAAAAA!".to_string().into_bytes()), 0, cb).is_ok());
//         // let cb = Arc::new(|result: SResult<()>| {
//         //     assert!(result.is_ok());
//         // });
//         // assert!(log.write(Guid(0xffffffffu128), Arc::new("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC!".to_string().into_bytes()), 0, cb).is_ok());
//     });
//     let result = Log::new(Atom::from(r".\log"), Config::new(), 0, cb);
//     if result.is_some() {
//         assert!(result.unwrap().is_ok());
//     }
//     thread::sleep_ms(3000);
// }