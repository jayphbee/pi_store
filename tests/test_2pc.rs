extern crate rocksdb;
extern crate pi_store;
extern crate pi_db;
extern crate pi_lib;
extern crate pi_vm;

use std::sync::Arc;
use std::time::Duration;
use std::thread;

use rocksdb::{TXN_DB, TXN, Options, TransactionDBOptions, TransactionOptions, WriteOptions, ReadOptions};

use pi_store::db::{SysDb, FDBTab, FDB, STORE_TASK_POOL};
use pi_db::db::{TabKV, Txn, DBResult, TabBuilder};
use pi_lib::atom::{Atom};
use pi_lib::guid::{Guid, GuidGen};
use pi_lib::time::now_nanos;
use pi_vm::worker_pool::WorkerPool;
use std::str;

fn tabkv(tab: Atom, key: Vec<u8>, v: Option<Arc<Vec<u8>>>) -> Vec<TabKV> {
    let mut tabkv = TabKV::new(tab, key);
    tabkv.value = v;
    let mut v = Vec::new();
    v.push(tabkv);
    return v
}


#[test]
fn test_2pc() {
    let mut worker_pool = Box::new(WorkerPool::new(10, 1024 * 1024, 10000));
    worker_pool.run(STORE_TASK_POOL.clone());
    let tab = Atom::from("_rust_rocksdb_2pc");
    let sys_tab = Atom::from("_rust_rocksdb_systab");
    {
        println!("1111111111");
        let sdb = SysDb::new(&sys_tab);
        let db = sdb.open(&tab, Box::new(|v|{})).unwrap().unwrap();
        let guid_gen = GuidGen::new(1, now_nanos() as u32);
		let guid = guid_gen.gen(2);
        let txn = db.transaction(&guid, true, 10);
        //插入测试数据
        let mut kv_arr = Vec::new();
        let mut kv = TabKV::new(tab.clone(), b"k1".to_vec());
        kv.value = Some(Arc::new(b"v11".to_vec()));
        kv_arr.push(kv);
        let mut kv = TabKV::new(tab.clone(), b"k2".to_vec());
        kv.value = Some(Arc::new(b"v22".to_vec()));
        kv_arr.push(kv);
        let mut kv = TabKV::new(tab.clone(), b"k3".to_vec());
        kv.value = Some(Arc::new(b"v33".to_vec()));
        kv_arr.push(kv);
        let mut kv = TabKV::new(tab.clone(), b"k4".to_vec());
        kv.value = Some(Arc::new(b"v44".to_vec()));
        kv_arr.push(kv);

        
        txn.modify(Arc::new(kv_arr), Some(0), false, Arc::new(
            |v: Result<usize, String>|{
                assert!(v.unwrap() == 4);
            }
        ));
        thread::sleep(Duration::from_millis(1000));
        println!("---------------------");
        let txn2 = txn.clone();
        let tab2 = tab.clone();
        //删除k1
        let p = txn.modify(Arc::new(tabkv(tab.clone(), b"k1".to_vec(), None)), Some(0), false, Arc::new(
            move |v1: Result<usize, String>| {
                assert!(v1.unwrap() == 1);
                println!("del k1 ok!!!!!!!!");
                let r: Option<DBResult<Vec<TabKV>>> = txn2.query(Arc::new(tabkv(tab2.clone(), b"k1".to_vec(), None)), Some(0), false, Arc::new(
                    |v2| {
                        for values in v2.unwrap() {
                        assert!(values.value == None);
                        println!("query k1 ok!!!!!!!!");
                }
                    }
                ));     
            }
        ));
        thread::sleep(Duration::from_millis(1000));
        println!("---------------------");
        //查询k3
        let r: Option<DBResult<Vec<TabKV>>> = txn.query(Arc::new(tabkv(tab.clone(), b"k3".to_vec(), None)), Some(0), false, Arc::new(
            |v: DBResult<Vec<TabKV>>| {
                for values in v.unwrap() {
                    assert!(str::from_utf8(values.value.unwrap().as_slice()).unwrap() == "v33");
                    println!("query k3 ok!!!!!!!!!");
                }
            }
        ));
        thread::sleep(Duration::from_millis(1000));
        println!("---------------------");
        //迭代(异步)
        let p = txn.iter(&tab, None, false, true, "test".to_string(), Arc::new(
            |v| {
                let mut iter = v.unwrap();
                println!("iter!!!!!!!start!!!!!");
                assert_eq!(iter.state(), Ok(true));
                assert_eq!(iter.key(), Arc::new(b"k2".to_vec()));
                assert_eq!(iter.value(), Some(Arc::new(b"v22".to_vec())));
                //下一个
                iter.next();
                println!("iter!!!!!!!next!!!!!");
                assert_eq!(iter.state(), Ok(true));
                assert_eq!(iter.key(), Arc::new(b"k3".to_vec()));
                assert_eq!(iter.value(), Some(Arc::new(b"v33".to_vec())));
                println!("iter!!!!!!!next ok!!!!!");
            }
        ));
        thread::sleep(Duration::from_millis(1000));

        //迭代器(同步方法)
        // match txn.iter(&tab, None, false, true, "test".to_string(), Arc::new(|v| {})) {
        //     Some(v) => {
        //         let mut iter = v.unwrap();
        //         println!("iter!!!!!!!start!!!!!");
        //         assert_eq!(iter.state(), Ok(true));
        //         println!("qqqqqqqqqq");
        //         assert_eq!(iter.key(), Arc::new(b"k2".to_vec()));
        //         println!("wwwwwwwwwwwww");
        //         assert_eq!(iter.value(), Some(Arc::new(b"v22".to_vec())));
        //         //下一个
        //         iter.next();
        //         println!("iter!!!!!!!next!!!!!");
        //         assert_eq!(iter.state(), Ok(true));
        //         assert_eq!(iter.key(), Arc::new(b"k3".to_vec()));
        //         assert_eq!(iter.value(), Some(Arc::new(b"v33".to_vec())));
        //         println!("iter!!!!!!!next ok!!!!!");
        //     }
        //     None => println!("iter!!!!!!!error!!!!!"),
        // }

        println!("---------------------");
        //预提交
        txn.prepare(Arc::new(
            |v| {
                assert!(v.unwrap() == 1);
                println!("prepare ok!!!!!");
            }
        ));
        thread::sleep(Duration::from_millis(1000));
        println!("---------------------");
        //提交
        txn.commit(Arc::new(
            |v| {
                assert!(v.unwrap() == 1);
                println!("commit ok!!!!!");
            }
        ));
        println!("22222222");
        thread::sleep(Duration::from_millis(1000));
        println!("---------------------");
    }
}
