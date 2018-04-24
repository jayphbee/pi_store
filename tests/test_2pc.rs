extern crate rocksdb;
extern crate pi_store;
extern crate pi_db;
extern crate pi_lib;
extern crate pi_vm;

use std::sync::Arc;
use std::time::Duration;
use std::thread;

use rocksdb::{TXN_DB, TXN, Options, TransactionDBOptions, TransactionOptions, WriteOptions, ReadOptions};

use pi_store::db::{LocalTXN, STORE_TASK_POOL};
use pi_db::db::{TabKV, Txn, DBResult};
use pi_lib::atom::{Atom};
use pi_vm::worker_pool::WorkerPool;
use std::str;

fn tabkv(tab: String, key: Vec<u8>, v: Option<Arc<Vec<u8>>>) -> Vec<TabKV> {
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
    let path = "_rust_rocksdb_2pc";
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut txn_opts = TransactionOptions::default();
        let mut txn_db_opts = TransactionDBOptions::default();
        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();
        //打开rocksdb
        let db = TXN_DB::open(&opts, &txn_db_opts, &path).unwrap();
        println!("111111111111111111");
        //创建事务
        let mut txn = LocalTXN {
            txn: TXN::begin(&db, &write_opts, &txn_opts).unwrap(),
        };
        println!("22222222222222222222222");
        //为2pc事务命名（必要，否则无法使用预提交）
        assert!(txn.txn.setName("xid111").is_ok());
        println!("33333333333333333333");
        //插入测试数据
        let mut arrTabkv = Vec::new();
        let mut tabkv2 = TabKV::new(path.to_string(), b"k1".to_vec());
        tabkv2.value = Some(Arc::new(b"v11".to_vec()));
        arrTabkv.push(tabkv2);
        let mut tabkv2 = TabKV::new(path.to_string(), b"k2".to_vec());
        tabkv2.value = Some(Arc::new(b"v22".to_vec()));
        arrTabkv.push(tabkv2);
        let mut tabkv2 = TabKV::new(path.to_string(), b"k3".to_vec());
        tabkv2.value = Some(Arc::new(b"v33".to_vec()));
        arrTabkv.push(tabkv2);
        let mut tabkv2 = TabKV::new(path.to_string(), b"k4".to_vec());
        tabkv2.value = Some(Arc::new(b"v44".to_vec()));
        arrTabkv.push(tabkv2);

        let p = txn.modify(arrTabkv, Some(0), Arc::new(
            |v: Result<usize, String>|{
                println!("modify!!!!!!!!!!!!!!!!!!!!!!ok");
                assert!(v.unwrap() == 4);
            }
        ));
        thread::sleep(Duration::from_millis(1000));
        
        println!("555555555555555");
        //删除k1
        let p = txn.modify(tabkv(path.to_string(), b"k1".to_vec(), None), Some(0), Arc::new(
            |v1: Result<usize, String>| {
                assert!(v1.unwrap() == 1);
                // let r: Option<DBResult<Vec<TabKV>>> = txn.query(tabkv(path.to_string(), b"k1".to_vec(), None), Some(0), Arc::new(
                //     |v2| {
                //         for values in v2.unwrap() {
                //         assert!(values.value == None);
                // }
                //     }
                // ));     
            }
        ));
        thread::sleep(Duration::from_millis(1000));
        //查询k3
        let r: Option<DBResult<Vec<TabKV>>> = txn.query(tabkv(path.to_string(), b"k3".to_vec(), None), Some(0), Arc::new(
            |v: DBResult<Vec<TabKV>>| {
                for values in v.unwrap() {
                    assert!(str::from_utf8(values.value.unwrap().as_slice()).unwrap() == "v33");
                }
            }
        ));
        thread::sleep(Duration::from_millis(1000));
        println!("21212112112121");
        //迭代
        let p = txn.iter(Atom::from(path.to_string()), None, false, true, "test".to_string(), Arc::new(
            |v| {
                let mut iter = v.unwrap();
                assert_eq!(iter.state(), Ok(true));
                assert_eq!(iter.key(), b"k2");
                assert_eq!(iter.value(), Some(Arc::new(b"v22".to_vec())));
                println!("13222212123123");
                //下一个
                iter.next();
                assert_eq!(iter.state(), Ok(true));
                assert_eq!(iter.key(), b"k3");
                assert_eq!(iter.value(), Some(Arc::new(b"v33".to_vec())));
            }
        ));
        //
        // assert!(txn.prepare(Arc::new(|a|println!("ttttt"))).unwrap().is_ok());
        // assert!(txn.commit(Arc::new(|a|println!("ttttt"))).unwrap().is_ok());
        thread::sleep(Duration::from_millis(1000));
    }
}
