extern crate pi_store;
extern crate lmdb;
extern crate pi_db;
extern crate pi_lib;

use std::thread;
use std::path::Path;
use std::sync::Arc;

use pi_store::pool::ThreadPool;
use pi_store::pool::LmdbMessage;

use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::sinfo::{EnumType, StructInfo};
use pi_lib::bon::{ReadBuffer, WriteBuffer, Encode, Decode};

use pi_db::db::{
    Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult,
    NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn,
    Tab, OpenTab, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult,
    RwLog, TabMeta
};

use lmdb::{
    Environment, Database, WriteFlags, Error, Transaction, EnvironmentFlags,
    DatabaseFlags, RwTransaction, RoTransaction, RoCursor, Cursor, RwCursor,

    mdb_set_compare, MDB_txn, MDB_dbi, MDB_val, MDB_cmp_func
};

#[test]
fn test_new_txn() {
    let mut p = ThreadPool::with_capacity(10);
    let tx = p.pop().unwrap();

    let env = Arc::new(Environment::new()
                .set_flags(EnvironmentFlags::NO_TLS)
                .set_max_dbs(1024)
                .open(Path::new("_$lmdb"))
                .unwrap());
    thread::sleep_ms(1000);
    
    let _ = env.create_db(Some("test"), DatabaseFlags::empty());

    // assert_eq!(p.idle_threads(), 3);

    //test new tab txn
    assert_eq!(tx.send(LmdbMessage::NewTxn(env.clone(), "test".to_string(), true)).is_err(), false);
    thread::sleep_ms(1000);

    let mut wb1 = WriteBuffer::new();
    wb1.write_utf8("key1");
    let tab_name = Atom::from("player");
    let ware_name = Atom::from("file_test");
    let k1 = Arc::new(wb1.get_byte().to_vec());
    let value1 = Arc::new(Vec::from(String::from("value1").as_bytes()));
    let item1 = create_tabkv(ware_name.clone(), tab_name.clone(), k1.clone(), 0, Some(value1.clone()));
    let items =  Arc::new(vec![item1.clone()]);

    // test modify
    tx.send(LmdbMessage::Modify(env.clone(), "test".to_string(), items.clone(), Arc::new(move |m| {
        assert!(m.is_err());
    })));
    thread::sleep_ms(1000);

    //test commit
    tx.send(LmdbMessage::Commit(env.clone(), "test".to_string(), Arc::new(move |c| {
        c.is_err();
    }))).is_ok();
    thread::sleep_ms(1000);

    p.push(tx);
    let tx1 = p.pop().unwrap();

    assert_eq!(tx1.send(LmdbMessage::NewTxn(env.clone(), "test".to_string(), true)).is_err(), false);
    thread::sleep_ms(1000);

    // test query
    tx1.send(LmdbMessage::Query(env.clone(), "test".to_string(), items.clone(), Arc::new(move |q| {
        // q.is_err();
    })));
    thread::sleep_ms(1000);
}

fn create_tabkv(ware: Atom, tab: Atom, key: Bin, index: usize, value: Option<Bin>,) -> TabKV {
    TabKV{ware, tab, key, index, value}
}
