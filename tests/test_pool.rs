extern crate lmdb;
extern crate pi_db;
extern crate pi_lib;
extern crate pi_store;

use std::path::Path;
use std::sync::Arc;
use std::thread;

use pi_store::pool::LmdbMessage;
use pi_store::pool::ThreadPool;

use pi_lib::atom::Atom;
use pi_lib::bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_lib::guid::Guid;
use pi_lib::sinfo::{EnumType, StructInfo};

use pi_db::db::{
    Bin, CommitResult, DBResult, Filter, Iter, IterResult, KeyIterResult, MetaTxn, NextResult,
    OpenTab, RwLog, SResult, Tab, TabKV, TabMeta, TabTxn, TxCallback, TxQueryCallback, TxState,
    Txn, Ware, WareSnapshot,
};

use lmdb::{
    mdb_set_compare, Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Error,
    MDB_cmp_func, MDB_dbi, MDB_txn, MDB_val, RoCursor, RoTransaction, RwCursor, RwTransaction,
    Transaction, WriteFlags,
};

#[test]
fn test_thread_pool() {
    let env = Arc::new(
        Environment::new()
            .set_flags(EnvironmentFlags::NO_TLS)
            .set_max_dbs(1024)
            .open(Path::new("testdb"))
            .unwrap(),
    );
    thread::sleep_ms(50);

    let mut p = ThreadPool::new();
    p.start_pool(10, env.clone());

    let tx = p.pop().unwrap();

    let _ = env.create_db(Some("test"), DatabaseFlags::empty());

    let mut wb1 = WriteBuffer::new();
    wb1.write_utf8("key4");
    let tab_name = Atom::from("player");
    let ware_name = Atom::from("file_test");
    let k1 = Arc::new(wb1.get_byte().to_vec());
    let value1 = Arc::new(Vec::from(String::from("value2").as_bytes()));
    let item1 = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        k1.clone(),
        0,
        Some(value1.clone()),
    );
    let items = Arc::new(vec![item1.clone()]);

    // test modify
    tx.send(LmdbMessage::Modify(
        "test".to_string(),
        items.clone(),
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    ));
    thread::sleep_ms(50);

    //test commit
    tx.send(LmdbMessage::Commit(
        "test".to_string(),
        Arc::new(move |c| {
            // c.is_err();
        }),
    ))
    .is_ok();
    thread::sleep_ms(50);

    // test iter items
    tx.send(LmdbMessage::CreateItemIter("test".to_string(), true, None));
    thread::sleep_ms(50);

    // test iter items
    tx.send(LmdbMessage::CreateItemIter("test".to_string(), true, None));
    thread::sleep_ms(50);

    // test next item
    tx.send(LmdbMessage::NextItem(
        "test".to_string(),
        Arc::new(move |item| {
            println!("item: {:?}", item);
        }),
    ));

    // test iter kyes
    tx.send(LmdbMessage::CreateKeyIter("test".to_string(), true, None));
    thread::sleep_ms(50);

    // test next key
    tx.send(LmdbMessage::NextKey(
        "test".to_string(),
        Arc::new(move |key| {
            println!("key: {:?}", key);
        }),
    ));

    // test query
    tx.send(LmdbMessage::Query(
        "test".to_string(),
        items.clone(),
        Arc::new(move |q| {
            println!("queried value: {:?}", q);
        }),
    ));
    thread::sleep_ms(50);

    // test rollback
    let mut wb2 = WriteBuffer::new();
    wb2.write_utf8("rollback_key");
    let rollback_key = Arc::new(wb2.get_byte().to_vec());
    let rollback_value = Arc::new(Vec::from(String::from("rollback_value").as_bytes()));
    let rollback_item = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        rollback_key.clone(),
        0,
        Some(rollback_value.clone()),
    );
    let rollback_item = Arc::new(vec![rollback_item.clone()]);

    tx.send(LmdbMessage::Modify(
        "test".to_string(),
        rollback_item.clone(),
        Arc::new(move |m| {}),
    ));
    tx.send(LmdbMessage::Rollback(
        "test".to_string(),
        Arc::new(move |r| {
            println!("rollbacked: {:?}", r);
        }),
    ));

    tx.send(LmdbMessage::Query(
        "test".to_string(),
        rollback_item.clone(),
        Arc::new(move |q| {
            println!("queried value after rollback: {:?}", q);
        }),
    ));
    thread::sleep_ms(50);

    p.push(tx);

    assert_eq!(p.idle_threads(), 10);
}

fn create_tabkv(ware: Atom, tab: Atom, key: Bin, index: usize, value: Option<Bin>) -> TabKV {
    TabKV {
        ware,
        tab,
        key,
        index,
        value,
    }
}
