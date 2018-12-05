extern crate pi_base;
extern crate pi_db;
extern crate pi_lib;
extern crate pi_store;
extern crate tempdir;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use pi_base::pi_base_impl::STORE_TASK_POOL;
use pi_base::worker_pool::WorkerPool;

use pi_lib::atom::Atom;
use pi_lib::bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_lib::guid::Guid;
use pi_lib::sinfo::{EnumType, StructInfo};

use pi_db::db::{
    Bin, CommitResult, DBResult, Filter, Iter, IterResult, KeyIterResult, MetaTxn, NextResult,
    OpenTab, RwLog, SResult, Tab, TabKV, TabMeta, TabTxn, TxCallback, TxQueryCallback, TxState,
    Txn, Ware, WareSnapshot,
};

use pi_store::lmdb_file::{LmdbTable, LmdbWareHouse};

#[test]
fn test_create_transaction() {
    // https://www.openldap.org/lists/openldap-devel/201409/msg00001.html
    // lmdb-sys/lmdb/libraries/liblmdb/mdb.c +2687
    let tab = LmdbTable::new(&Atom::from("test"));

    // let tab = Arc::new(tab);
    let txn1 = tab.transaction(&Guid(0), false);
    let txn2 = tab.transaction(&Guid(0), false);
    let txn3 = tab.transaction(&Guid(0), false);
}

#[test]
fn test_get_put_iter() {
    let tab = Arc::new(LmdbTable::new(&Atom::from("test")));
    let txn4 = tab.transaction(&Guid(3), true);

    let tab_name = Atom::from("player");
    let ware_name = Atom::from("file_test");

    let key1 = Arc::new(Vec::from(String::from("key1").as_bytes()));
    let value1 = Arc::new(Vec::from(String::from("value1").as_bytes()));
    let key2 = Arc::new(Vec::from(String::from("key2").as_bytes()));
    let value2 = Arc::new(Vec::from(String::from("value2").as_bytes()));
    let key3 = Arc::new(Vec::from(String::from("key3").as_bytes()));
    let value3 = Arc::new(Vec::from(String::from("value3").as_bytes()));

    let mut wb1 = WriteBuffer::new();
    wb1.write_utf8("key1");
    let k1 = Arc::new(wb1.get_byte().to_vec());

    let mut wb2 = WriteBuffer::new();
    wb2.write_utf8("key2");
    let k2 = Arc::new(wb2.get_byte().to_vec());

    let mut wb3 = WriteBuffer::new();
    wb3.write_utf8("key3");
    let k3 = Arc::new(wb3.get_byte().to_vec());

    let item1 = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        k1.clone(),
        0,
        Some(value1.clone()),
    );
    let item2 = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        k2.clone(),
        0,
        Some(value2.clone()),
    );
    let item3 = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        k3.clone(),
        0,
        Some(value3.clone()),
    );
    let items = Arc::new(vec![item1.clone(), item2.clone(), item3.clone()]);

    txn4.modify(
        items.clone(),
        None,
        false,
        Arc::new(move |modify| {
            println!("modify data: {:?}", modify);
        }),
    );
    thread::sleep_ms(50);

    txn4.commit(Arc::new(move |c| {
        println!("commit");
    }));
    thread::sleep_ms(50);

    txn4.rollback(Arc::new(move |c| {
        println!("rollback");
    }));
    thread::sleep_ms(50);

    txn4.query(
        items.clone(),
        None,
        false,
        Arc::new(move |query| {
            println!("query data: {:?}", query);
        }),
    );
    thread::sleep_ms(50);

    txn4.iter(
        None,
        false,
        None,
        Arc::new(move |items| {
            items.unwrap().next(Arc::new(move |item| {
                println!("get item: {:?}", item);
            }));
        }),
    );
    thread::sleep_ms(50);
    // println!("{:?}", tab);
}

#[test]
fn test_lmdb_ware_house() {
    if !Path::new("_$lmdb").exists() {
        fs::create_dir("_$lmdb");
    }
    if !Path::new("_$sinfo").exists() {
        fs::create_dir("_$sinfo");
    }

    let db = LmdbWareHouse::new(Atom::from("testdb")).unwrap();

    let snapshot = db.snapshot();

    let tab_name = Atom::from("test_table");
    let tab_name_1 = Atom::from("test_table_1");
    let ware_name = Atom::from("testdb");

    let tab_name_1 = Atom::from("test_table_1");
    let tab_name_2 = Atom::from("test_table_2");

    let sinfo = Arc::new(TabMeta::new(
        EnumType::Str,
        EnumType::Struct(Arc::new(StructInfo::new(tab_name.clone(), 8888))),
    ));
    snapshot.alter(&tab_name_1, Some(sinfo.clone()));
    snapshot.alter(&tab_name_2, Some(sinfo.clone()));

    // there should be three tables: "player", "test_tab_1" and "_$sinfo"
    assert_eq!(snapshot.list().into_iter().count(), 3);
    assert!(snapshot.tab_info(&Atom::from("test_table_1")).is_some());
    assert!(snapshot
        .tab_info(&Atom::from("does_not_exist_table"))
        .is_none());

    let meta_txn = snapshot.meta_txn(&Guid(0));
    let tab_txn1 = snapshot
        .tab_txn(&Atom::from("_$sinfo"), &Guid(0), true, Box::new(|_r| {}))
        .unwrap()
        .expect("create player tab_txn fail");

    let mut wb1 = WriteBuffer::new();
    wb1.write_utf8("key1");
    let key1 = Arc::new(wb1.get_byte().to_vec());

    let value1 = Arc::new(Vec::from(String::from("value1").as_bytes()));
    let item1 = create_tabkv(
        ware_name.clone(),
        Atom::from("_$sinfo"),
        key1.clone(),
        0,
        Some(value1.clone()),
    );
    let arr = Arc::new(vec![item1.clone()]);

    tab_txn1.modify(
        arr.clone(),
        None,
        false,
        Arc::new(move |alter| {
            assert!(alter.is_ok());

            let meta_txn_clone = meta_txn.clone();
            let meta_txn = meta_txn.clone();
            meta_txn_clone.prepare(
                1000,
                Arc::new(move |prepare| {
                    assert!(prepare.is_ok());
                    meta_txn.commit(Arc::new(move |commit| {
                        match commit {
                            Ok(_) => (),
                            Err(e) => panic!("{:?}", e),
                        };
                        println!("meta_txn commit success");
                    }));
                }),
            );
        }),
    );
    thread::sleep_ms(50);

    tab_txn1.query(
        arr,
        None,
        true,
        Arc::new(move |q| {
            println!("test query: {:?}", q);
        }),
    );
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
