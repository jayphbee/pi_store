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

fn create_tabkv(ware: Atom, tab: Atom, key: Bin, index: usize, value: Option<Bin>) -> TabKV {
    TabKV {
        ware,
        tab,
        key,
        index,
        value,
    }
}

fn build_db_key(key: &str) -> Arc<Vec<u8>> {
    let mut wb = WriteBuffer::new();
    wb.write_utf8(key);
    Arc::new(wb.get_byte().to_vec())
}

fn build_db_val(val: &str) -> Arc<Vec<u8>> {
    Arc::new(Vec::from(String::from(val).as_bytes()))
}

#[test]
fn test_get_put_iter() {
    let tab = Arc::new(LmdbTable::new(&Atom::from("test")));
    let txn = tab.transaction(&Guid(3), true);

    let tab_name = Atom::from("player");
    let ware_name = Atom::from("file_test");

    let key1 = build_db_key("key1");
    let value1 = build_db_val("value1");
    let key2 = build_db_key("key1");
    let value2 = build_db_val("value2");
    let key3 = build_db_key("key1");
    let value3 = build_db_val("value3");

    let item1 = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        key1.clone(),
        0,
        Some(value1.clone()),
    );
    let item2 = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        key2.clone(),
        0,
        Some(value2.clone()),
    );
    let item3 = create_tabkv(
        ware_name.clone(),
        tab_name.clone(),
        key3.clone(),
        0,
        Some(value3.clone()),
    );
    let items = Arc::new(vec![item1.clone(), item2.clone(), item3.clone()]);

    txn.modify(
        items.clone(),
        None,
        false,
        Arc::new(move |modify| {
            println!("modify data: {:?}", modify);
        }),
    );
    thread::sleep_ms(50);

    txn.commit(Arc::new(move |c| {
        println!("commit");
    }));
    thread::sleep_ms(50);

    txn.rollback(Arc::new(move |c| {
        println!("rollback");
    }));
    thread::sleep_ms(50);

    txn.query(
        items.clone(),
        None,
        false,
        Arc::new(move |query| {
            println!("query data: {:?}", query);
        }),
    );
    thread::sleep_ms(50);

    txn.iter(
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
