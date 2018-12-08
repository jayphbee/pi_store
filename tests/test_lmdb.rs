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

use pi_store::lmdb_file::{LmdbTable, DB};

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
    let mut wb = WriteBuffer::new();
    wb.write_utf8(val);

    // Arc::new(Vec::from(String::from(val).as_bytes()))
    // println!("value: {:?}", Arc::new(wb.get_byte().to_vec()));
    Arc::new(wb.get_byte().to_vec())
}

#[test]
fn test_lmdb_ware_house() {
    let db = DB::new(Atom::from("testdb")).unwrap();

    let snapshot = db.snapshot();
    // newly created DB should have a "_$sinfo" table
    assert!(snapshot.tab_info(&Atom::from("_$sinfo")).is_some());

    // insert table meta info to "_$sinfo" table
    let sinfo = Arc::new(TabMeta::new(
        EnumType::Str,
        EnumType::Struct(Arc::new(StructInfo::new(Atom::from("test_table_1"), 8888))),
    ));
    snapshot.alter(&Atom::from("test_table_1"), Some(sinfo.clone()));

    let meta_txn = snapshot.meta_txn(&Guid(0));

    meta_txn.alter(&Atom::from("test_table_1"), Some(sinfo.clone()), Arc::new(move |alter| {
        assert!(alter.is_ok());
    }));
    meta_txn.prepare(1000, Arc::new(move |p| {
        assert!(p.is_ok());
    }));
    meta_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);

    // newly created table should be there
    assert!(snapshot.tab_info(&Atom::from("test_table_1")).is_some());

    for t in snapshot.list().into_iter() {
        println!("tables: {:?}", t);
    }

    // insert items to "test_table_1"
    let tab_txn = snapshot
        .tab_txn(&Atom::from("test_table_1"), &Guid(0), true, Box::new(|_r| {}))
        .unwrap()
        .unwrap();

    let key1 = build_db_key("key1");
    let value1 = build_db_val("value1");

    let key2 = build_db_key("key2");
    let value2 = build_db_val("value2");

    let key3 = build_db_key("key3");
    let value3 = build_db_val("value3");

    let item1 = create_tabkv(Atom::from("testdb"), Atom::from("test_table_1"), key1.clone(), 0, Some(value1.clone()));
    let arr =  Arc::new(vec![item1.clone()]);

    let item2 = create_tabkv(Atom::from("testdb"), Atom::from("test_table_1"), key2.clone(), 0, Some(value2.clone()));
    let arr2 =  Arc::new(vec![item2.clone()]);

    let item3 = create_tabkv(Atom::from("testdb"), Atom::from("test_table_1"), key3.clone(), 0, Some(value3.clone()));
    let arr3 =  Arc::new(vec![item1.clone(), item2.clone(), item3.clone()]);

    tab_txn.modify(arr.clone(), None, false, Arc::new(move |m| {
        assert!(m.is_ok());
    }));
    tab_txn.modify(arr3.clone(), None, false, Arc::new(move |m| {
        assert!(m.is_ok());
    }));

    tab_txn.prepare(1000, Arc::new(move |p| {
        assert!(p.is_ok());
    }));
    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);

    tab_txn.query(arr, None, false, Arc::new(move |q| {
        assert!(q.is_ok());
        println!("queried value: {:?}", q);
    }));

    // rollback
    tab_txn.modify(arr2.clone(), None, false, Arc::new(move |m| {
        assert!(m.is_ok());
    }));

    tab_txn.rollback(Arc::new(move |r| {
        assert!(r.is_ok());
    }));

    // insert more item
    tab_txn.modify(arr2.clone(), None, false, Arc::new(move |m| {
        assert!(m.is_ok());
    }));

    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    // iter items from end to begining
    tab_txn.iter(None, false, None, Arc::new(move |iter| {
        assert!(iter.is_ok());
        let mut it = iter.unwrap();

        it.next(Arc::new(move |item| {
            println!("item: {:?}", item);
        }));
        it.next(Arc::new(move |item| {
            println!("item: {:?}", item);
        }));
        it.next(Arc::new(move |item| {
            println!("item: {:?}", item);
        }));
    }));

    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    // empty txn commit should succeed
    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    // empty txn rollback should be succeed
    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);
}
