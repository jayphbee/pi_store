extern crate pi_store;
extern crate pi_lib;
extern crate pi_db;
extern crate pi_base;
extern crate tempdir;

use std::sync::Arc;
use std::thread;

use pi_base::worker_pool::WorkerPool;
use pi_base::pi_base_impl::STORE_TASK_POOL;

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
    // let t1 = tab.clone();
    // let t2 = tab.clone();
    // let t3 = tab.clone();

    // thread::spawn(move || {
    //     t1.transaction(&Guid(0), false);
    // });
    // thread::sleep_ms(1000);

    // thread::spawn(move || {

    //     t2.transaction(&Guid(0), false);
    // });
    // thread::sleep_ms(1000);

    // thread::spawn(move || {
    //     t3.transaction(&Guid(0), true);
    // });
    // thread::sleep_ms(1000);
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

    let item1 = create_tabkv(ware_name.clone(), tab_name.clone(), k1.clone(), 0, Some(value1.clone()));
    let item2 = create_tabkv(ware_name.clone(), tab_name.clone(), k2.clone(), 0, Some(value2.clone()));
    let item3 = create_tabkv(ware_name.clone(), tab_name.clone(), k3.clone(), 0, Some(value3.clone()));
    let items =  Arc::new(vec![item1.clone(), item2.clone(), item3.clone()]);

    txn4.modify(items.clone(), None, false, Arc::new(move |modify| {
        println!("modify data: {:?}", modify);
    }));
    thread::sleep_ms(1000);

    txn4.commit(Arc::new(move |c| {
        println!("commit");
    }));
    thread::sleep_ms(1000);

    txn4.rollback(Arc::new(move |c| {
        println!("rollback");
    }));
    thread::sleep_ms(1000);

    txn4.query(items.clone(), None, false, Arc::new(move |query| {
        println!("query data: {:?}", query);
    }));
    thread::sleep_ms(1000);

    txn4.iter(None, false, None, Arc::new(move |items| {
        items.unwrap().next(Arc::new(move |item| {
            println!("get item: {:?}", item);
        }));
    }));
    thread::sleep_ms(1000);
    // println!("{:?}", tab);
}

#[test]
fn test_lmdb_ware_house() {
    // let db = LmdbWareHouse::new(Atom::from("testdb")).unwrap();
    // let db1 = LmdbWareHouse::new(Atom::from("hello")).unwrap();

    // for t in db1.list() {
    //     println!("{:?}", t);
    // }

    // // get tab info
    // println!("yyyyyyyyyy, {:?}", db.tab_info(&Atom::from("_$sinfo1")));

    // let snapshot = db.snapshot();

    // let meta_txn = snapshot.meta_txn(&Guid(0));
    // let tab_name = Atom::from("player");

    // let sinfo = Arc::new(TabMeta::new(EnumType::Str, EnumType::Struct(Arc::new(StructInfo::new(tab_name.clone(), 8888)))));
    // snapshot.alter(&tab_name, Some(sinfo.clone()));
    // println!("xxxx, {:?}", snapshot.list().next());

    // let tab_txn1 = snapshot.tab_txn(&Atom::from("_$sinfo"), &Guid(0), true, Box::new(|_r|{})).unwrap().expect("create player tab_txn fail");
}

fn create_tabkv(ware: Atom, tab: Atom, key: Bin, index: usize, value: Option<Bin>,) -> TabKV{
    TabKV{ware, tab, key, index, value}
}