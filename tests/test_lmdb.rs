extern crate pi_base;
extern crate pi_db;
extern crate pi_lib;
extern crate pi_store;
extern crate tempdir;

use std::cell::RefCell;
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
    Arc::new(wb.get_byte().to_vec())
}

#[test]
fn test_lmdb_single_thread() {
    if Path::new("testdb").exists() {
        fs::remove_dir_all("testdb");
    }

    let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();

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

    meta_txn.alter(
        &Atom::from("test_table_1"),
        Some(sinfo.clone()),
        Arc::new(move |alter| {
            assert!(alter.is_ok());
        }),
    );
    meta_txn.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );
    meta_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);

    // newly created table should be there
    assert!(snapshot.tab_info(&Atom::from("_$sinfo")).is_some());
    assert!(snapshot.tab_info(&Atom::from("test_table_1")).is_some());

    for t in snapshot.list().into_iter() {
        println!("tables: {:?}", t);
    }

    // insert items to "test_table_1"
    let tab_txn = snapshot
        .tab_txn(
            &Atom::from("test_table_1"),
            &Guid(4),
            true,
            Box::new(|_r| {}),
        )
        .unwrap()
        .unwrap();

    let key1 = build_db_key("key1");
    let value1 = build_db_val("value1");

    let key2 = build_db_key("key2");
    let value2 = build_db_val("value2");

    let key3 = build_db_key("key3");
    let value3 = build_db_val("value3");

    let item1 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        key1.clone(),
        0,
        Some(value1.clone()),
    );
    let arr = Arc::new(vec![item1.clone()]);

    let item2 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        key2.clone(),
        0,
        Some(value2.clone()),
    );
    let arr2 = Arc::new(vec![item2.clone()]);

    let item3 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        key3.clone(),
        0,
        Some(value3.clone()),
    );
    let arr3 = Arc::new(vec![item1.clone(), item2.clone(), item3.clone()]);

    // insert data consecutively
    tab_txn.modify(
        arr.clone(),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );
    // insert data
    tab_txn.modify(
        arr3.clone(),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );
    // insert data
    tab_txn.modify(
        arr2.clone(),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );

    // pre commit
    tab_txn.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );

    // commit
    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);

    // query inserted value should be succeed
    tab_txn.query(
        arr,
        None,
        false,
        Arc::new(move |q| {
            assert!(q.is_ok());
            println!("queried value: {:?}", q);
        }),
    );

    // insert data again to test rollback
    tab_txn.modify(
        arr2.clone(),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );

    // rollback
    tab_txn.rollback(Arc::new(move |r| {
        assert!(r.is_ok());
    }));

    // rollbacked data query get None value
    tab_txn.query(
        arr2.clone(),
        None,
        false,
        Arc::new(move |q| {
            if let Ok(v) = q.clone() {
                println!("v: {:?}", v);
            }
            // query is ok, but 'value' field should be  None
            assert!(q.is_ok());
        }),
    );

    // insert more item
    tab_txn.modify(
        arr2.clone(),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );

    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    // delete item
    let delete_item2 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        key2.clone(),
        0,
        None,
    );
    let delted_arr2 = Arc::new(vec![delete_item2.clone()]);
    tab_txn.modify(
        delted_arr2.clone(),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );

    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    // iter items from end to begining, there should be only one item left
    tab_txn.iter(
        None,
        false,
        None,
        Arc::new(move |iter| {
            assert!(iter.is_ok());
            let mut it = iter.unwrap();

            for i in 0..3 {
                it.next(Arc::new(move |item| {
                    println!("item {:?}: {:?}", i, item);
                }));
            }
        }),
    );

    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    // empty txn commit should always succeed
    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    // empty txn rollback should always succeed
    tab_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);

    // ------------------------------------------------------
    let k1 = build_db_key("foo1");
    let v1 = build_db_val("bar1");

    let k2 = build_db_key("foo2");
    let v2 = build_db_val("bar2");

    let k3 = build_db_key("foo3");
    let v3 = build_db_val("bar3");

    let k4 = build_db_key("foo4");
    let v4 = build_db_val("bar4");

    let k5 = build_db_key("foo5");
    let v5 = build_db_val("bar5");

    let item1 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        k1.clone(),
        0,
        Some(v1.clone()),
    );
    let delete_item1 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        k1.clone(),
        0,
        None,
    );
    let item2 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        k2.clone(),
        0,
        Some(v2.clone()),
    );
    let item3 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        k3.clone(),
        0,
        Some(v3.clone()),
    );
    let item4 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        k4.clone(),
        0,
        Some(v4.clone()),
    );
    let item5 = create_tabkv(
        Atom::from("testdb"),
        Atom::from("test_table_1"),
        k5.clone(),
        0,
        Some(v5.clone()),
    );

    // test multiply txns read write delete
    let txn1 = snapshot
        .tab_txn(
            &Atom::from("test_table_1"),
            &Guid(0),
            true,
            Box::new(|_r| {}),
        )
        .unwrap()
        .unwrap();

    let txn2 = snapshot
        .tab_txn(
            &Atom::from("test_table_1"),
            &Guid(1),
            true,
            Box::new(|_r| {}),
        )
        .unwrap()
        .unwrap();

    let txn3 = snapshot
        .tab_txn(
            &Atom::from("test_table_1"),
            &Guid(2),
            true,
            Box::new(|_r| {}),
        )
        .unwrap()
        .unwrap();

    // txn1 insert item1
    txn1.modify(
        Arc::new(vec![item1.clone()]),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );
    // txn2 insert item2
    txn2.modify(
        Arc::new(vec![item2]),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );
    // txn1 pre commit
    txn1.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );
    // txn1 commit
    txn1.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));
    // txn3 insert item3, item4, item5
    txn3.modify(
        Arc::new(vec![item3, item4, item5]),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );
    // txn2 rollback
    txn2.rollback(Arc::new(move |r| {
        assert!(r.is_ok());
    }));
    // txn2 pre-commit
    txn2.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );
    // txn2 commit
    txn2.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));
    // txn1 delete item1
    txn1.modify(
        Arc::new(vec![delete_item1]),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());
        }),
    );
    // txn3 pre commit
    txn3.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );
    // commit txn3
    txn3.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));
    // txn1 pre commit
    txn1.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );
    // commit txn1
    txn1.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));
    // use txn3 to query item1, value should be None
    txn3.query(
        Arc::new(vec![item1.clone()]),
        None,
        false,
        Arc::new(move |q| {
            assert!(q.is_ok());
            println!("queried value: {:?}", q);
        }),
    );

    // reuse txn1 to iterate inserted items, there should be item3, item4, item5
    // puls the previously inserted value1 and value3 because item1 is deleted, item2 is rollbacked
    txn1.iter(
        None,
        false,
        None,
        Arc::new(move |iter| {
            assert!(iter.is_ok());
            let mut it = iter.unwrap();

            for i in 0..6 {
                it.next(Arc::new(move |item| {
                    println!("item {:?}: {:?}", i, item);
                }));
            }
        }),
    );

    // NOTE: txn1 should be committed, or will block other write txns
    txn1.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);
}

#[test]
fn test_lmdb_multi_thread() {
    if Path::new("testdb").exists() {
        fs::remove_dir_all("testdb");
    }

    let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();

    let snapshot = db.snapshot();
    // newly created DB should have a "_$sinfo" table
    assert!(snapshot.tab_info(&Atom::from("_$sinfo")).is_some());

    // insert table meta info to "_$sinfo" table
    let sinfo = Arc::new(TabMeta::new(
        EnumType::Str,
        EnumType::Struct(Arc::new(StructInfo::new(Atom::from("test_table_2"), 8888))),
    ));
    snapshot.alter(&Atom::from("test_table_2"), Some(sinfo.clone()));

    let meta_txn = snapshot.meta_txn(&Guid(0));

    meta_txn.alter(
        &Atom::from("test_table_2"),
        Some(sinfo.clone()),
        Arc::new(move |alter| {
            assert!(alter.is_ok());
        }),
    );
    meta_txn.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );
    meta_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);

    // concurrently insert 3000 items with 3 threads
    let h1 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn1 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(0),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 0..1000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn1.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }

        txn1.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        txn1.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    let h2 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn2 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(1),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 1000..2000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn2.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }

        txn2.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        txn2.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    let h3 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn3 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(2),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 2000..3000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn3.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }

        txn3.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        txn3.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    assert!(h1.join().is_ok());
    assert!(h2.join().is_ok());
    assert!(h3.join().is_ok());

    // thread 4 iterate all the inserted value from begining to end
    let h4 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn4 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(3),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        txn4.iter(
            None,
            true,
            None,
            Arc::new(move |iter| {
                assert!(iter.is_ok());

                let mut it = iter.unwrap();
                for i in 0..3001 {
                    it.next(Arc::new(move |item| {
                        if let Ok(elem) = item {
                            if elem.is_none() {
                                // the 3000th item is None
                                assert_eq!(3000, i);
                                println!("multi thread item {:?}: {:?}", i, elem);
                            } else {
                                assert_ne!(3000, i);
                            }
                        }
                    }));
                }
            }),
        );

        txn4.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );

        // NOTE: txn4 should be committed, or will block other write txns
        txn4.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    assert!(h4.join().is_ok());

    thread::sleep_ms(1000);

    // query even indexed data items
    let h = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn5 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(4),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        // even indexed items should be there
        (0..3000)
            .step_by(2)
            .map(|i| {
                let k = build_db_key(&format!("test_key{:?}", i));
                let v = build_db_val(&format!("test_value{:?}", i));
                let item = create_tabkv(
                    Atom::from("testdb"),
                    Atom::from("test_table_2"),
                    k.clone(),
                    0,
                    Some(v.clone()),
                );

                txn5.query(
                    Arc::new(vec![item]),
                    None,
                    false,
                    Arc::new(move |q| {
                        assert!(q.is_ok());
                    }),
                );
            })
            .for_each(drop);
    });

    assert!(h.join().is_ok());

    // thread 5, 6, 7 concurrently delete all data inserted before
    let h5 = thread::spawn(move || {
        println!("delete item in thread 5");
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn5 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(4),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 0..1000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                None,
            );

            txn5.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }
        txn5.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        txn5.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    let h6 = thread::spawn(move || {
        println!("delete item in thread 6");
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn6 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(5),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 1000..2000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                None,
            );

            txn6.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }
        txn6.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        txn6.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    let h7 = thread::spawn(move || {
        println!("delete item in thread 7");
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn7 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(6),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 2000..3000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                None,
            );

            txn7.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }
        txn7.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        txn7.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    assert!(h5.join().is_ok());
    assert!(h6.join().is_ok());
    assert!(h7.join().is_ok());

    // check that all items are deleted
    let h8 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn8 = snapshot
            .tab_txn(
                &Atom::from("test_table_2"),
                &Guid(7),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        txn8.iter(
            None,
            true,
            None,
            Arc::new(move |iter| {
                assert!(iter.is_ok());
                let mut it = iter.unwrap();
                for i in 0..6 {
                    it.next(Arc::new(move |item| {
                        println!("None items {:?}: {:?}", i, item);
                    }));
                }
            }),
        );
        txn8.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        txn8.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    assert!(h8.join().is_ok());

    thread::sleep_ms(1000);
}

#[test]
fn test_exception() {
    if Path::new("testdb").exists() {
        fs::remove_dir_all("testdb");
    }

    let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();

    let snapshot = db.snapshot();
    // newly created DB should have a "_$sinfo" table
    assert!(snapshot.tab_info(&Atom::from("_$sinfo")).is_some());

    // insert table meta info to "_$sinfo" table
    let sinfo = Arc::new(TabMeta::new(
        EnumType::Str,
        EnumType::Struct(Arc::new(StructInfo::new(Atom::from("test_table_3"), 8888))),
    ));
    snapshot.alter(&Atom::from("test_table_3"), Some(sinfo.clone()));

    let meta_txn = snapshot.meta_txn(&Guid(0));

    meta_txn.alter(
        &Atom::from("test_table_3"),
        Some(sinfo.clone()),
        Arc::new(move |alter| {
            assert!(alter.is_ok());
        }),
    );
    meta_txn.prepare(
        1000,
        Arc::new(move |p| {
            assert!(p.is_ok());
        }),
    );
    meta_txn.commit(Arc::new(move |c| {
        assert!(c.is_ok());
    }));

    thread::sleep_ms(500);

    // thread 1 is crashed, no items inserted
    let h1 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn1 = snapshot
            .tab_txn(
                &Atom::from("test_table_3"),
                &Guid(0),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 0..1000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn1.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );

            if i == 555 {
                panic!("thread 1 crashed when inserting items");
            }
        }

        txn1.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );

        txn1.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));

        println!("XXXX impossible to reach there XXXXX");
    });

    // thread 2 normally insert items
    let h2 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn2 = snapshot
            .tab_txn(
                &Atom::from("test_table_3"),
                &Guid(0),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 1000..2000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn2.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }

        txn2.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );

        txn2.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));
    });

    // thread 3 successfully insert/commit items and then crashed
    let h3 = thread::spawn(move || {
        let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 100).unwrap();
        let snapshot = db.snapshot();
        let txn3 = snapshot
            .tab_txn(
                &Atom::from("test_table_3"),
                &Guid(0),
                true,
                Box::new(|_r| {}),
            )
            .unwrap()
            .unwrap();

        for i in 2000..3000 {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_2"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn3.modify(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |m| {
                    assert!(m.is_ok());
                }),
            );
        }

        txn3.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );

        txn3.commit(Arc::new(move |c| {
            assert!(c.is_ok());
        }));

        panic!("thread 3 crashed after commit");
    });

    assert!(h1.join().is_err());
    assert!(h2.join().is_ok());
    assert!(h3.join().is_err());

    let txn1 = snapshot
        .tab_txn(
            &Atom::from("test_table_3"),
            &Guid(1),
            true,
            Box::new(|_r| {}),
        )
        .unwrap()
        .unwrap();

    (0..1000)
        .map(|i| {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_3"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn1.query(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |q| {
                    // query result is ok, but value is None
                    assert!(q.is_ok());
                    println!("after crash {:?} : {:?}", i, q);
                }),
            );
        })
        .for_each(drop);

    (1000..3000)
        .map(|i| {
            let k = build_db_key(&format!("test_key{:?}", i));
            let v = build_db_val(&format!("test_value{:?}", i));
            let item = create_tabkv(
                Atom::from("testdb"),
                Atom::from("test_table_3"),
                k.clone(),
                0,
                Some(v.clone()),
            );

            txn1.query(
                Arc::new(vec![item]),
                None,
                false,
                Arc::new(move |q| {
                    assert!(q.is_ok());
                    println!("after crash {:?} : {:?}", i, q);
                }),
            );
        })
        .for_each(drop);

    thread::sleep_ms(1000);
}
