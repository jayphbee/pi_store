extern crate pi_db;
extern crate pi_store;
extern crate tempdir;

extern crate atom;
extern crate bon;
extern crate guid;
extern crate sinfo;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time;

use atom::Atom;
use bon::{Decode, Encode, ReadBonErr, ReadBuffer, WriteBuffer};
use guid::Guid;
use sinfo::{EnumType, StructInfo};

use pi_db::db::{Bin, TabKV, TabMeta, Ware};

use pi_store::lmdb_file::DB;

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
        let _ = fs::remove_dir_all("testdb");
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
    let _ = snapshot.prepare(&Guid(0));
    snapshot.commit(&Guid(0));

    let meta_txn = snapshot.meta_txn(&Guid(0));
    let m1 = meta_txn.clone();
    let m2 = meta_txn.clone();

    meta_txn.alter(
        &Atom::from("test_table_1"),
        Some(sinfo.clone()),
        Arc::new(move |alter| {
            assert!(alter.is_ok());
            match m1.prepare(1000, Arc::new(move |_p| {})) {
                Some(_) => {
                    match m2.commit(Arc::new(move |c| {
                        assert!(c.is_ok());
                    })) {
                        Some(c) => {
                            println!("commit success: {:?}", c);
                        }

                        None => {
                            println!("async commit");
                        }
                    }
                }
                None => {}
            }
        }),
    );

    thread::sleep(time::Duration::from_millis(500));

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
    let _arr3 = Arc::new(vec![item1.clone(), item2.clone(), item3.clone()]);

    let txn1 = tab_txn.clone();

    txn1.modify(
        arr.clone(),
        None,
        false,
        Arc::new(move |m1| {
            println!("hello world  --------------------- ");
            assert!(m1.is_ok());
            let txn2 = tab_txn.clone();
            let tab_txn = tab_txn.clone();
            txn2.modify(
                arr2.clone(),
                None,
                false,
                Arc::new(move |m2| {
                    assert!(m2.is_ok());
                    let txn3 = tab_txn.clone();
                    txn3.prepare(1000, Arc::new(move |_p| {}));
                    txn3.commit(Arc::new(move |c| {
                        assert!(c.is_ok());
                    }));
                }),
            );
        }),
    );

    thread::sleep(time::Duration::from_millis(500));

    let tab_txn = snapshot
        .tab_txn(
            &Atom::from("test_table_1"),
            &Guid(4),
            true,
            Box::new(|_r| {}),
        )
        .unwrap()
        .unwrap();
    let t1 = tab_txn.clone();
    tab_txn.query(
        Arc::new(vec![item1.clone()]),
        None,
        false,
        Arc::new(move |q| {
            assert!(q.is_ok());
            println!("q: {:?}", q);
            t1.prepare(1000, Arc::new(move |_p| {}));
            t1.commit(Arc::new(move |c| {
                c.is_ok();
            }));
        }),
    );

    thread::sleep(time::Duration::from_millis(500));
}

#[test]
fn test_lmdb_multi_thread() {
    if Path::new("testdb").exists() {
        let _ = fs::remove_dir_all("testdb");
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

    thread::sleep(time::Duration::from_millis(500));

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

        for i in 0..10 {
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
        println!("multi thread 1: modify XXXXXXXXXXXXXXXXXXXXXXXXX ");

        txn1.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );
        println!("multi thread 1: prepare XXXXXXXXXXXXXXXXXXXXXXXXX ");
        thread::sleep(time::Duration::from_millis(500));
        for i in 0..10 {
            txn1.commit(Arc::new(move |c| {
                assert!(c.is_ok());
            }));
            println!("commit for the {:?} time", i);
            thread::sleep(time::Duration::from_millis(500));
        }
    })
    .join();

    thread::sleep(time::Duration::from_millis(500));

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

        for i in 10..20 {
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
        for i in 0..10 {
            txn2.commit(Arc::new(move |c| {
                assert!(c.is_ok());
            }));
            println!("commit for the {:?} time", i);
            thread::sleep(time::Duration::from_millis(500));
        }
    })
    .join();

    thread::sleep(time::Duration::from_millis(500));

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

        for i in 20..30 {
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
        for i in 0..10 {
            txn3.commit(Arc::new(move |c| {
                assert!(c.is_ok());
            }));
            println!("commit for the {:?} time", i);
            thread::sleep(time::Duration::from_millis(500));
        }
    })
    .join();

    thread::sleep(time::Duration::from_millis(500));

    // assert!(h1.join().is_ok());
    // assert!(h2.join().is_ok());
    // assert!(h3.join().is_ok());

    // thread 4 iterate all the inserted value from begining to end
    let h4 = thread::spawn(move || {
        println!("in thread 4 *****************");
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

        txn4.iter(None, true, None, Arc::new(move |_iter| {}));
        let mut iter = txn4
            .iter(None, true, None, Arc::new(move |_| {}))
            .unwrap()
            .unwrap();
        for i in 0..31 {
            iter.next(Arc::new(move |item| {
                if let Ok(elem) = item {
                    println!("multi thread item {:?}: {:?}", i, elem);
                }
            }));
        }

        thread::sleep(time::Duration::from_millis(500));
        txn4.prepare(
            1000,
            Arc::new(move |p| {
                assert!(p.is_ok());
            }),
        );

        // NOTE: txn4 should be committed, or will block other write txns
        for i in 0..10 {
            txn4.commit(Arc::new(move |c| {
                assert!(c.is_ok());
            }));
            println!("commit for the {:?} time", i);
            thread::sleep(time::Duration::from_millis(500));
        }
    })
    .join();

    // assert!(h4.join().is_ok());

    thread::sleep(time::Duration::from_millis(500));

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
        (0..30)
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
        txn5.prepare(1000, Arc::new(|p| {}));
        txn5.commit(Arc::new(|c| {
            assert!(c.is_ok());
        }));
    })
    .join();

    // assert!(h.join().is_ok());

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

        for i in 0..10 {
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
        for i in 0..10 {
            txn5.commit(Arc::new(move |c| {
                assert!(c.is_ok());
            }));
            println!("thread 5 commit for the {:?} time", i);
        }
    })
    .join();

    thread::sleep(time::Duration::from_millis(500));

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

        for i in 10..20 {
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
        for i in 0..10 {
            txn6.commit(Arc::new(move |c| {
                assert!(c.is_ok());
            }));
            println!("thread 6 commit for the {:?} time", i);
        }
    })
    .join();

    thread::sleep(time::Duration::from_millis(500));

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

        for i in 20..30 {
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
        for i in 0..10 {
            txn7.commit(Arc::new(move |c| {
                assert!(c.is_ok());
            }));
            println!("thread 7 commit for the {:?} time", i);
        }
    })
    .join();

    thread::sleep(time::Duration::from_millis(500));

    // assert!(h5.join().is_ok());
    // assert!(h6.join().is_ok());
    // assert!(h7.join().is_ok());

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
                assert!(iter.is_err());
                let mut it = iter.unwrap();
                for i in 0..6 {
                    println!("here");
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

    thread::sleep(time::Duration::from_millis(500));
}

#[test]
fn test_exception() {
    if Path::new("testdb").exists() {
        let _ = fs::remove_dir_all("testdb");
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

    thread::sleep(time::Duration::from_millis(500));

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
                Atom::from("test_table_3"),
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

        unreachable!();
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
                Atom::from("test_table_3"),
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
                Atom::from("test_table_3"),
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
                    println!("value should be none {:?} : {:?}", i, q);
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
                    println!("value should be some {:?} : {:?}", i, q);
                }),
            );
        })
        .for_each(drop);

    thread::sleep(time::Duration::from_millis(500));
}

#[cfg(test)]
#[derive(Debug)]
struct Player {
    name: String,
    id: u32,
}

#[cfg(test)]
impl Encode for Player {
    fn encode(&self, bb: &mut WriteBuffer) {
        self.name.encode(bb);
        self.id.encode(bb);
    }
}

#[cfg(test)]
impl Decode for Player {
    fn decode(bb: &mut ReadBuffer) -> Result<Self, ReadBonErr> {
        Ok(Player {
            name: String::decode(bb)?,
            id: u32::decode(bb)?,
        })
    }
}

#[test]
fn test_file_db_mgr() {
    use guid::GuidGen;
    use pi_db::db::SResult;
    use pi_db::mgr::Mgr;

    let mgr = Mgr::new(GuidGen::new(1, 1));
    let db = DB::new(Atom::from("testdb"), 1024 * 1024 * 10).unwrap();
    mgr.register(Atom::from("testdb"), Arc::new(db));
    let mgr = Arc::new(mgr);

    let tr = mgr.transaction(true);
    let tr1 = tr.clone();
    let tr2 = tr.clone();

    tr.alter(
        &Atom::from("testdb"),
        &Atom::from("test_table_mgr_1"),
        Some(Arc::new(TabMeta {
            k: EnumType::Str,
            v: EnumType::Str,
        })),
        Arc::new(move |a| {
            assert!(a.is_ok());
            match tr1.prepare(Arc::new(move |p| {
                assert!(p.is_ok());
            })) {
                Some(p) => println!("prepare: {:?}", p),
                None => println!("None arm"),
            }
        }),
    );

    tr.alter(
        &Atom::from("testdb"),
        &Atom::from("test_table_mgr_2"),
        Some(Arc::new(TabMeta {
            k: EnumType::Str,
            v: EnumType::Str,
        })),
        Arc::new(move |a| {
            assert!(a.is_ok());
            match tr2.prepare(Arc::new(move |p| {
                assert!(p.is_ok());
            })) {
                Some(p) => println!("prepare: {:?}", p),
                None => println!("None arm"),
            }
        }),
    );

    thread::sleep_ms(1000);

    tr.commit(Arc::new(|c| {
        assert!(c.is_ok());
    }));

    let t1 = mgr.transaction(true);
    let t2 = t1.clone();
    let t3 = t1.clone();

    let mut arr = Vec::new();
    let mut arr2 = Vec::new();

    for i in 0..10 {
        let k = build_db_key(&format!("test_key{:?}", i));
        let v = build_db_val(&format!("test_value{:?}", i));
        let item = create_tabkv(
            Atom::from("testdb"),
            Atom::from("test_table_mgr_1"),
            k.clone(),
            0,
            Some(v.clone()),
        );

        arr.push(item);
    }

    for i in 0..10 {
        let k = build_db_key(&format!("test_key{:?}", i));
        let v = build_db_val(&format!("test_value{:?}", i));
        let item = create_tabkv(
            Atom::from("testdb"),
            Atom::from("test_table_mgr_2"),
            k.clone(),
            0,
            Some(v.clone()),
        );

        arr2.push(item);
    }

    t1.modify(
        arr.clone(),
        None,
        false,
        Arc::new(move |m| {
            assert!(m.is_ok());

            match t2.prepare(Arc::new(move |p| {
                unreachable!();
            })) {
                Some(p) => {
                    match t3.commit(Arc::new(|c| {
                        assert!(c.is_ok());
                    })) {
                        Some(c) => println!("commit {:?}", c),
                        None => println!("commit none arm"),
                    }
                }
                None => println!("prepare none arm"),
            }
        }),
    );
    thread::sleep_ms(2000);

    println!("-------------------------------------------------- ");

    let tt1 = mgr.transaction(true);
    let tt2 = tt1.clone();

    let ttt1 = mgr.transaction(true);
    let ttt2 = ttt1.clone();

    tt1.modify(
        arr.clone(),
        None,
        true,
        Arc::new(|q| {
            assert!(q.is_ok());
            println!("modify ====================== : {:?}", q);
        }),
    );
    thread::sleep_ms(2000);

    tt1.modify(
        arr.clone(),
        None,
        true,
        Arc::new(|q| {
            assert!(q.is_ok());
            println!("modify ====================== : {:?}", q);
        }),
    );

    thread::sleep_ms(2000);

    tt1.prepare(Arc::new(|p| {}));
    tt1.commit(Arc::new(|c| {}));

    thread::sleep_ms(2000);

    ttt1.modify(
        arr.clone(),
        None,
        true,
        Arc::new(|q| {
            assert!(q.is_ok());
            println!("modify ********************* : {:?}", q);
        }),
    );
    thread::sleep_ms(2000);

    ttt1.prepare(Arc::new(|p| {}));
    ttt1.commit(Arc::new(|c| {}));

    thread::sleep_ms(2000);
}
