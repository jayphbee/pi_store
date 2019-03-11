extern crate lmdb;
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
                Some(p) => println!("prepare111: {:?}", p),
                None => println!("None arm"),
            }
        }),
    );

    thread::sleep_ms(2000);

    tr.alter(
        &Atom::from("testdb"),
        &Atom::from("test_table_mgr_2"),
        Some(Arc::new(TabMeta {
            k: EnumType::Str,
            v: EnumType::Str,
        })),
        Arc::new(move |a| {
            println!("what's wrong here ++++++++++++");
            assert!(a.is_ok());
            match tr2.prepare(Arc::new(move |p| {
                assert!(p.is_ok());
                println!("prepare callback !!!!!!!! ");
            })) {
                Some(p) => println!("prepare222: {:?}", p),
                None => println!("None arm"),
            }
        }),
    );

    thread::sleep_ms(1000);

    tr.commit(Arc::new(|c| {
        assert!(c.is_ok());
        println!("commit ok -----------------");
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

        let item2 = create_tabkv(
            Atom::from("testdb"),
            Atom::from("test_table_mgr_2"),
            k.clone(),
            0,
            Some(v.clone()),
        );

        arr.push(item);
        arr.push(item2);
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
    println!("modfiy 1");

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
    println!("modfiy 2");


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
    println!("modfiy 3");


    ttt1.prepare(Arc::new(|p| {}));
    ttt1.commit(Arc::new(|c| {}));

    thread::sleep_ms(2000);
}
