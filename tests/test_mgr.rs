extern crate pi_store;
extern crate pi_lib;
extern crate pi_db;
extern crate pi_vm;

use pi_store::db::{SysDb, FDBTab, FDB, STORE_TASK_POOL};
use pi_lib::bon::{BonBuffer, Encode, Decode};
use pi_lib::atom::Atom;
use std::sync::Arc;
use pi_db::db::{UsizeResult, Cursor, DBResult, TabKV, TxCallback, TxIterCallback, TxQueryCallback, TxState, MetaTxn, Tab, TabTxn, TabBuilder};
use pi_db::mgr::Mgr;
use std::collections::HashMap;
use pi_lib::sinfo::StructInfo;
use pi_lib::guid::{Guid, GuidGen};
use std::thread;
use std::time::Duration;
use pi_vm::worker_pool::WorkerPool;

#[derive(Debug)]
struct Player{
	name: String,
	id: u32,
}

impl Encode for Player{
	fn encode(&self, bb: &mut BonBuffer){
		self.name.encode(bb);
		self.id.encode(bb);
	}
}

impl Decode for Player{
	fn decode(bb: &mut BonBuffer) -> Self{
		Player{
			name: String::decode(bb),
			id: u32::decode(bb),
		}
	}
}

#[test]
fn test_file_db_mgr(){
    let mut worker_pool = Box::new(WorkerPool::new(10, 1024 * 1024, 10000));
    worker_pool.run(STORE_TASK_POOL.clone());
	let mgr = Mgr::new(GuidGen::new(1,1));
	mgr.register_builder(&Atom::from("file"), Arc::new(SysDb::new(&Atom::from("file"))));
	let mgr = Arc::new(mgr);

    //  插入元信息
	let tr = mgr.transaction(true, 1000);
	let tr1 = tr.clone();
	let mut sinfo = StructInfo::new(Atom::from("Player"), 555555555555);
	let mut m = HashMap::new();
	m.insert(Atom::from("class"), Atom::from("file"));
	sinfo.notes = Some(m);
	let alter_back = Arc::new(move|r: DBResult<usize>|{
        match r {
            Ok(_) => (),
            Err(v) => {println!("alter_fail:{}", v);},
        };

        let prepare_back = Arc::new(|r|{
            match r {
                Ok(_) => (),
                Err(v) => {println!("preparer_alter_fail:{}", v);},
            };
        });
        let r = tr1.prepare(prepare_back.clone());
        if r.is_some(){
            prepare_back(r.unwrap());
        }
        thread::sleep(Duration::from_millis(300));

        let commit_back = Arc::new(|r|{
            match r {
                Ok(_) => (),
                Err(v) => {println!("commit_alter_fail:{}", v);},
            };
        });
        let r = tr1.commit(commit_back.clone());
        if r.is_some(){
            commit_back(r.unwrap());
        }
    });
	let r = tr.alter(&Atom::from("Player"), Some(Arc::new(sinfo)), alter_back.clone());
	if r.is_some(){
		alter_back(r.unwrap());
	}

    thread::sleep(Duration::from_millis(1000));

    //写数据
    let tr = mgr.transaction(true, 1000);
    let tr1 = tr.clone();
    let p = Player{
        name: String::from("chuanyan"),
        id:5
    };
    let mut bonbuf = BonBuffer::new();
    let bon = p.encode(&mut bonbuf);
    let buf = bonbuf.unwrap();

    let mut arr = Vec::new();
    let t1 = TabKV{
        tab: Atom::from("Player"),
        key: vec![5u8],
        index: 0,
        value: Some(Arc::new(buf)),
    };
    arr.push(t1);
    let write_back = Arc::new(move|r|{
        match r {
            Ok(_) => (),
            Err(v) => {println!("write_fail:{}", v);},
        };

        let prepare_back = Arc::new(|r|{
            match r {
                Ok(_) => (),
                Err(v) => {println!("prepare_write_fail:{}", v);},
            };
        });
        let r = tr1.prepare(prepare_back.clone());
        if r.is_some(){
            prepare_back(r.unwrap());
        }
        thread::sleep(Duration::from_millis(300));

        let commit_back = Arc::new(|r|{
            match r {
                Ok(_) => (),
                Err(v) => {println!("commit_write_fail:{}", v);},
            };
        });
        let r = tr1.commit(commit_back.clone());
        if r.is_some(){
            commit_back(r.unwrap());
        }
    });
    let r = tr.modify(arr, Some(100), false, write_back.clone());
    if r.is_some(){
        write_back(r.unwrap());
    }

    thread::sleep(Duration::from_millis(1500));

    let tr = mgr.transaction(false, 1000);
    let tr1 = tr.clone();
    let mut arr = Vec::new();
    let t1 = TabKV{
        tab: Atom::from("Player"),
        key: vec![5u8],
        index: 0,
        value: None,
    };
    arr.push(t1);
    let read_back = Arc::new(move|r: DBResult<Vec<TabKV>>|{
        match r {
            Ok(mut v) => {
                for elem in v.iter_mut(){
                    match elem.value {
                        Some(ref mut v) => {
                            let mut buf = BonBuffer::with_bytes(Arc::make_mut(v).clone(), None, None);
                            let p = Player::decode(&mut buf);
                            assert_eq!(p.name, "chuanyan");
                            assert_eq!(p.id, 5);
                        },
                        None => (),
                    }
                }
            },
            Err(v) => println!("read_fail:{}", v),
        }

        let prepare_back = Arc::new(|r|{
            match r {
                Ok(_) => (),
                Err(v) => {println!("prepare_read_fail:{}", v);},
            };
        });
        let r = tr1.prepare(prepare_back.clone());
        if r.is_some(){
            prepare_back(r.unwrap());
        }
        thread::sleep(Duration::from_millis(300));

        let commit_back = Arc::new(|r|{
            match r {
                Ok(_) => (),
                Err(v) => {println!("commit_read_fail:{}", v);},
            };
        });
        let r = tr1.commit(commit_back.clone());
        if r.is_some(){
            commit_back(r.unwrap());
        }
        //println!("succsess:{}", arr.len());
    });

    let r = tr.query(arr, Some(100), true, read_back.clone());
    if r.is_some(){
        read_back(r.unwrap());
    }

    thread::sleep(Duration::from_millis(1000));
}
