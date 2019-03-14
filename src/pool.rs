/*
1. 只读事务
    a. 记录本次guid对哪些表进行了读操作，提交时通过该表对应的 dbi 映射到一个线程上提交，这样就不会造成死锁
    b. 一个表上有多个迭代器如何处理？ 如何保存迭代器状态？(LmdbItemsIter里面加字段保存状态)

2. 读写事务
    在一个事务里面先读后写，读事务每次都自动提交，所以可以只关心写事务的提交。写事务单独一个线程处理，用 unbounded channel
    保持每个事务的顺序


3. 工作线程池
    每个工作线程有一个unbounded channel 接收任务

4. 元信息事务
    需要做特殊处理，标识是否是一个元信息修改

5. 关于回滚
*/
use crossbeam_channel::{bounded, unbounded, Sender};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::thread;
use std::boxed::FnBox;

use lmdb::{Cursor, Database, DatabaseFlags, Environment, Error, Transaction, WriteFlags};

use worker::impls::cast_store_task;
use worker::task::TaskType;

const MDB_SET_KEY: u32 = 16;
const MDB_PREV: u32 = 12;
const MDB_NEXT: u32 = 8;
const MDB_FIRST: u32 = 0;
const MDB_LAST: u32 = 6;

use pi_db::db::{Bin, NextResult, TabKV, TxCallback, TxQueryCallback};

use atom::Atom;

pub enum ReaderMsg {
    Query(Arc<Vec<TabKV>>, TxQueryCallback),
    CreateItemIter(bool, Atom, Option<Bin>, Sender<Option<Bin>>),
    NextItem(
        bool,
        Atom,
        Option<Bin>,
        Arc<Fn(NextResult<(Bin, Bin)>)>,
        Sender<Option<Bin>>,
    ),
    Commit(TxCallback),
    Rollback(TxCallback),
}

unsafe impl Send for ReaderMsg {}

pub enum WriterMsg {
    Modify(TxCallback),
    Commit(Arc<Vec<TabKV>>, bool, TxCallback),
    Rollback(TxCallback),
}

unsafe impl Send for WriterMsg {}

pub struct LmdbService {
    env: Option<Arc<Environment>>,
    // how many threads to serve db read, only 1 writer thread
    readers_count: usize,
    readers: Vec<Sender<ReaderMsg>>,
    writer: Option<Sender<WriterMsg>>,
}

impl LmdbService {
    pub fn new(readers_count: usize) -> LmdbService {
        Self {
            env: None,
            readers_count,
            readers: vec![],
            writer: None,
        }
    }

    pub fn set_env(&mut self, env: Arc<Environment>) {
        self.env = Some(env);
    }

    pub fn create_tab(&mut self, tab: &Atom) {
        println!("create tab: {:?}", tab);
        OPENED_TABLES
            .write()
            .unwrap()
            .entry(tab.get_hash())
            .or_insert_with(|| {
                self.env
                    .as_ref()
                    .unwrap()
                    .create_db(Some(tab.as_str()), DatabaseFlags::empty())
                    .expect("Fatal error: open table failed")
            });
    }

    pub fn start(&mut self) {
        self.spawn_readers();
        self.spawn_writer();
    }

    pub fn ro_sender(&self, tab: &Atom) -> Option<Sender<ReaderMsg>> {
        Some(self.readers[(tab.get_hash() as usize) % self.readers_count].clone())
    }

    pub fn rw_sender(&self) -> Option<Sender<WriterMsg>> {
        self.writer.clone()
    }

    fn spawn_readers(&mut self) {
        (0..self.readers_count).for_each(|i| {
            let env = self.env.clone();
            let (tx, rx) = unbounded();

            let _ = thread::Builder::new().name("Lmdb Reader".to_string()).spawn(move || loop {
                match rx.recv() {
                    Ok(ReaderMsg::Commit(cb)) => {
                        let t = Box::new(move |_| {
                            cb(Ok(()));
                        });
                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader commit"));
                    }
                    Ok(ReaderMsg::Query(queries, cb)) => {
                        let mut qr = vec![];
                        let mut query_error = false;
                        let txn = env
                            .as_ref()
                            .unwrap()
                            .begin_ro_txn()
                            .expect("Fatal error: Lmdb can't create ro txn");
                        for q in queries.iter() {
                            let db = OPENED_TABLES
                                .read()
                                .unwrap()
                                .get(&q.tab.get_hash())
                                .unwrap()
                                .clone();
                            match txn.get(db, q.key.as_ref()) {
                                Ok(v) => {
                                    qr.push(TabKV {
                                        ware: q.ware.clone(),
                                        tab: q.tab.clone(),
                                        key: q.key.clone(),
                                        index: q.index,
                                        value: Some(Arc::new(Vec::from(v))),
                                    });
                                }

                                Err(Error::NotFound) => {
                                    qr.push(TabKV {
                                        ware: q.ware.clone(),
                                        tab: q.tab.clone(),
                                        key: q.key.clone(),
                                        index: q.index,
                                        value: None,
                                    });
                                }

                                Err(_) => {
                                    query_error = true;
                                    break;
                                }
                            }
                        }

                        if query_error {
                            let t = Box::new(move |_| {
                                cb(Err(format!("lmdb query internal error")));
                            });
                            cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader query error"));
                            println!("lmdb query error ===================");
                        } else {
                            let t = Box::new(move |_| {
                                cb(Ok(qr));
                            });
                            cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader query ok"));
                            println!("lmdb query success ==================");
                        }

                        let _ = txn.commit();
                    }
                    Ok(ReaderMsg::CreateItemIter(descending, tab, start_key, sndr)) => {
                        let txn = env
                            .as_ref()
                            .unwrap()
                            .begin_ro_txn()
                            .expect("Fatal error: Lmdb can't create ro txn");
                        let db = OPENED_TABLES
                            .read()
                            .unwrap()
                            .get(&tab.get_hash())
                            .unwrap()
                            .clone();
                        let cursor = txn
                            .open_ro_cursor(db)
                            .expect(&format!("Fatal error: open cursor for db: {:?} failed", db));

                        match (descending, start_key) {
                            (true, None) => match cursor.get(None, None, MDB_FIRST) {
                                Ok(val) => {
                                    let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                }
                                Err(Error::NotFound) => {
                                    let _ = sndr.send(None);
                                }

                                Err(_) => {}
                            },
                            (true, Some(sk)) => {
                                match cursor.get(Some(sk.as_ref()), None, MDB_SET_KEY) {
                                    Ok(val) => {
                                        let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                    }
                                    Err(Error::NotFound) => {
                                        let _ = sndr.send(None);
                                    }
                                    Err(_) => {}
                                }
                            }
                            (false, Some(sk)) => {
                                match cursor.get(Some(sk.as_ref()), None, MDB_SET_KEY) {
                                    Ok(val) => {
                                        let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                    }
                                    Err(Error::NotFound) => {
                                        let _ = sndr.send(None);
                                    }
                                    Err(_) => {}
                                }
                            }
                            (false, None) => match cursor.get(None, None, MDB_LAST) {
                                Ok(val) => {
                                    let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                }
                                Err(Error::NotFound) => {
                                    let _ = sndr.send(None);
                                }
                                Err(_) => {}
                            },
                        }
                    }
                    Ok(ReaderMsg::NextItem(descending, tab, cur_key, cb, sndr)) => {
                        // println!("pool next item descending: {:?}, cur_key: {:?}", descending, cur_key);
                        let txn = env
                            .as_ref()
                            .unwrap()
                            .begin_ro_txn()
                            .expect("Fatal error: Lmdb can't create ro txn");
                        let db = OPENED_TABLES
                            .read()
                            .unwrap()
                            .get(&tab.get_hash())
                            .unwrap()
                            .clone();
                        let cursor = txn
                            .open_ro_cursor(db)
                            .expect(&format!("Fatal error: open cursor for db: {:?} failed", db));

                        // println!("next item create txn and cursor");
                        match (descending, cur_key) {
                            (true, Some(ck)) => {
                                let cb1 = cb.clone();
                                let ck1 = ck.clone();
                                let cb2 = cb.clone();
                                let ck2 = ck.clone();
                                match cursor.get(Some(ck.as_ref()), None, MDB_SET_KEY) {
                                    Ok(val) => {
                                        let v = val.1.to_vec();
                                        let t = Box::new(move |_: Option<isize>| {
                                            cb1(Ok(Some((ck1, Arc::new(v)))));
                                        });
                                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader get next item"));
                                    }
                                    Err(Error::NotFound) => {}
                                    Err(_) => {}
                                }

                                // get next key
                                match cursor.get(Some(ck.as_ref()), None, MDB_NEXT) {
                                    Ok(val) => {
                                        let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                    }

                                    Err(Error::NotFound) => {
                                        let _ = sndr.send(None);
                                    }

                                    Err(e) => {
                                        let t = Box::new(move |_: Option<isize>| {
                                            cb(Err(format!("lmdb iter internal error: {:?}", e)));
                                        });
                                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader get next item error")); 
                                    }
                                }
                            }
                            (false, Some(ck)) => {
                                let cb1 = cb.clone();
                                let ck1 = ck.clone();
                                let cb2 = cb.clone();
                                match cursor.get(Some(ck.as_ref()), None, MDB_SET_KEY) {
                                    Ok(val) => {
                                        let v = val.1.to_vec();
                                        let t = Box::new(move |_: Option<isize>| {
                                            cb1(Ok(Some((ck1, Arc::new(v)))));
                                        });
                                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader get next item"));
                                    }
                                    Err(Error::NotFound) => {}
                                    Err(_) => {}
                                }

                                // get next key
                                match cursor.get(Some(ck.as_ref()), None, MDB_PREV) {
                                    Ok(val) => {
                                        let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                    }

                                    Err(Error::NotFound) => {
                                        let _ = sndr.send(None);
                                    }

                                    Err(e) => {
                                        let t = Box::new(move |_: Option<isize>| {
                                            cb2(Err(format!("Lmdb reader lmdb next item error: {:?}", e)));
                                        });
                                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader query error"));
                                    }
                                }
                            }

                            _ => (),
                        }
                    }
                    Ok(ReaderMsg::Rollback(cb)) => cb(Ok(())),
                    Err(_) => (),
                }
            });
            self.readers.push(tx);
        })
    }

    fn spawn_writer(&mut self) {
        let env = self.env.clone();
        let (tx, rx) = unbounded();

        let _ = thread::Builder::new().name("Lmdb writer".to_string()).spawn(move || loop {
            match rx.recv() {
                Ok(WriterMsg::Modify(cb)) => {
                    let t = Box::new(move |_: Option<isize>| {
                        cb(Ok(()));
                    });
                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer modify"));
                }
                Ok(WriterMsg::Commit(modifies, meta, cb)) => {
                    // if it is a meta txn commit it directly
                    if meta {
                        let db = OPENED_TABLES
                            .read()
                            .unwrap()
                            .get(&Atom::from("_$sinfo").get_hash())
                            .unwrap()
                            .clone();

                        let mut txn = env
                            .as_ref()
                            .unwrap()
                            .begin_rw_txn()
                            .expect("Fatal error: failed to begin rw txn");

                        for m in modifies.iter() {
                            if m.tab == Atom::from("") { continue } //skip placeholder item
                            match txn.put(
                                db,
                                m.key.as_ref(),
                                m.value.clone().unwrap().as_ref(),
                                WriteFlags::empty(),
                            ) {
                                Ok(_) => {}
                                Err(_) => cb(Err("meta txn modify error".to_owned())),
                            }
                        }

                        match txn.commit() {
                            Ok(_) => {
                                let t = Box::new(move |_: Option<isize>| {
                                    cb(Ok(()));
                                });
                                cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer meta txn commit"));
                            }
                            Err(_) => {
                                let t = Box::new(move |_: Option<isize>| {
                                    cb(Err("meta txn commit erorr".to_owned()));
                                });
                                cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer meta txn commit error"));
                            }
                        }
                        // println!("meta txn finally commited =========== ");
                    } else {
                        let mut txn = env
                            .as_ref()
                            .unwrap()
                            .begin_rw_txn()
                            .expect("Fatal error: failed to begin rw txn");

                        for m in modifies.iter() {
                            // this db should be opened previously, or this is a bug
                            let db = OPENED_TABLES
                                .read()
                                .unwrap()
                                .get(&m.tab.get_hash())
                                .unwrap()
                                .clone();

                            // value is some, insert data
                            if m.value.is_some() {
                                match txn.put(
                                    db,
                                    m.key.as_ref(),
                                    m.value.clone().unwrap().as_ref(),
                                    WriteFlags::empty(),
                                ) {
                                    Ok(_) => {}
                                    Err(e) => cb(Err(format!(
                                        "lmdb internal insert data error: {:?}",
                                        e.to_string()
                                    ))),
                                }
                            // value is None, delete data
                            } else {
                                match txn.del(db, m.key.as_ref(), None) {
                                    Ok(_) => {}
                                    Err(Error::NotFound) => {
                                        // TODO: when not found?
                                    }
                                    Err(e) => {
                                        cb(Err(format!("delete data error: {:?}", e.to_string())))
                                    }
                                }
                            }
                        }

                        match txn.commit() {
                            Ok(_) => {
                                let t = Box::new(move |_: Option<isize>| {
                                    cb(Ok(()));
                                });
                                cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer normal txn commit"));
                            }
                            Err(e) => {
                                let t = Box::new(move |_: Option<isize>| {
                                    cb(Err(format!("commit failed with error: {:?}", e.to_string())));
                                });
                                cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer normal txn commit error"));
                            }
                        }
                        // println!("normal txn finally committed ==========");
                    }
                }
                Ok(WriterMsg::Rollback(cb)) => {
                    let t = Box::new(move |_: Option<isize>| {
                        cb(Ok(()));
                    });
                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer rollback txn commit"));
                }
                Err(_) => (),
            }
        });
        self.writer = Some(tx);
    }
}

lazy_static! {
    // all opened dbs in this env
    static ref OPENED_TABLES: Arc<RwLock<HashMap<u64, Database>>> = Arc::new(RwLock::new(HashMap::new()));
}
