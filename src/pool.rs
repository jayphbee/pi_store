use crossbeam_channel::{unbounded, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::RwLock;
use std::thread;

use lmdb::{Cursor, Database, DatabaseFlags, Environment, Error, Transaction, WriteFlags, RwTransaction};

use worker::impls::cast_store_task;
use worker::task::TaskType;

const MDB_SET_KEY: u32 = 16;
const MDB_SET_RANGE: u32 = 17;
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
    Query(u64, Arc<Vec<TabKV>>, TxQueryCallback),
    CreateItemIter(u64, bool, Atom, Option<Bin>, Sender<Option<Bin>>),
    NextItem(
        u64,
        bool,
        Atom,
        Option<Bin>,
        Arc<Fn(NextResult<(Bin, Bin)>)>,
        Sender<Option<Bin>>,
    ),
    Modify(u64, TxCallback),
    Commit(Sender<()>, u64, Arc<Vec<TabKV>>, TxCallback),
    Rollback(u64, TxCallback),
    InProgressTx(u64, Sender<u64>),
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
        info!("create tab: {:?}", tab);
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

            let _ = thread::Builder::new().name(format!("Lmdb Reader {:?}", i)).spawn(move ||
            loop {
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
                            let db = get_db(q.tab.get_hash());
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
                            warn!("queries error: {:?}", queries);
                        } else {
                            info!("lmdb query success: {:?}", qr);
                            let t = Box::new(move |_| {
                                cb(Ok(qr));
                            });
                            cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader query ok"));
                        }

                        match txn.commit() {
                            Ok(_) => {}
                            Err(e) => panic!("query txn commit error: {:?}", e.to_string()),
                        }
                    }
                    Ok(ReaderMsg::CreateItemIter(descending, tab, start_key, sndr)) => {
                        let txn = env
                            .as_ref()
                            .unwrap()
                            .begin_ro_txn()
                            .expect("Fatal error: Lmdb can't create ro txn");
                        let db = get_db(tab.get_hash());
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
                            // MDB_SET_RANGE 会找到第一个大于或者等于 sk 的 key
                            (true, Some(sk)) => {
                                match cursor.get(Some(sk.as_ref()), None, MDB_SET_RANGE) {
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
                                match cursor.get(Some(sk.as_ref()), None, MDB_SET_RANGE) {
                                    Ok(val) => {
                                        let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                    }
                                    Err(Error::NotFound) => {
                                        // 降序迭代起始 key 超过最大 key 则定位到表中最后一个元素
                                        match cursor.get(None, None, MDB_LAST) {
                                            Ok(val) => {
                                                let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                            }
                                            Err(Error::NotFound) => {
                                                let _ = sndr.send(None);
                                            }
                                            Err(_) => {}
                                        }
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

                        drop(cursor);
                        match txn.commit() {
                            Ok(_) => {}
                            Err(e) => panic!("create iter txn commit error: {:?}", e.to_string()),
                        }
                    }
                    Ok(ReaderMsg::NextItem(descending, tab, cur_key, cb, sndr)) => {
                        let txn = env
                            .as_ref()
                            .unwrap()
                            .begin_ro_txn()
                            .expect("Fatal error: Lmdb can't create ro txn");
                        let db = get_db(tab.get_hash());
                        let cursor = txn
                            .open_ro_cursor(db)
                            .expect(&format!("Fatal error: open cursor for db: {:?} failed", db));

                        match (descending, cur_key) {
                            (true, Some(ck)) => {
                                let cb1 = cb.clone();
                                let ck1 = ck.clone();
                                match cursor.get(Some(ck.as_ref()), None, MDB_SET_KEY) {
                                    Ok(val) => {
                                        let v = val.1.to_vec();
                                        info!("iter next item descendin key: {:?}, value: {:?}", ck.clone(), v.clone());
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
                                        info!("iter next key descending: item: {:?}", val.clone());
                                        let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                    }

                                    Err(Error::NotFound) => {
                                        info!("iter next key descending: NotFound");
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
                                        info!("iter next item ascending key: {:?}, value: {:?}", ck.clone(), v.clone());
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
                                        info!("iter next item ascending item: {:?}", val);
                                        let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                    }

                                    Err(Error::NotFound) => {
                                        info!("iter next item ascending item: NotFound");
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

                        drop(cursor);
                        match txn.commit() {
                            Ok(_) => {},
                            Err(e) => panic!("Next item txn commit error: {:?}", e.to_string()),
                        }
                    }
                    Ok(ReaderMsg::Rollback(cb)) => {
                        let t = Box::new(move |_: Option<isize>| {
                            cb(Ok(()));
                        });
                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb reader rollback error"));
                    }
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
            let mut rw_txn: Option<RwTransaction> = None;
            let mut in_progress_tx = 0;

            match rx.recv() {
                Ok(WriterMsg::InProgressTx(txid, sndr)) => {
                    info!("******* InProgressTx txid: {:?} ********", in_progress_tx);

                    if in_progress_tx == 0 {
                        let _ = sndr.send(0);
                        in_progress_tx = txid;
                    } else {
                        let _ = sndr.send(in_progress_tx);
                    }
                }
                Ok(WriterMsg::Query(txid, queries, cb)) => {
                    let mut qr = vec![];
                    let mut query_error = false;
                    if rw_txn.is_none() {
                        rw_txn = Some(env
                        .as_ref()
                        .unwrap()
                        .begin_rw_txn()
                        .expect("Fatal error: failed to begin rw txn"));
                    }

                    for q in queries.iter() {
                        let db = get_db(q.tab.get_hash());
                        match rw_txn.as_ref().unwrap().get(db, q.key.as_ref()) {
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
                            cb(Err(format!("lmdb rw query internal error")));
                        });
                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer query error"));
                        warn!("queries error: {:?}", queries);
                    } else {
                        info!("lmdb rw query success: {:?}", qr);
                        let t = Box::new(move |_| {
                            cb(Ok(qr));
                        });
                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer query ok"));
                    }
                }

                Ok(WriterMsg::CreateItemIter(txid, descending, tab, start_key, sndr)) => {
                    if rw_txn.is_none() {
                        rw_txn = Some(env
                        .as_ref()
                        .unwrap()
                        .begin_rw_txn()
                        .expect("Fatal error: failed to begin rw txn"));
                    }

                    let db = get_db(tab.get_hash());
                    let cursor = rw_txn.as_mut().unwrap()
                        .open_rw_cursor(db)
                        .expect(&format!("Fatal error: open rw cursor for db: {:?} failed", db));

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
                        // MDB_SET_RANGE 会找到第一个大于或者等于 sk 的 key
                        (true, Some(sk)) => {
                            match cursor.get(Some(sk.as_ref()), None, MDB_SET_RANGE) {
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
                            match cursor.get(Some(sk.as_ref()), None, MDB_SET_RANGE) {
                                Ok(val) => {
                                    let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                }
                                Err(Error::NotFound) => {
                                    // 降序迭代起始 key 超过最大 key 则定位到表中最后一个元素
                                    match cursor.get(None, None, MDB_LAST) {
                                        Ok(val) => {
                                            let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                        }
                                        Err(Error::NotFound) => {
                                            let _ = sndr.send(None);
                                        }
                                        Err(_) => {}
                                    }
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

                    drop(cursor);
                }

                Ok(WriterMsg::NextItem(txid, descending, tab, cur_key, cb, sndr)) => {
                    if rw_txn.is_none() {
                        rw_txn = Some(env
                        .as_ref()
                        .unwrap()
                        .begin_rw_txn()
                        .expect("Fatal error: failed to begin rw txn"));
                    }
                    let db = get_db(tab.get_hash());
                    let cursor = rw_txn.as_mut().unwrap()
                        .open_rw_cursor(db)
                        .expect(&format!("Fatal error: open rw cursor for db: {:?} failed", db));

                    match (descending, cur_key) {
                        (true, Some(ck)) => {
                            let cb1 = cb.clone();
                            let ck1 = ck.clone();
                            match cursor.get(Some(ck.as_ref()), None, MDB_SET_KEY) {
                                Ok(val) => {
                                    let v = val.1.to_vec();
                                    info!("iter next item descendin key: {:?}, value: {:?}", ck.clone(), v.clone());
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
                                    info!("rw iter next key descending: item: {:?}", val.clone());
                                    let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                }

                                Err(Error::NotFound) => {
                                    info!("rw iter next key descending: NotFound");
                                    let _ = sndr.send(None);
                                }

                                Err(e) => {
                                    let t = Box::new(move |_: Option<isize>| {
                                        cb(Err(format!("lmdb rw iter internal error: {:?}", e)));
                                    });
                                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer get next item error")); 
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
                                    info!("rw iter next item ascending key: {:?}, value: {:?}", ck.clone(), v.clone());
                                    let t = Box::new(move |_: Option<isize>| {
                                        cb1(Ok(Some((ck1, Arc::new(v)))));
                                    });
                                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer get next item"));
                                }
                                Err(Error::NotFound) => {}
                                Err(_) => {}
                            }

                            // get next key
                            match cursor.get(Some(ck.as_ref()), None, MDB_PREV) {
                                Ok(val) => {
                                    info!("rw iter next item ascending item: {:?}", val);
                                    let _ = sndr.send(Some(Arc::new(val.0.unwrap().to_vec())));
                                }

                                Err(Error::NotFound) => {
                                    info!("rw iter next item ascending item: NotFound");
                                    let _ = sndr.send(None);
                                }

                                Err(e) => {
                                    let t = Box::new(move |_: Option<isize>| {
                                        cb2(Err(format!("Lmdb writer lmdb next item error: {:?}", e)));
                                    });
                                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer iter error"));
                                }
                            }
                        }

                        _ => (),
                    }

                    drop(cursor);
                }

                Ok(WriterMsg::Modify(txid, cb)) => {
                    if rw_txn.is_none() {
                        rw_txn = Some(env
                        .as_ref()
                        .unwrap()
                        .begin_rw_txn()
                        .expect("Fatal error: failed to begin rw txn"));
                    }

                    let t = Box::new(move |_: Option<isize>| {
                        cb(Ok(()));
                    });
                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer modify"));
                }
                Ok(WriterMsg::Commit(s, txid, modifies, cb)) => {
                    if rw_txn.is_none() {
                        rw_txn = Some(env
                        .as_ref()
                        .unwrap()
                        .begin_rw_txn()
                        .expect("Fatal error: failed to begin rw txn"));
                    }

                    let mut modify_error = false;

                    for m in modifies.iter() {
                        let db = get_db(m.tab.get_hash());
                        // value is some, insert data
                        if m.value.is_some() {
                            match rw_txn.as_mut().unwrap().put(
                                db,
                                m.key.as_ref(),
                                m.value.clone().unwrap().as_ref(),
                                WriteFlags::empty(),
                            ) {
                                Ok(_) => {}
                                Err(_) => modify_error = true,
                            }
                        // value is None, delete data
                        } else {
                            match rw_txn.as_mut().unwrap().del(db, m.key.as_ref(), None) {
                                Ok(_) => {}
                                Err(Error::NotFound) => {
                                    // TODO: when not found?
                                }
                                Err(_) => modify_error = true,
                            }
                        }
                    }
                    let cb1 = cb.clone();
                    if modify_error {
                        let t = Box::new(move |_: Option<isize>| {
                            cb(Err("modify error".to_string()));
                        });
                        cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer error"));
                        warn!("lmdb modify error");
                    }

                    match rw_txn.take().unwrap().commit() {
                        Ok(_) => {
                            let t = Box::new(move |_: Option<isize>| {
                                cb1(Ok(()));
                            });
                            cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer normal txn commit"));
                            // cb1(Ok(()))
                        }
                        Err(e) => {
                            let t = Box::new(move |_: Option<isize>| {
                                cb1(Err(format!("commit failed with error: {:?}", e.to_string())));
                            });
                            cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer normal txn commit error"));
                            // cb1(Err(format!("commit failed with error: {:?}", e.to_string())));
                        }
                    }
                    info!("==== in_progress_tx {:?} set to 0 self txid {:?} =======", in_progress_tx, txid);

                    in_progress_tx = 0;
                    let _ = s.send(());

                }
                Ok(WriterMsg::Rollback(txid, cb)) => {
                    let t = Box::new(move |_: Option<isize>| {
                        cb(Ok(()));
                    });
                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("Lmdb writer rollback txn commit"));
                    in_progress_tx = 0;
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

fn get_db(tab: u64) -> Database {
    OPENED_TABLES
        .read()
        .unwrap()
        .get(&tab)
        .unwrap()
        .clone()
}
