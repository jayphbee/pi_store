use crossbeam_channel::{bounded, Sender};
use std::slice::from_raw_parts;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use lmdb::{
    mdb_set_compare, Cursor, Database, DatabaseFlags, Environment, Error, Iter as LmdbIter,
    MDB_cmp_func, MDB_val, RoCursor, RwTransaction, Transaction, WriteFlags,
};

const MDB_SET: u32 = 15;
const MDB_PREV: u32 = 12;
const MDB_NEXT: u32 = 8;
const MDB_FIRST: u32 = 0;
const MDB_LAST: u32 = 6;

use pi_db::db::{Bin, NextResult, SResult, TabKV, TxCallback, TxQueryCallback};

use atom::Atom;
use bon::ReadBuffer;

pub enum LmdbMessage {
    CreateDb(String, Sender<()>),
    Query(Arc<Vec<TabKV>>, TxQueryCallback),
    NextItem(Arc<Fn(NextResult<(Bin, Bin)>)>),
    NextKey(Arc<Fn(NextResult<Bin>)>),
    CreateItemIter(bool, Option<Bin>, Sender<()>),
    CreateKeyIter(bool, Option<Bin>, Sender<()>),
    Modify(Arc<Vec<TabKV>>, TxCallback),
    Commit(TxCallback),
    Rollback(TxCallback),
    TableSize(Arc<Fn(SResult<usize>)>),
    NoOp(TxCallback),
    CleanUp,
}

unsafe impl Send for LmdbMessage {}

#[derive(Debug)]
pub struct ThreadPool {
    senders: Vec<Sender<LmdbMessage>>,
    total: usize,
    idle: usize,
}

pub enum NopMessage {
    Nop(TxCallback),
}

#[derive(Debug)]
pub struct NopPool {
    senders: Vec<Sender<NopMessage>>,
}

unsafe impl Send for NopMessage {}

impl NopPool {
    pub fn new() -> Self {
        Self {
            senders: Vec::new(),
        }
    }

    pub fn start_nop_pool(&mut self, cap: usize) {
        for _ in 0..cap {
            let (tx, rx) = bounded(1);

            thread::spawn(move || loop {
                match rx.recv() {
                    Ok(NopMessage::Nop(cb)) => cb(Ok(())),

                    Err(_) => {}
                }
            });

            self.senders.push(tx);
        }
    }

    pub fn push(&mut self, sender: Sender<NopMessage>) {
        self.senders.push(sender);
    }

    pub fn pop(&mut self) -> Option<Sender<NopMessage>> {
        self.senders.pop()
    }
}

impl ThreadPool {
    pub fn new() -> Self {
        ThreadPool {
            senders: Vec::new(),
            total: 0,
            idle: 0,
        }
    }
    pub fn start_pool(&mut self, cap: usize, env: Arc<Environment>) {
        for _ in 0..cap {
            let clone_env = env.clone();
            let (tx, rx) = bounded(1);

            thread::spawn(move || {
                let env = clone_env;
                let mut thread_local_txn: Option<RwTransaction> = None;
                let mut db: Option<Database> = None;
                let mut desc = false;
                let mut cur_iter_key: Option<Bin> = None;
                let mut iter_from_start_or_end = true;

                loop {
                    match rx.recv() {
                        Ok(LmdbMessage::CleanUp) => {
                            thread_local_txn = None;
                            db = None;
                            cur_iter_key = None;
                        }

                        Ok(LmdbMessage::NoOp(cb)) => cb(Ok(())),

                        Ok(LmdbMessage::CreateDb(db_name, tx)) => {
                            // db = match env.open_db(Some(&db_name.to_string())) {
                            //     Ok(db) => Some(db),
                            //     Err(_) => Some(
                            //         env.create_db(
                            //             Some(&db_name.to_string()),
                            //             DatabaseFlags::empty(),
                            //         )
                            //         .unwrap(),
                            //     ),
                            // };
                            db = Some(
                                env.create_db(Some(&db_name.to_string()), DatabaseFlags::empty())
                                    .unwrap(),
                            );

                            let _ = tx.send(());
                        }

                        Ok(LmdbMessage::Query(keys, cb)) => {
                            println!(
                                "enter thread pool query: keys = {:?}, len = {:?}",
                                keys,
                                keys.len()
                            );
                            let mut values = Vec::new();

                            if thread_local_txn.is_none() {
                                println!("before create rw txn..............");
                                thread_local_txn = env.begin_rw_txn().ok();
                                println!("after create rw txn");
                            }

                            for kv in keys.iter() {
                                let db_name = kv.tab.clone();

                                let db = env.open_db(Some(&db_name.to_string())).ok();
                                
                                println!("query get db name: dbi = {:?}, kv: {:?}", db, kv.key.as_ref());

                                match thread_local_txn
                                    .as_ref()
                                    .unwrap()
                                    .get(db.clone().unwrap(), kv.key.as_ref())
                                {
                                    Ok(v) => {
                                        println!("query get value {:?}", v);
                                        values.push(TabKV {
                                            ware: kv.ware.clone(),
                                            tab: kv.tab.clone(),
                                            key: kv.key.clone(),
                                            index: kv.index,
                                            value: Some(Arc::new(Vec::from(v))),
                                        });
                                        cb(Ok(values.clone()));

                                    }
                                    Err(Error::NotFound) => {
                                        println!("query error not found");
                                        values.push(TabKV {
                                            ware: kv.ware.clone(),
                                            tab: kv.tab.clone(),
                                            key: kv.key.clone(),
                                            index: kv.index,
                                            value: None,
                                        });
                                        cb(Ok(values.clone()));                                        
                                    }
                                    Err(e) => {
                                        println!("query interal error: {:?}", e);
                                        cb(Err(format!(
                                            "lmdb internal error: {:?}",
                                            e.to_string()
                                        )));
                                        break;
                                    }
                                }
                            }
                            // cb(Ok(values));
                        }

                        Ok(LmdbMessage::CreateItemIter(descending, key, tx)) => {
                            desc = descending;
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            iter_from_start_or_end = if key.is_none() {
                                true
                            } else {
                                cur_iter_key = key;
                                false
                            };

                            let _ = tx.send(());
                        }

                        Ok(LmdbMessage::NextItem(cb)) => {
                            let cursor = thread_local_txn
                                .as_ref()
                                .unwrap()
                                .open_ro_cursor(db.clone().unwrap())
                                .unwrap();

                            if cur_iter_key.is_some() {
                                match cursor.get(
                                    Some(cur_iter_key.clone().unwrap().as_ref()),
                                    None,
                                    MDB_SET,
                                ) {
                                    Ok(_) => {}
                                    Err(Error::NotFound) => {}
                                    Err(e) => {
                                        cb(Err(format!("MDB_SET failed: {:?}", e.to_string())))
                                    }
                                }
                            }

                            if iter_from_start_or_end {
                                if desc {
                                    // for the first iteration, cur_iter_key is none
                                    if cur_iter_key.is_none() {
                                        match cursor.get(None, None, MDB_NEXT) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some((
                                                    Arc::new(val.0.unwrap().to_vec()),
                                                    Arc::new(val.1.to_vec()),
                                                ))));
                                            }

                                            Err(Error::NotFound) => cb(Ok(None)),

                                            Err(e) => cb(Err(format!(
                                                "cursor get error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    } else {
                                        match cursor.get(None, None, MDB_NEXT) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some((
                                                    Arc::new(val.0.unwrap().to_vec()),
                                                    Arc::new(val.1.to_vec()),
                                                ))));
                                            }

                                            Err(Error::NotFound) => cb(Ok(None)),

                                            Err(e) => cb(Err(format!(
                                                "cursor get error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    }
                                } else {
                                    if cur_iter_key.is_none() {
                                        match cursor.get(None, None, MDB_PREV) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some((
                                                    Arc::new(val.0.unwrap().to_vec()),
                                                    Arc::new(val.1.to_vec()),
                                                ))));
                                            }

                                            Err(_) => cb(Ok(None)),
                                        }
                                    } else {
                                        match cursor.get(None, None, MDB_PREV) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some((
                                                    Arc::new(val.0.unwrap().to_vec()),
                                                    Arc::new(val.1.to_vec()),
                                                ))));
                                            }

                                            Err(Error::NotFound) => cb(Ok(None)),

                                            Err(e) => cb(Err(format!(
                                                "cursor get error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    }
                                }
                            } else {
                                if desc {
                                    match cursor.get(None, None, MDB_NEXT) {
                                        Ok(val) => {
                                            cur_iter_key = Some(Arc::new(val.0.unwrap().to_vec()));
                                            cb(Ok(Some((
                                                Arc::new(val.0.unwrap().to_vec()),
                                                Arc::new(val.1.to_vec()),
                                            ))));
                                        }

                                        Err(Error::NotFound) => cb(Ok(None)),

                                        Err(e) => cb(Err(format!(
                                            "cursor get error: {:?}",
                                            e.to_string()
                                        ))),
                                    }
                                } else {
                                    match cursor.get(
                                        Some(cur_iter_key.clone().unwrap().as_ref()),
                                        None,
                                        MDB_PREV,
                                    ) {
                                        Ok(val) => {
                                            cur_iter_key = Some(Arc::new(val.0.unwrap().to_vec()));
                                            cb(Ok(Some((
                                                Arc::new(val.0.unwrap().to_vec()),
                                                Arc::new(val.1.to_vec()),
                                            ))));
                                        }

                                        Err(Error::NotFound) => cb(Ok(None)),

                                        Err(e) => cb(Err(format!(
                                            "cursor get error: {:?}",
                                            e.to_string()
                                        ))),
                                    }
                                }
                            }
                        }

                        Ok(LmdbMessage::CreateKeyIter(descending, key, tx)) => {
                            desc = descending;
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            iter_from_start_or_end = if key.is_none() {
                                true
                            } else {
                                cur_iter_key = key;

                                false
                            };

                            let _ = tx.send(());
                        }

                        Ok(LmdbMessage::NextKey(cb)) => {
                            let cursor = thread_local_txn
                                .as_ref()
                                .unwrap()
                                .open_ro_cursor(db.clone().unwrap())
                                .unwrap();

                            if cur_iter_key.is_some() {
                                match cursor.get(
                                    Some(cur_iter_key.clone().unwrap().as_ref()),
                                    None,
                                    MDB_SET,
                                ) {
                                    Ok(_) => {}
                                    Err(Error::NotFound) => {}
                                    Err(e) => {
                                        cb(Err(format!("MDB_SET failed: {:?}", e.to_string())))
                                    }
                                }
                            }

                            if iter_from_start_or_end {
                                if desc {
                                    if cur_iter_key.is_none() {
                                        match cursor.get(None, None, MDB_NEXT) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some(Arc::new(val.0.unwrap().to_vec()))));
                                            }

                                            Err(Error::NotFound) => cb(Ok(None)),

                                            Err(e) => cb(Err(format!(
                                                "cursor get error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    } else {
                                        match cursor.get(
                                            Some(cur_iter_key.clone().unwrap().as_ref()),
                                            None,
                                            MDB_NEXT,
                                        ) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some(Arc::new(val.0.unwrap().to_vec()))));
                                            }

                                            Err(Error::NotFound) => cb(Ok(None)),

                                            Err(e) => cb(Err(format!(
                                                "cursor get error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    }
                                } else {
                                    if cur_iter_key.is_none() {
                                        match cursor.get(None, None, MDB_PREV) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some(Arc::new(val.0.unwrap().to_vec()))));
                                            }

                                            Err(Error::NotFound) => cb(Ok(None)),

                                            Err(e) => cb(Err(format!(
                                                "cursor get error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    } else {
                                        match cursor.get(
                                            Some(cur_iter_key.clone().unwrap().as_ref()),
                                            None,
                                            MDB_PREV,
                                        ) {
                                            Ok(val) => {
                                                cur_iter_key =
                                                    Some(Arc::new(val.0.unwrap().to_vec()));
                                                cb(Ok(Some(Arc::new(val.0.unwrap().to_vec()))));
                                            }

                                            Err(Error::NotFound) => cb(Ok(None)),

                                            Err(e) => cb(Err(format!(
                                                "cursor get error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    }
                                }
                            } else {
                                if desc {
                                    match cursor.get(
                                        Some(cur_iter_key.clone().unwrap().as_ref()),
                                        None,
                                        MDB_NEXT,
                                    ) {
                                        Ok(val) => {
                                            cur_iter_key = Some(Arc::new(val.0.unwrap().to_vec()));
                                            cb(Ok(Some(Arc::new(val.0.unwrap().to_vec()))));
                                        }

                                        Err(Error::NotFound) => cb(Ok(None)),

                                        Err(e) => cb(Err(format!(
                                            "cursor get error: {:?}",
                                            e.to_string()
                                        ))),
                                    }
                                } else {
                                    match cursor.get(
                                        Some(cur_iter_key.clone().unwrap().as_ref()),
                                        None,
                                        MDB_PREV,
                                    ) {
                                        Ok(val) => {
                                            cur_iter_key = Some(Arc::new(val.0.unwrap().to_vec()));
                                            cb(Ok(Some(Arc::new(val.0.unwrap().to_vec()))));
                                        }

                                        Err(Error::NotFound) => cb(Ok(None)),

                                        Err(e) => cb(Err(format!(
                                            "cursor get error: {:?}",
                                            e.to_string()
                                        ))),
                                    }
                                }
                            }
                        }

                        Ok(LmdbMessage::Modify(keys, cb)) => {
                            println!(
                                "enter thread pool modify: keys = {:?}, len = {:?}, db = {:?}",
                                keys,
                                keys.len(),
                                db
                            );

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            if keys[0].ware == Atom::from("") && keys[0].tab == Atom::from("") {
                                // meta
                                if let Some(_) = keys[1].value {
                                    match thread_local_txn.as_mut().unwrap().put(
                                        db.clone().unwrap(),
                                        keys[1].key.as_ref(),
                                        keys[1].clone().value.unwrap().as_ref(),
                                        WriteFlags::empty(),
                                    ) {
                                        Ok(_) => {}
                                        Err(e) => cb(Err(format!(
                                            "insert data error: {:?}",
                                            e.to_string()
                                        ))),
                                    };
                                } else {
                                    match thread_local_txn.as_mut().unwrap().del(
                                        db.clone().unwrap(),
                                        keys[1].key.as_ref(),
                                        None,
                                    ) {
                                        Ok(_) => {}
                                        Err(Error::NotFound) => {}
                                        Err(e) => cb(Err(format!(
                                            "delete data error: {:?}",
                                            e.to_string()
                                        ))),
                                    };
                                }
                            } else {
                                for kv in keys.iter() {
                                    let db_name = kv.tab.clone();
                                    let db = env.open_db(Some(&db_name.to_string())).ok();

                                    println!("modify get db name: dbi = {:?}, kv: {:?}", db, kv);

                                    if let Some(_) = kv.value {
                                        match thread_local_txn.as_mut().unwrap().put(
                                            db.clone().unwrap(),
                                            kv.key.as_ref(),
                                            kv.clone().value.unwrap().as_ref(),
                                            WriteFlags::empty(),
                                        ) {
                                            Ok(_) => {}
                                            Err(e) => cb(Err(format!(
                                                "insert data error: {:?}",
                                                e.to_string()
                                            ))),
                                        };
                                    } else {
                                        match thread_local_txn.as_mut().unwrap().del(
                                            db.clone().unwrap(),
                                            kv.key.as_ref(),
                                            None,
                                        ) {
                                            Ok(_) => {}
                                            Err(Error::NotFound) => {}
                                            Err(e) => cb(Err(format!(
                                                "delete data error: {:?}",
                                                e.to_string()
                                            ))),
                                        };
                                    }
                                }
                            }

                            cb(Ok(()))
                        }

                        Ok(LmdbMessage::Commit(cb)) => {
                            if let Some(txn) = thread_local_txn.take() {
                                match txn.commit() {
                                    Ok(_) => {
                                        cb(Ok(()));
                                    }
                                    Err(e) => cb(Err(format!(
                                        "commit failed with error: {:?}",
                                        e.to_string()
                                    ))),
                                }
                            } else {
                                cb(Ok(()))
                            }
                        }

                        Ok(LmdbMessage::Rollback(cb)) => {
                            if let Some(txn) = thread_local_txn.take() {
                                txn.abort();
                                cb(Ok(()))
                            } else {
                                cb(Ok(()))
                            }
                        }

                        Ok(LmdbMessage::TableSize(cb)) => match env.stat() {
                            Ok(stat) => cb(Ok(stat.entries())),
                            Err(e) => cb(Err(e.to_string())),
                        },

                        Err(_e) => {
                            // unexpected message, do nothing
                        }
                    }
                }
            });
            self.senders.push(tx);
        }
        self.idle = cap;
        self.total = cap;
    }

    pub fn pop(&mut self) -> Option<Sender<LmdbMessage>> {
        self.idle -= 1;
        self.senders.pop()
    }

    pub fn push(&mut self, sender: Sender<LmdbMessage>) {
        self.idle += 1;
        self.senders.push(sender);
    }

    pub fn total_threads(&self) -> usize {
        self.total
    }

    pub fn idle_threads(&self) -> usize {
        self.idle
    }
}

lazy_static! {
    pub static ref THREAD_POOL: Arc<Mutex<ThreadPool>> = Arc::new(Mutex::new(ThreadPool::new()));
    pub static ref NO_OP_POOL: Arc<Mutex<NopPool>> = Arc::new(Mutex::new(NopPool::new()));
}
