use std::path::Path;
use std::slice::from_raw_parts;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use crossbeam_channel::{bounded, unbounded, Receiver, Select, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use lmdb::{
    mdb_set_compare, Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Error,
    Iter as LmdbIter, MDB_cmp_func, MDB_dbi, MDB_txn, MDB_val, RoCursor, RoTransaction, RwCursor,
    RwTransaction, Stat, Transaction, WriteFlags,
};

use lmdb_file::{MDB_FIRST, MDB_LAST, MDB_NEXT, MDB_PREV, MDB_SET};

use pi_db::db::{
    Bin, CommitResult, DBResult, Iter, IterResult, KeyIterResult, MetaTxn, NextResult, OpenTab,
    RwLog, SResult, Tab, TabKV, TabMeta, TabTxn, TxCallback, TxQueryCallback, TxState, Txn, Ware,
    WareSnapshot,
};

use pi_lib::bon::{Decode, Encode, ReadBuffer, WriteBuffer};

pub enum LmdbMessage {
    CreateDb(String, Sender<()>),
    Query(String, Arc<Vec<TabKV>>, TxQueryCallback),
    NextItem(String, Arc<Fn(NextResult<(Bin, Bin)>)>),
    NextKey(String, Arc<Fn(NextResult<Bin>)>),
    CreateItemIter(String, bool, Option<Bin>),
    CreateKeyIter(String, bool, Option<Bin>),
    Modify(String, Arc<Vec<TabKV>>, TxCallback),
    Commit(String, TxCallback),
    Rollback(String, TxCallback),
    TableSize(Arc<Fn(SResult<usize>)>),
}

unsafe impl Send for LmdbMessage {}

#[derive(Debug)]
pub struct ThreadPool {
    senders: Vec<Sender<LmdbMessage>>,
    total: usize,
    idle: usize,
}

impl ThreadPool {
    pub fn new() -> Self {
        println!("create thread pool");
        ThreadPool {
            senders: Vec::new(),
            total: 0,
            idle: 0,
        }
    }
    pub fn start_pool(&mut self, cap: usize, env: Arc<Environment>) {
        println!("start thread pool");
        for i in 0..cap {
            let clone_env = env.clone();
            let (tx, rx) = bounded(1);

            thread::spawn(move || {
                let env = clone_env;
                let mut thread_local_txn: Option<RwTransaction> = None;
                let mut thread_local_iter: Option<LmdbIter> = None;
                let mut db: Option<Database> = None;

                loop {
                    match rx.recv() {
                        Ok(LmdbMessage::CreateDb(db_name, tx)) => {
                            db = match env.open_db(Some(&db_name.to_string())) {
                                Ok(db) => Some(db),
                                Err(_) => Some(
                                    env.create_db(
                                        Some(&db_name.to_string()),
                                        DatabaseFlags::empty(),
                                    )
                                    .unwrap(),
                                ),
                            };

                            tx.send(());
                        }

                        Ok(LmdbMessage::Query(db_name, keys, cb)) => {
                            let mut values = Vec::new();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let txn = thread_local_txn.take().unwrap();
                            unsafe {
                                mdb_set_compare(
                                    txn.txn(),
                                    db.clone().unwrap().dbi(),
                                    mdb_cmp_func as *mut MDB_cmp_func,
                                );
                            }

                            for kv in keys.iter() {
                                match txn.get(db.clone().unwrap(), kv.key.as_ref()) {
                                    Ok(v) => {
                                        values.push(TabKV {
                                            ware: kv.ware.clone(),
                                            tab: kv.tab.clone(),
                                            key: kv.key.clone(),
                                            index: kv.index,
                                            value: Some(Arc::new(Vec::from(v))),
                                        });
                                    }
                                    Err(Error::NotFound) => {
                                        values.push(TabKV {
                                            ware: kv.ware.clone(),
                                            tab: kv.tab.clone(),
                                            key: kv.key.clone(),
                                            index: kv.index,
                                            value: None,
                                        });
                                    }
                                    Err(e) => {
                                        cb(Err(format!(
                                            "lmdb internal error: {:?}",
                                            e.to_string()
                                        )));
                                        break;
                                    }
                                }
                            }
                            cb(Ok(values));
                        }

                        Ok(LmdbMessage::CreateItemIter(db_name, descending, key)) => {
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                                let txn = thread_local_txn.as_mut().unwrap();
                                let mut cursor = txn.open_ro_cursor(db.clone().unwrap()).unwrap();
                                if let Some(k) = key {
                                    thread_local_iter = Some(
                                        cursor.iter_from_with_direction(k.to_vec(), descending),
                                    );
                                } else {
                                    thread_local_iter =
                                        Some(cursor.iter_items_with_direction(descending));
                                }
                            }
                        }

                        Ok(LmdbMessage::NextItem(db_name, cb)) => {
                            if let Some(ref mut iter) = thread_local_iter {
                                match iter.next() {
                                    Some(v) => cb(Ok(Some((
                                        Arc::new(v.0.to_vec()),
                                        Arc::new(v.1.to_vec()),
                                    )))),
                                    None => cb(Ok(None)),
                                }
                            } else {
                                cb(Err("Iterator not initialized".to_string()))
                            }
                        }

                        Ok(LmdbMessage::CreateKeyIter(db_name, descending, key)) => {
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                                let txn = thread_local_txn.as_mut().unwrap();
                                let mut cursor = txn.open_ro_cursor(db.clone().unwrap()).unwrap();
                                if let Some(k) = key {
                                    thread_local_iter = Some(
                                        cursor.iter_from_with_direction(k.to_vec(), descending),
                                    );
                                } else {
                                    thread_local_iter =
                                        Some(cursor.iter_items_with_direction(descending));
                                }
                            }
                        }

                        Ok(LmdbMessage::NextKey(db_name, cb)) => {
                            if let Some(ref mut iter) = thread_local_iter {
                                match iter.next() {
                                    Some(v) => cb(Ok(Some(Arc::new(v.0.to_vec())))),
                                    None => cb(Ok(None)),
                                }
                            } else {
                                cb(Err("Iterator not initialized".to_string()))
                            }
                        }

                        Ok(LmdbMessage::Modify(db_name, keys, cb)) => {
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let mut rw_txn = thread_local_txn.as_mut().unwrap();
                            unsafe {
                                mdb_set_compare(
                                    rw_txn.txn(),
                                    db.clone().unwrap().dbi(),
                                    mdb_cmp_func as *mut MDB_cmp_func,
                                );
                            }

                            for kv in keys.iter() {
                                if let Some(_) = kv.value {
                                    match rw_txn.put(
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
                                    match rw_txn.del(db.clone().unwrap(), kv.key.as_ref(), None) {
                                        Ok(_) => {}
                                        Err(Error::NotFound) => {}
                                        Err(e) => cb(Err(format!(
                                            "delete data error: {:?}",
                                            e.to_string()
                                        ))),
                                    };
                                }
                            }
                            cb(Ok(()))
                        }

                        Ok(LmdbMessage::Commit(db_name, cb)) => {
                            if let Some(txn) = thread_local_txn.take() {
                                match txn.commit() {
                                    Ok(_) => cb(Ok(())),
                                    Err(e) => cb(Err(format!(
                                        "commit failed with error: {:?}",
                                        e.to_string()
                                    ))),
                                }
                            } else {
                                cb(Ok(()))
                            }
                        }

                        Ok(LmdbMessage::Rollback(db_name, cb)) => {
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

                        Err(e) => {
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

// comprator function
fn mdb_cmp_func(a: *const MDB_val, b: *const MDB_val) -> i32 {
    unsafe {
        let buf1 = from_raw_parts::<u8>((*a).mv_data as *const u8, (*a).mv_size);
        let buf2 = from_raw_parts::<u8>((*b).mv_data as *const u8, (*b).mv_size);

        let b1 = ReadBuffer::new(buf1, 0);
        let b2 = ReadBuffer::new(buf2, 0);

        if b1 > b2 {
            1
        } else if b1 == b2 {
            0
        } else {
            -1
        }
    }
}

lazy_static! {
    pub static ref THREAD_POOL: Arc<Mutex<ThreadPool>> = Arc::new(Mutex::new(ThreadPool::new()));
}
