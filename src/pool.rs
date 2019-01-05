use crossbeam_channel::{bounded, Sender};
use std::slice::from_raw_parts;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use lmdb::{
    mdb_set_compare, Cursor, Database, DatabaseFlags, Environment, Error, Iter as LmdbIter,
    MDB_cmp_func, MDB_val, RwTransaction, RoTransaction, Transaction, WriteFlags,
};

use pi_db::db::{Bin, NextResult, SResult, TabKV, TxCallback, TxQueryCallback};

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
}

unsafe impl Send for LmdbMessage {}

#[derive(Debug)]
pub struct ThreadPool {
    rw_sender: Option<Sender<LmdbMessage>>,
    senders: Vec<Sender<LmdbMessage>>,
    total: usize,
    idle: usize,
}

impl ThreadPool {
    pub fn new() -> Self {
        ThreadPool {
            rw_sender: None,
            senders: Vec::new(),
            total: 0,
            idle: 0,
        }
    }

    pub fn create_rw_txn(&mut self, env: Arc<Environment>) {
        let (tx, rx) = bounded(1);
        self.rw_sender = Some(tx);

        thread::spawn(move || {
            let mut rw_txn: Option<RwTransaction> = None;
            loop {
                match rx.recv() {
                    Ok(LmdbMessage::Modify(items, cb)) => {
                        for item in items.iter() {
                            let db = match env.open_db(Some(&item.tab.to_string())) {
                                Ok(db) => Some(db),
                                Err(_) => Some(
                                    env.create_db(
                                        Some(&item.tab.to_string()),
                                        DatabaseFlags::empty(),
                                    )
                                    .unwrap(),
                                ),
                            };

                            if rw_txn.is_none() {
                                rw_txn = env.begin_rw_txn().ok();
                                unsafe {
                                    mdb_set_compare(
                                        rw_txn.as_ref().unwrap().txn(),
                                        db.clone().unwrap().dbi(),
                                        mdb_cmp_func as *mut MDB_cmp_func,
                                    );
                                }
                            }

                            if let Some(_) = item.clone().value {
                                match rw_txn.as_mut().unwrap().put(
                                    db.clone().unwrap(),
                                    item.key.as_ref(),
                                    item.clone().value.unwrap().as_ref(),
                                    WriteFlags::empty()
                                ) {
                                    Ok(_) => {}
                                    Err(e) => cb(Err(format!(
                                        "insert data error: {:?}",
                                        e.to_string()
                                    ))),
                                }
                            } else {
                                match rw_txn.as_mut().unwrap().del(db.clone().unwrap(), item.key.as_ref(), None) {
                                    Ok(_) => {}
                                    Err(Error::NotFound) => {}
                                    Err(e) => cb(Err(format!(
                                        "delete data error: {:?}",
                                        e.to_string()
                                    ))),
                                };
                            }
                            cb(Ok(()))
                        }
                    }

                    Ok(LmdbMessage::Commit(cb)) => {
                        if let Some(txn) = rw_txn.take() {
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

                    Ok(LmdbMessage::Rollback(cb)) => {
                        if let Some(txn) = rw_txn.take() {
                            txn.abort();
                            cb(Ok(()))
                        } else {
                            cb(Ok(()))
                        }
                    }

                    Ok(_) => {
                        panic!("Unexpected message received in read-write transaction");
                    }

                    Err(_e) => {

                    }
                }
            }
        });
    }

    pub fn start_pool(&mut self, cap: usize, env: Arc<Environment>) {
        for _ in 0..cap {
            let clone_env = env.clone();
            let (tx, rx) = bounded(1);

            thread::spawn(move || {
                let env = clone_env;
                let mut thread_local_txn: Option<RoTransaction> = None;
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

                            let _ = tx.send(());
                        }

                        Ok(LmdbMessage::Query(keys, cb)) => {
                            let mut values = Vec::new();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_ro_txn().ok();
                                unsafe {
                                    mdb_set_compare(
                                        thread_local_txn.as_ref().unwrap().txn(),
                                        db.clone().unwrap().dbi(),
                                        mdb_cmp_func as *mut MDB_cmp_func,
                                    );
                                }
                            }

                            let txn = thread_local_txn.take().unwrap();

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

                        Ok(LmdbMessage::CreateItemIter(descending, key, tx)) => {
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_ro_txn().ok();
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
                            let _ = tx.send(());
                        }

                        Ok(LmdbMessage::NextItem(cb)) => {
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

                        Ok(LmdbMessage::CreateKeyIter(descending, key, tx)) => {
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_ro_txn().ok();
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
                            let _ = tx.send(());
                        }

                        Ok(LmdbMessage::NextKey(cb)) => {
                            if let Some(ref mut iter) = thread_local_iter {
                                match iter.next() {
                                    Some(v) => cb(Ok(Some(Arc::new(v.0.to_vec())))),
                                    None => cb(Ok(None)),
                                }
                            } else {
                                cb(Err("Iterator not initialized".to_string()))
                            }
                        }

                        Ok(LmdbMessage::Modify(_, _)) => {
                            panic!("Unexpected Lmdb::Modify message received in read-only transaction");
                        }

                        Ok(LmdbMessage::Commit(cb)) => {
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
