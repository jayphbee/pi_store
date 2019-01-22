use crossbeam_channel::{bounded, Sender};
use std::slice::from_raw_parts;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use lmdb::{
    mdb_set_compare, Cursor, Database, DatabaseFlags, Environment, Error, Iter as LmdbIter,
    MDB_cmp_func, MDB_val, RwTransaction, Transaction, WriteFlags,
};

use pi_db::db::{Bin, NextResult, SResult, TabKV, TxCallback, TxQueryCallback};

use bon::ReadBuffer;
use atom::Atom;

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
}

unsafe impl Send for LmdbMessage {}

#[derive(Debug)]
pub struct ThreadPool {
    senders: Vec<Sender<LmdbMessage>>,
    total: usize,
    idle: usize,
}

pub enum NopMessage {
    Nop(TxCallback)
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

            thread::spawn(move || {
                loop {
                    match rx.recv() {
                        Ok(NopMessage::Nop(cb)) => {
                            cb(Ok(()))
                        },

                        Err(_) => {

                        }
                    }
                }
            });

            self.senders.push(tx);
        }
    }

    pub fn push(&mut self, sender: Sender<NopMessage>) {
        self.senders.push(sender);
    }

    pub fn pop (&mut self) -> Option<Sender<NopMessage>> {
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
                let mut thread_local_iter: Option<LmdbIter> = None;
                let mut db: Option<Database> = None;

                loop {
                    match rx.recv() {
                        Ok(LmdbMessage::NoOp(cb)) => {
                            cb(Ok(()))
                        }

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
                                thread_local_txn = env.begin_rw_txn().ok();
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
                                thread_local_txn = env.begin_rw_txn().ok();
                                let txn = thread_local_txn.as_mut().unwrap();
                                let mut cursor = txn.open_ro_cursor(db.clone().unwrap()).unwrap();
                                if let Some(k) = key {
                                    thread_local_iter = Some(
                                        cursor.iter_from_with_direction(k.to_vec(), descending),
                                    );
                                    println!("create item iter success");
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

                        Ok(LmdbMessage::Modify(keys, cb)) => {
                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();

                                unsafe {
                                    mdb_set_compare(
                                        thread_local_txn.as_ref().unwrap().txn(),
                                        db.clone().unwrap().dbi(),
                                        mdb_cmp_func as *mut MDB_cmp_func,
                                    );
                                }
                            }

                            let rw_txn = thread_local_txn.as_mut().unwrap();

                            for kv in keys.iter() {
                                if kv.ware == Atom::from("") && kv.tab == Atom::from("") {
                                    continue;
                                }

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
    pub static ref NO_OP_POOL: Arc<Mutex<NopPool>> = Arc::new(Mutex::new(NopPool::new()));
}
