use std::thread;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{ Sender, Receiver, channel };
use std::slice::from_raw_parts;

use lmdb::{
    Environment, Database, WriteFlags, Error, Transaction, EnvironmentFlags,
    DatabaseFlags, RwTransaction, RoTransaction, RoCursor, Cursor, RwCursor,

    mdb_set_compare, MDB_txn, MDB_dbi, MDB_val, MDB_cmp_func
};

use lmdb_file::{
    MDB_SET, MDB_PREV, MDB_NEXT, MDB_FIRST, MDB_LAST
};

use pi_db::db::{
    Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult,
    NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn,
    Tab, OpenTab, Ware, WareSnapshot, TxState, Iter, CommitResult,
    RwLog, TabMeta
};

use pi_lib::bon::{ReadBuffer, WriteBuffer, Encode, Decode};

pub enum LmdbMessage {
    NewTxn(Arc<Environment>, String, bool),
    Query(String, Arc<Vec<TabKV>>, TxQueryCallback),
    IterItems(String, bool, Option<Bin>, Arc<Fn(IterResult)>),
    IterKeys(String, Arc<Fn(KeyIterResult)>),
    Modify(String, Arc<Vec<TabKV>>, TxCallback),
    Commit(String, TxCallback),
    Rollback(String, TxCallback),
}

#[derive(Debug, Clone)]
pub enum TxnType {
    ReadWrite,
    ReadOnly
}

unsafe impl Send for LmdbMessage {}

pub struct ThreadPool {
    senders: Vec<Sender<LmdbMessage>>,
    total: usize,
    idle: usize
}

impl ThreadPool {
    pub fn with_capacity(cap: usize, env: Arc::<Environment>) -> Self {
        let mut senders = Vec::new();

        for i in 0..cap {
            let clone_env = env.clone();
            let (tx, rx) = channel();

            thread::spawn(move || {
                let env = clone_env;
                let mut thread_local_txn: Option<RwTransaction> = None;

                loop {
                    match rx.recv() {
                        Ok(LmdbMessage::NewTxn(db_env, db_name, writable)) => {

                        },

                        Ok(LmdbMessage::Query(db_name, keys, cb)) => {
                            let mut values = Vec::new();
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let txn = thread_local_txn.take().unwrap();
                            unsafe { mdb_set_compare(txn.txn(), db.dbi(), mdb_cmp_func as *mut MDB_cmp_func); }

                            for kv in keys.iter() {
                                match txn.get(db, kv.key.as_ref()) {
                                    Ok(v) => {
                                        values.push(TabKV {
                                            ware: kv.ware.clone(),
                                            tab: kv.tab.clone(),
                                            key: kv.key.clone(),
                                            index: kv.index,
                                            value: Some(Arc::new(Vec::from(v)))
                                        });
                                        println!("query success: {:?}", values);
                                    },
                                    Err(e) => {
                                        println!("query failed {:?}", e);
                                        cb(Err(e.to_string()));
                                    }
                                }
                            }
                            cb(Ok(values));
                        },

                        Ok(LmdbMessage::IterItems(db_name, descending, key, cb)) => {
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let txn = thread_local_txn.take().unwrap();
                            unsafe { mdb_set_compare(txn.txn(), db.dbi(), mdb_cmp_func as *mut MDB_cmp_func); }
                            let mut cursor = txn.open_ro_cursor(db).unwrap();

                            if let Some(k) = key.clone() {
                                println!("get items1 {:?}", cursor.get(Some(k.as_ref()), None, MDB_SET));
                            } else {
                                if descending {
                                    println!("get items2 {:?}", cursor.get(None, None, MDB_FIRST));
                                } else {
                                    println!("get items3 {:?}", cursor.get(None, None, MDB_LAST));
                                }
                            }

                            for item in cursor.iter() {
                                println!("{:?}", item);
                            }
                        },

                        Ok(LmdbMessage::IterKeys(db_name, cb)) => {
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            thread_local_txn.as_mut().unwrap().put(db, b"foo", b"bar", WriteFlags::empty());
                            thread_local_txn.as_mut().unwrap().put(db, b"foo1", b"bar1", WriteFlags::empty());

                            let txn = thread_local_txn.take().unwrap();
                            unsafe { mdb_set_compare(txn.txn(), db.dbi(), mdb_cmp_func as *mut MDB_cmp_func); }

                            match txn.commit() {
                                Ok(_) => {
                                    println!("gtxn commit success: {:?}", thread_local_txn);
                                },
                                Err(_) => {
                                    println!("gtxn commit failed");
                                }
                            }
                        },

                        Ok(LmdbMessage::Modify(db_name, keys, cb)) => {
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let mut rw_txn = thread_local_txn.take().unwrap();
                            unsafe { mdb_set_compare(rw_txn.txn(), db.dbi(), mdb_cmp_func as *mut MDB_cmp_func); }

                            for kv in keys.iter() {
                                if let Some(_) = kv.value {
                                    match rw_txn.put(db, kv.key.as_ref(), kv.clone().value.unwrap().as_ref(), WriteFlags::empty()) {
                                        Ok(_) => {
                                            println!("insert {:?} success", kv.clone().key.as_ref());
                                        }
                                        Err(e) => {
                                            println!("modify error {:?}", e);
                                            return cb(Err("insert failed".to_string()))
                                        }
                                    };
                                } else {
                                    match rw_txn.del(db, kv.key.as_ref(), None) {
                                        Ok(_) => {
                                            println!("delete {:?} success", kv.clone().key.as_ref());
                                        }
                                        Err(e) => return cb(Err(e.to_string()))
                                    };
                                }
                            }
                            thread_local_txn = Some(rw_txn);
                        },

                        // only commit rw txn
                        Ok(LmdbMessage::Commit(db_name, cb)) => {
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let mut rw_txn = thread_local_txn.take().unwrap();

                            match rw_txn.commit() {
                                Ok(_) => {
                                    cb(Ok(()));
                                    println!("commit success");
                                },
                                Err(e) => {
                                    cb(Err(e.to_string()));
                                    println!("commit failed: {}", e);
                                }
                            }
                        },

                        // only abort tw txn
                        Ok(LmdbMessage::Rollback(db_name, cb)) => {
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let mut rw_txn = thread_local_txn.take().unwrap();

                            rw_txn.abort();
                            cb(Ok(()));
                        },

                        Err(e) => {
                            // unexpected message, do nothing
                        },

                    }
                }
            });
            senders.push(tx);
        }

        ThreadPool {
            senders,
            total: cap,
            idle: cap
        }
    }

    pub fn pop(&mut self) -> Option<Sender<LmdbMessage>> {
        self.idle = self.idle - 1;
        self.senders.pop()
    }

    pub fn push(&mut self, sender: Sender<LmdbMessage>) {
        self.idle = self.idle + 1;
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