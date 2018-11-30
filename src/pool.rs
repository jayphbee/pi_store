use std::thread;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{ Sender, Receiver, channel };

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

pub enum LmdbMessage {
    NewTxn(Arc<Environment>, String, bool),
    Query(String, Arc<Vec<TabKV>>, TxQueryCallback),
    IterItems(Arc<Environment>, String, bool, Option<Bin>, Arc<Fn(IterResult)>),
    IterKeys(Arc<Environment>, String, Arc<Fn(KeyIterResult)>),
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
                println!("create thread with thread id {:?}", thread::current().id());

                let mut rw_txn_ptr: usize = 0;
                let mut ro_txn_ptr: usize = 0;

                let env = clone_env;
                let mut thread_local_txn: Option<RwTransaction> = None;

                loop {
                    match rx.recv() {
                        // This is the very first message should be sent before any database operation, or will be crashed.
                        Ok(LmdbMessage::NewTxn(db_env, db_name, writable)) => {
                            let db = db_env.open_db(Some(&db_name.to_string())).unwrap();
                            if writable {
                                rw_txn_ptr = unsafe {
                                    match db_env.begin_rw_txn() {
                                        Ok(mut txn) => {
                                            match txn.put(db, b"foo", b"bar", WriteFlags::empty()) {
                                                Ok(_) => println!("NewTxn write success"),
                                                Err(e) => println!("Netxn write failed: {:?}", e)
                                            }
                                            let boxed = Box::into_raw(Box::new(txn)) as usize;
                                            println!("after boxed: {:?}", boxed);
                                            boxed
                                        },
                                        Err(e) => {
                                            println!("falid to create txn");
                                            0
                                        }
                                    }

                                    // Box::into_raw(Box::new(db_env.begin_rw_txn().unwrap())) as usize
                                };
                                println!("create rw txn in thread: {:?} rw_txn_ptr: {}", thread::current().id(), rw_txn_ptr);
                            } else {
                                ro_txn_ptr = unsafe {
                                    Box::into_raw(Box::new(db_env.begin_ro_txn().unwrap())) as usize
                                };
                                println!("create ro txn in thread: {:?} ro_txn_ptr: {}", thread::current().id(), ro_txn_ptr);
                            }
                        },

                        Ok(LmdbMessage::Query(db_name, keys, cb)) => {
                            let mut values = Vec::new();
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            let txn = thread_local_txn.take().unwrap();

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

                        Ok(LmdbMessage::IterItems(db_env, db_name, descending, key, cb)) => {
                            let db = db_env.open_db(Some(&db_name.to_string())).unwrap();
                            let ro_txn = unsafe {
                                Box::from_raw(ro_txn_ptr as *mut RoTransaction)
                            };
                            let cursor = ro_txn.open_ro_cursor(db).unwrap();

                            if let Some(k) = key.clone() {
                                cursor.get(Some(k.as_ref()), None, MDB_SET);
                            } else {
                                if descending {
                                    cursor.get(None, None, MDB_FIRST);
                                } else {
                                    cursor.get(None, None, MDB_LAST);
                                }
                            }

                            println!("iter items");
                        },

                        Ok(LmdbMessage::IterKeys(db_env, db_name, cb)) => {
                            let db = env.open_db(Some(&db_name.to_string())).unwrap();

                            if thread_local_txn.is_none() {
                                thread_local_txn = env.begin_rw_txn().ok();
                            }

                            thread_local_txn.as_mut().unwrap().put(db, b"foo", b"bar", WriteFlags::empty());
                            thread_local_txn.as_mut().unwrap().put(db, b"foo1", b"bar1", WriteFlags::empty());

                            let txn = thread_local_txn.take();

                            match txn.unwrap().commit() {
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
                                    println!("del");
                                    match rw_txn.del(db, kv.key.as_ref(), None) {
                                        Ok(_) => {
                                            println!("delete {:?} success", kv.clone().key.as_ref());
                                        }
                                        Err(e) => return cb(Err(e.to_string()))
                                    };
                                }
                            }
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
                            println!("rollback in thread {:?} with rw_tx_ptr: {}", thread::current().id(), rw_txn_ptr);
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