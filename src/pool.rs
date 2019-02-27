use crossbeam_channel::{bounded, unbounded, Sender};
use std::collections::HashMap;
use std::slice::from_raw_parts;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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

pub enum DbTabRWMessage {
    Modify(u64, Arc<Vec<TabKV>>, TxCallback),
    Commit(u64, TxCallback),
    Rollback(u64, TxCallback),
}

unsafe impl Send for DbTabRWMessage {}

pub enum DbTabROMessage {
    Query(u64, Arc<Vec<TabKV>>, TxQueryCallback),
    CreateItemIter(u64, bool, Option<Bin>, Sender<()>),
    NextItem(u64, Arc<Fn(NextResult<(Bin, Bin)>)>),
}

unsafe impl Send for DbTabROMessage {}

#[derive(Debug)]
pub struct DbTabWrite {
    env: Option<Arc<Environment>>,
    sender: Option<Sender<DbTabRWMessage>>,
    modifications: Arc<Mutex<HashMap<u64, (Vec<Arc<Vec<TabKV>>>, u32)>>>,
    must_abort: Arc<Mutex<HashMap<u64, bool>>>,
}

impl DbTabWrite {
    pub fn new() -> Self {
        Self {
            env: None,
            sender: None,
            modifications: Arc::new(Mutex::new(HashMap::new())),
            must_abort: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set_env(&mut self, env: Arc<Environment>) {
        self.env = Some(env);
    }

    pub fn spawn_write_service(&mut self) {
        let env = self.env.clone().unwrap();
        let modifications = self.modifications.clone();
        let must_abort = self.must_abort.clone();
        let (tx, rx) = unbounded::<DbTabRWMessage>();

        let _ = thread::Builder::new()
            .name("lmdb_write_thread".to_owned())
            .spawn(move || loop {
                match rx.recv() {
                    Ok(DbTabRWMessage::Modify(txid, mods, cb)) => {
                        // batch write for one txn
                        modifications
                            .lock()
                            .unwrap()
                            .entry(txid)
                            .and_modify(|(m, c)| {
                                m.push(mods.clone());
                                *c += 1;
                            })
                            .or_insert((vec![mods.clone()], 1));

                        OPENED_TABLES
                            .lock()
                            .unwrap()
                            .entry(mods[0].tab.get_hash())
                            .or_insert_with(|| {
                                env.create_db(
                                    Some(&mods[0].tab.to_string()),
                                    DatabaseFlags::empty(),
                                )
                                .unwrap()
                            });
                        cb(Ok(()));
                    }

                    Ok(DbTabRWMessage::Commit(txid, cb)) => {
                        let (modifies, count) =
                            modifications.lock().unwrap().get(&txid).unwrap().clone();

                        if count > 1 {
                            modifications
                                .lock()
                                .unwrap()
                                .entry(txid)
                                .and_modify(|(_, c)| {
                                    *c -= 1;
                                });
                            cb(Ok(()));
                        } else {
                            let mut txn = env.begin_rw_txn().unwrap();

                            for mods in modifies.iter() {
                                for m in mods.iter() {
                                    // this db should be opened previously, or this is a bug
                                    let db = OPENED_TABLES
                                        .lock()
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
                                            Err(e) => cb(Err(format!(
                                                "delete data error: {:?}",
                                                e.to_string()
                                            ))),
                                        }
                                    }
                                }
                            }

                            if must_abort.lock().unwrap().get(&txid).is_some() {
                                txn.abort();
                                cb(Ok(()))
                            } else {
                                match txn.commit() {
                                    Ok(_) => {
                                        cb(Ok(()));
                                    }
                                    Err(e) => cb(Err(format!(
                                        "commit failed with error: {:?}",
                                        e.to_string()
                                    ))),
                                }
                            }
                            println!("======= txid: {:?} finally committed ==========", txid);
                            // remove txid from cache
                            modifications.lock().unwrap().remove(&txid);
                        }
                    }

                    Ok(DbTabRWMessage::Rollback(txid, cb)) => {
                        if modifications.lock().unwrap().contains_key(&txid) {
                            must_abort.lock().unwrap().entry(txid).and_modify(|v| {
                                *v = true;
                            });
                        }
                        cb(Ok(()));
                    }

                    Err(_e) => {}
                }
            });

        self.sender = Some(tx);
    }

    pub fn get_sender(&self) -> Option<Sender<DbTabRWMessage>> {
        self.sender.clone()
    }
}

#[derive(Debug)]
pub struct DbTabReadPool {
    env: Option<Arc<Environment>>,
    tab_sender_map: Arc<Mutex<HashMap<u64, Sender<DbTabROMessage>>>>,
    tab_iters: Arc<Mutex<HashMap<u64, (Option<Bin>, bool)>>>,
    no_op_senders: Arc<Mutex<HashMap<u64, Vec<Sender<NopMessage>>>>>,
    txn_count: Arc<Mutex<HashMap<u64, u32>>>,
}

impl DbTabReadPool {
    pub fn new() -> Self {
        Self {
            env: None,
            tab_sender_map: Arc::new(Mutex::new(HashMap::new())),
            tab_iters: Arc::new(Mutex::new(HashMap::new())),
            no_op_senders: Arc::new(Mutex::new(HashMap::new())),
            txn_count: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set_env(&mut self, env: Arc<Environment>) {
        self.env = Some(env);
    }

    pub fn spawn_read_service(&mut self, tab: Atom) {
        println!("======= spawn read service for tab: {:?} =======", tab);
        let env = self.env.clone().unwrap();
        let tab_iters = self.tab_iters.clone();
        let txn_count = self.txn_count.clone();

        OPENED_TABLES
            .lock()
            .unwrap()
            .entry(tab.get_hash())
            .or_insert_with(|| {
                env.create_db(Some(&tab.to_string()), DatabaseFlags::empty())
                    .unwrap()
            });

        self.tab_sender_map
            .lock()
            .unwrap()
            .entry(tab.get_hash())
            .or_insert_with(|| {
                let (tx, rx) = unbounded();

                let _ = thread::Builder::new()
                    .name(format!("lmdb_read_{}", tab.get_hash()))
                    .spawn(move || {
                        let db = env
                            .create_db(Some(&tab.to_string()), DatabaseFlags::empty())
                            .unwrap();
                        loop {
                            // handle channel message
                            match rx.recv() {
                                Ok(DbTabROMessage::Query(txid, queries, cb)) => {
                                    let mut qr = vec![];
                                    let mut query_error = false;
                                    let txn = env.begin_ro_txn().unwrap();

                                    txn_count
                                        .lock()
                                        .unwrap()
                                        .entry(txid)
                                        .and_modify(|count| *count += 1)
                                        .or_insert(1);

                                    for q in queries.iter() {
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
                                    let _ = txn.commit().unwrap();

                                    if query_error {
                                        cb(Err(format!("lmdb query internal error")));
                                    } else {
                                        cb(Ok(qr));
                                    }
                                }

                                Ok(DbTabROMessage::CreateItemIter(txid, desc, key, sender)) => {
                                    let txn = env.begin_ro_txn().unwrap();
                                    let cursor = txn.open_ro_cursor(db).unwrap();

                                    txn_count
                                        .lock()
                                        .unwrap()
                                        .entry(txid)
                                        .and_modify(|count| *count += 1)
                                        .or_insert(1);

                                    // full iterataion
                                    if key.is_none() {
                                        if desc {
                                            match cursor.get(None, None, MDB_FIRST) {
                                                Ok(val) => {
                                                    // save the first record key
                                                    tab_iters
                                                        .lock()
                                                        .unwrap()
                                                        .entry(txid)
                                                        .or_insert_with(|| {
                                                            if let Some(v) = val.0 {
                                                                (Some(Arc::new(v.to_vec())), desc)
                                                            } else {
                                                                (None, desc)
                                                            }
                                                        });
                                                }
                                                Err(Error::NotFound) => {
                                                    tab_iters
                                                        .lock()
                                                        .unwrap()
                                                        .entry(txid)
                                                        .or_insert((None, desc));
                                                }
                                                Err(_) => {}
                                            }
                                        } else {
                                            match cursor.get(None, None, MDB_LAST) {
                                                Ok(val) => {
                                                    // save the last record key
                                                    tab_iters
                                                        .lock()
                                                        .unwrap()
                                                        .entry(txid)
                                                        .or_insert_with(|| {
                                                            if let Some(v) = val.0 {
                                                                (Some(Arc::new(v.to_vec())), desc)
                                                            } else {
                                                                (None, desc)
                                                            }
                                                        });
                                                }
                                                Err(Error::NotFound) => {
                                                    tab_iters
                                                        .lock()
                                                        .unwrap()
                                                        .entry(txid)
                                                        .or_insert((None, desc));
                                                }
                                                Err(_) => {}
                                            }
                                        }
                                    // partial iteration
                                    } else {
                                        match cursor.get(
                                            Some(key.clone().unwrap().as_ref()),
                                            None,
                                            MDB_SET,
                                        ) {
                                            // save the current key
                                            Ok(val) => {
                                                tab_iters
                                                    .lock()
                                                    .unwrap()
                                                    .entry(txid)
                                                    .or_insert_with(|| {
                                                        if let Some(v) = val.0 {
                                                            (Some(Arc::new(v.to_vec())), desc)
                                                        } else {
                                                            (None, desc)
                                                        }
                                                    });
                                            }
                                            Err(Error::NotFound) => {
                                                tab_iters
                                                    .lock()
                                                    .unwrap()
                                                    .entry(txid)
                                                    .or_insert((None, desc));
                                            }
                                            Err(_) => {}
                                        }
                                    }

                                    // notify create iter success
                                    let _ = sender.send(()).unwrap();
                                }

                                Ok(DbTabROMessage::NextItem(txid, cb)) => {
                                    let (cur_key, desc) =
                                        tab_iters.lock().unwrap().get(&txid).unwrap().clone();
                                    let txn = env.begin_ro_txn().unwrap();
                                    let cursor = txn.open_ro_cursor(db).unwrap();

                                    // from top to bottom
                                    if desc {
                                        if cur_key.is_none() {
                                            cb(Ok(None));
                                        } else {
                                            match cursor.get(
                                                Some(cur_key.clone().unwrap().as_ref()),
                                                None,
                                                MDB_SET,
                                            ) {
                                                Ok(val) => cb(Ok(Some((
                                                    cur_key.clone().unwrap(),
                                                    Arc::new(val.1.to_vec()),
                                                )))),

                                                Err(Error::NotFound) => {}

                                                Err(_) => {}
                                            }

                                            // get next key
                                            match cursor.get(
                                                Some(cur_key.clone().unwrap().as_ref()),
                                                None,
                                                MDB_NEXT,
                                            ) {
                                                Ok(val) => {
                                                    // update current key for txid
                                                    tab_iters.lock().unwrap().insert(
                                                        txid,
                                                        (
                                                            Some(Arc::new(val.0.unwrap().to_vec())),
                                                            desc,
                                                        ),
                                                    );
                                                }

                                                Err(Error::NotFound) => {
                                                    tab_iters
                                                        .lock()
                                                        .unwrap()
                                                        .insert(txid, (None, desc));
                                                }

                                                Err(_) => {}
                                            }
                                        }
                                    // from bottom to top
                                    } else {
                                        if cur_key.is_none() {
                                            cb(Ok(None));
                                        } else {
                                            match cursor.get(
                                                Some(cur_key.clone().unwrap().as_ref()),
                                                None,
                                                MDB_SET,
                                            ) {
                                                Ok(val) => cb(Ok(Some((
                                                    cur_key.clone().unwrap(),
                                                    Arc::new(val.1.to_vec()),
                                                )))),

                                                Err(Error::NotFound) => {}

                                                Err(_) => {}
                                            }

                                            // get next key
                                            match cursor.get(
                                                Some(cur_key.clone().unwrap().as_ref()),
                                                None,
                                                MDB_PREV,
                                            ) {
                                                Ok(val) => {
                                                    // update current key for txid
                                                    tab_iters.lock().unwrap().insert(
                                                        txid,
                                                        (
                                                            Some(Arc::new(val.0.unwrap().to_vec())),
                                                            desc,
                                                        ),
                                                    );
                                                }

                                                Err(Error::NotFound) => {
                                                    tab_iters
                                                        .lock()
                                                        .unwrap()
                                                        .insert(txid, (None, desc));
                                                }

                                                Err(_) => {}
                                            }
                                        }
                                    }

                                    std::mem::drop(cursor);
                                    let _ = txn.commit().unwrap();
                                }

                                Err(_) => {}
                            }
                        }
                    });
                tx
            });
    }

    pub fn get_ro_sender(&self, tab: &Atom) -> Option<Sender<DbTabROMessage>> {
        self.tab_sender_map
            .lock()
            .unwrap()
            .get(&tab.get_hash())
            .map(|sender| sender.clone())
    }

    pub fn commit_ro_txn(&self, txid: u64, cb: TxCallback) {
        if *self.txn_count.lock().unwrap().get(&txid).unwrap() == 1 {
            // give back no op sender
            if let Some(senders) = self.no_op_senders.lock().unwrap().get(&txid) {
                senders
                    .iter()
                    .for_each(|s| NO_OP_POOL.lock().unwrap().push(s.clone()));
            }
            self.no_op_senders.lock().unwrap().remove(&txid);
            self.tab_iters.lock().unwrap().remove(&txid);
        } else {
            let nop_sender = NO_OP_POOL.lock().unwrap().pop().unwrap();
            let _ = nop_sender.send(NopMessage::Nop(cb));
            self.no_op_senders
                .lock()
                .unwrap()
                .entry(txid)
                .and_modify(|senders| {
                    senders.push(nop_sender.clone());
                })
                .or_insert(vec![nop_sender]);
        }
        println!("after commit ro txn cb....");
    }

    pub fn rollback_ro_txn(&self, txid: u64, cb: TxCallback) {
        if *self.txn_count.lock().unwrap().get(&txid).unwrap() == 1 {
            // give back no op sender
            self.no_op_senders
                .lock()
                .unwrap()
                .get(&txid)
                .unwrap()
                .iter()
                .for_each(|s| {
                    NO_OP_POOL.lock().unwrap().push(s.clone());
                });
            self.no_op_senders.lock().unwrap().remove(&txid);
            self.tab_iters.lock().unwrap().remove(&txid);
        } else {
            let nop_sender = NO_OP_POOL.lock().unwrap().pop().unwrap();
            let _ = nop_sender.send(NopMessage::Nop(cb));
            self.no_op_senders
                .lock()
                .unwrap()
                .entry(txid)
                .and_modify(|senders| {
                    senders.push(nop_sender.clone());
                })
                .or_insert(vec![nop_sender]);
        }
    }
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

lazy_static! {
    pub static ref NO_OP_POOL: Arc<Mutex<NopPool>> = Arc::new(Mutex::new(NopPool::new()));

    // all opened dbs in this env
    static ref OPENED_TABLES: Arc<Mutex<HashMap<u64, Database>>> = Arc::new(Mutex::new(HashMap::new()));

    pub static ref DB_TAB_READ_POOL: Arc<Mutex<DbTabReadPool>> = Arc::new(Mutex::new(DbTabReadPool::new()));
    pub static ref DB_TAB_WRITE: Arc<Mutex<DbTabWrite>> = Arc::new(Mutex::new(DbTabWrite::new()));
}
