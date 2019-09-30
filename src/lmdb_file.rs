use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::Ordering;
use std::thread;

use worker::impls::cast_store_task;
use worker::task::TaskType;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use atom::Atom;
use apm::counter::{GLOBAL_PREF_COLLECT, PrefCounter};
use bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use guid::Guid;
use pi_db::db::{Bin, CommitResult, DBResult, Filter, Iter, IterResult, KeyIterResult, MetaTxn, NextResult,OpenTab, SResult, Tab, TabKV, TabMeta, TabTxn, TxCallback, TxQueryCallback, TxState, Txn, Ware,WareSnapshot};
use sinfo::EnumType;
use pi_db::tabs::{TabLog, Tabs};
use pi_db::db::Event;
use pi_db::mgr::{COMMIT_CHAN, CommitChan};
use lmdb::{ Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use pool::{LmdbService, ReaderMsg, WriterMsg, OPENED_TABLES, IN_PROGRESS_TX};

const SINFO: &str = "_$sinfo";
const MAX_DBS_PER_ENV: u32 = 1024;
const TIMEOUT: usize = 100;

//LMDB库前缀
const LMDB_WARE_PREFIX: &'static str = "lmdb_ware_";
//LMDB表前缀
const LMDB_TABLE_PREFIX: &'static str = "lmdb_table_";
//LMDB表事务创建数量后缀
const LMDB_TABLE_TRANS_COUNT_SUFFIX: &'static str = "_trans_count";
//LMDB表事务预提交数量后缀
const LMDB_TABLE_PREPARE_COUNT_SUFFIX: &'static str = "_prepare_count";
//LMDB表事务提交数量后缀
const LMDB_TABLE_COMMIT_COUNT_SUFFIX: &'static str = "_commit_count";
//LMDB表事务回滚数量后缀
const LMDB_TABLE_ROLLBACK_COUNT_SUFFIX: &'static str = "_rollback_count";
//LMDB表读记录数量后缀
const LMDB_TABLE_READ_COUNT_SUFFIX: &'static str = "_read_count";
//LMDB表读记录字节数量后缀
const LMDB_TABLE_READ_BYTE_COUNT_SUFFIX: &'static str = "_read_byte_count";
//LMDB表写记录数量后缀
const LMDB_TABLE_WRITE_COUNT_SUFFIX: &'static str = "_write_count";
//LMDB表写记录字节数量后缀
const LMDB_TABLE_WRITE_BYTE_COUNT_SUFFIX: &'static str = "_write_byte_count";
//LMDB表删除记录数量后缀
const LMDB_TABLE_REMOVE_COUNT_SUFFIX: &'static str = "_remove_count";
//LMDB表删除记录字节数量后缀
const LMDB_TABLE_REMOVE_BYTE_COUNT_SUFFIX: &'static str = "_remove_byte_count";
//LMDB表关键字迭代数量后缀
const LMDB_TABLE_KEY_ITER_COUNT_SUFFIX: &'static str = "_key_iter_count";
//LMDB表关键字迭代字节数量后缀
const LMDB_TABLE_KEY_ITER_BYTE_COUNT_SUFFIX: &'static str = "_key_iter_byte_count";
//LMDB表迭代数量后缀
const LMDB_TABLE_ITER_COUNT_SUFFIX: &'static str = "_iter_count";
//LMDB表关键字迭代字节数量后缀
const LMDB_TABLE_ITER_BYTE_COUNT_SUFFIX: &'static str = "_iter_byte_count";

lazy_static! {
	//LMDB库创建数量
	static ref LMDB_WARE_CREATE_COUNT: PrefCounter = GLOBAL_PREF_COLLECT.new_static_counter(Atom::from("lmdb_ware_create_count"), 0).unwrap();
	//LMDB表创建数量
	static ref LMDB_TABLE_CREATE_COUNT: PrefCounter = GLOBAL_PREF_COLLECT.new_static_counter(Atom::from("lmdb_table_create_count"), 0).unwrap();
}

#[derive(Debug, Clone)]
pub struct LmdbTable {
    name: Atom,
    env_flags: EnvironmentFlags,
    write_flags: WriteFlags,
    db_flags: DatabaseFlags,
    trans_count:	PrefCounter,	//事务计数
}

impl Tab for LmdbTable {
    fn new(db_name: &Atom) -> Self {
        LMDB_TABLE_CREATE_COUNT.sum(1);
        LmdbTable {
            name: db_name.clone(),
            env_flags: EnvironmentFlags::empty(),
            write_flags: WriteFlags::empty(),
            db_flags: DatabaseFlags::empty(),
            trans_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + db_name + LMDB_TABLE_TRANS_COUNT_SUFFIX), 0).unwrap(),
        }
    }

    fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn> {
        info!("create new txid: {:?}, tab: {:?}, writable: {:?}", id.time(), self.name, writable);
        self.trans_count.sum(1);

        let tab = &self.name;
        let t = Arc::new(LmdbTableTxn {
            id: id.time(),
            tab: tab.clone(),
            writable: writable,
            state: Arc::new(Mutex::new(TxState::Ok)),            
            prepare_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_PREPARE_COUNT_SUFFIX), 0).unwrap(),
            commit_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_COMMIT_COUNT_SUFFIX), 0).unwrap(),
            rollback_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_ROLLBACK_COUNT_SUFFIX), 0).unwrap(),
            read_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_READ_COUNT_SUFFIX), 0).unwrap(),
            read_byte: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_READ_BYTE_COUNT_SUFFIX), 0).unwrap(),
            write_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_WRITE_COUNT_SUFFIX), 0).unwrap(),
            write_byte: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_WRITE_BYTE_COUNT_SUFFIX), 0).unwrap(),
            remove_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_REMOVE_COUNT_SUFFIX), 0).unwrap(),
            remove_byte: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + tab + LMDB_TABLE_REMOVE_BYTE_COUNT_SUFFIX), 0).unwrap(),
        });

        t
    }
}

pub struct LmdbTableTxn {
    id: u64,
    tab: Atom,
    writable: bool,
    state: Arc<Mutex<TxState>>,
    prepare_count:	PrefCounter,	//预提交计数
    commit_count:	PrefCounter,	//提交计数
    rollback_count:	PrefCounter,	//回滚计数
    read_count:		PrefCounter,	//读计数
    read_byte:		PrefCounter,	//读字节
    write_count:	PrefCounter,	//写计数
    write_byte:		PrefCounter,	//写字节
    remove_count:	PrefCounter,	//删除计数
    remove_byte:	PrefCounter,	//删除字节
}

impl Txn for LmdbTableTxn {
    fn get_state(&self) -> TxState {
        if let Ok(state) = self.state.lock() {
            state.clone()
        } else {
            TxState::Err
        }
    }

    fn prepare(&self, _timeout: usize, _cb: TxCallback) -> DBResult {
        *self.state.lock().unwrap() = TxState::Preparing;

        self.prepare_count.sum(1);

        Some(Ok(()))
    }

    fn commit(&self, cb: TxCallback) -> CommitResult {
        self.commit_count.sum(1);

        *self.state.lock().unwrap() = TxState::Committing;
        let state1 = self.state.clone();

        let ro_sender = LMDB_SERVICE
                        .lock()
                        .unwrap()
                        .ro_sender(&self.tab.clone())
                        .expect(&format!("Fatal error: cannot get ro sender for {:?}", &self.tab.to_string()));

        let _ = ro_sender.send(ReaderMsg::Commit(Arc::new(move |c| match c {
            Ok(_) => {
                *state1.lock().unwrap() = TxState::Commited;
                cb(Ok(()));
            }
            Err(e) => {
                *state1.lock().unwrap() = TxState::CommitFail;
                cb(Err(e.to_string()));
            }
        })));

        None
    }

    fn rollback(&self, cb: TxCallback) -> DBResult {
        self.rollback_count.sum(1);

        *self.state.lock().unwrap() = TxState::Rollbacking;
        let state1 = self.state.clone();
        let ro_sender = LMDB_SERVICE
                        .lock()
                        .unwrap()
                        .ro_sender(&self.tab)
                        .expect(&format!("Fatal error: cannot get ro sender for {:?}", &self.tab.to_string()));

        // 删除未提交的修改
        match MODS.lock().unwrap().remove(&self.id) {
            Some(m) => info!("rollback txid: {:?}, modifies: {:?}", self.id, m),
            None => {}
        }

        // 不管是读写事务还是只读事务，直接回滚，调用上层回调
        let _ = ro_sender.send(ReaderMsg::Rollback(Arc::new(move |c| match c {
            Ok(_) => {
                *state1.lock().unwrap() = TxState::Rollbacked;
                cb(Ok(()));
            }
            Err(e) => {
                *state1.lock().unwrap() = TxState::RollbackFail;
                cb(Err(e.to_string()));
            }
        })));

        None
    }
}

impl TabTxn for LmdbTableTxn {
    fn key_lock(
        &self,
        _arr: Arc<Vec<TabKV>>,
        _lock_time: usize,
        _readonly: bool,
        _cb: TxCallback,
    ) -> DBResult {
        None
    }

    fn query(
        &self,
        arr: Arc<Vec<TabKV>>,
        _lock_time: Option<usize>,
        _readonly: bool,
        cb: TxQueryCallback,
    ) -> Option<SResult<Vec<TabKV>>> {
        info!("query txid: {:?}, query item: {:?}", self.id, arr);
        let read_byte = self.read_byte.clone();
        match self.writable {
            true => {
                let rw_sender = LMDB_SERVICE.lock().unwrap().rw_sender().unwrap();
                let mut retry = 250;
                let cb1= cb.clone();
                while retry > 0 {
                    if IN_PROGRESS_TX.load(Ordering::SeqCst) == self.id || IN_PROGRESS_TX.compare_and_swap(0, self.id, Ordering::SeqCst) == 0 {
                        let _ = rw_sender.send(WriterMsg::Query(
                            arr.clone(),
                            Arc::new(move |q| match q {
                                Ok(v) => {
                                    read_byte.sum(v.len());

                                    cb(Ok(v))
                                },
                                Err(e) => cb(Err(e.to_string())),
                            }),
                        ));
                        break;
                    }
                    thread::sleep_ms(20);
                    retry -= 1;
                }

                if retry == 0 {
                    let t = Box::new(move |_| {
                        cb1(Err("query timeout".to_string()));
                    });
                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("query timeout callback"));
                }
            }

            false => {
                let sender = LMDB_SERVICE.lock().unwrap().ro_sender(&self.tab).unwrap();
                let _ = sender.send(ReaderMsg::Query(
                    arr,
                    Arc::new(move |q| match q {
                        Ok(v) => {
                            read_byte.sum(v.len());

                            cb(Ok(v))
                        },
                        Err(e) => cb(Err(e.to_string())),
                    }),
                ));
            }
        }

        self.read_count.sum(1);

        None
    }

    fn modify(
        &self,
        arr: Arc<Vec<TabKV>>,
        _lock_time: Option<usize>,
        _readonly: bool,
        cb: TxCallback,
    ) -> DBResult {
        info!("MODIFY: txid: {:?}, tab: {:?}, len: {:?}", self.id, self.tab, arr);

        let sender = LMDB_SERVICE.lock().unwrap().rw_sender().unwrap();
        let _ = sender.send(WriterMsg::Modify(Arc::new(move |m| match m {
            Ok(_) => cb(Ok(())),
            Err(e) => cb(Err(e.to_string())),
        })));

        let data = arr.iter().cloned().collect::<Vec<TabKV>>();

        let write_count = self.write_count.clone();
        let write_byte = self.write_byte.clone();
        let remove_count = self.remove_count.clone();
        let remove_byte = self.remove_byte.clone();
        for kv in data.iter() {
            if let &Some(ref v) = &kv.value {
                //插入或更新
                write_byte.sum(kv.key.len() + v.len());
                write_count.sum(1);
            } else {
                //移除
                remove_byte.sum(kv.key.len());
                remove_count.sum(1);
            }
        }

        MODS.lock().unwrap()
            .entry(self.id)
            .and_modify(|v| {
                v.extend(data.iter().cloned());
            })
            .or_insert(data);

        None
    }

    fn iter(
        &self,
        tab: &Atom,
        key: Option<Bin>,
        descending: bool,
        filter: Filter,
        cb: Arc<Fn(IterResult)>,
    ) -> Option<IterResult> {
        info!("create iter for txid: {:?}, tab: {:?}, key: {:?}, descending: {:?}", self.id, self.tab, key, descending);
        let (tx, rx) = bounded(1);
        match self.writable {
            true => {
                let rw_sender = LMDB_SERVICE.lock().unwrap().rw_sender().unwrap();
                let mut retry = 250;
                while retry > 0 {
                    if IN_PROGRESS_TX.load(Ordering::SeqCst) == self.id || IN_PROGRESS_TX.compare_and_swap(0, self.id, Ordering::SeqCst) == 0 {
                        let _ = rw_sender.send(WriterMsg::CreateItemIter(
                            descending,
                            tab.clone(),
                            key.clone(),
                            tx.clone(),
                        ));
                        break;
                    }
                    thread::sleep_ms(20);
                    retry -= 1;
                }

                if retry == 0 {
                    let t = Box::new(move |_| {
                        cb(Err("create iter timeout".to_string()));
                    });
                    cast_store_task(TaskType::Async(false), 100, None, t, Atom::from("create iter timeout callback"));
                }
            }

            false => {
                let ro_sender = LMDB_SERVICE.lock().unwrap().ro_sender(&tab).unwrap();
                let _ = ro_sender.send(ReaderMsg::CreateItemIter(
                    descending,
                    tab.clone(),
                    key.clone(),
                    tx.clone(),
                ));
            }
        }

        match rx.recv() {
            Ok(k) => {
                return Some(Ok(Box::new(LmdbItemsIter::new(
                    self.id,
                    descending,
                    tab.clone(),
                    k,
                    tx,
                    rx,
                    filter,
                ))));
            }
            Err(e) => {
                panic!("Create db item iter in pool failed: {:?}", e.to_string());
            }
        }
    }

    fn key_iter(
        &self,
        key: Option<Bin>,
        descending: bool,
        filter: Filter,
        _cb: Arc<Fn(KeyIterResult)>,
    ) -> Option<KeyIterResult> {
        None
    }

    fn index(
        &self,
        _tab: &Atom,
        _index_key: &Atom,
        _key: Option<Bin>,
        _descending: bool,
        _filter: Filter,
        _cb: Arc<Fn(IterResult)>,
    ) -> Option<IterResult> {
        None
    }

    fn tab_size(&self, cb: Arc<Fn(SResult<usize>)>) -> Option<SResult<usize>> {
        None
    }
}

/// lmdb iterator that navigate key and value
pub struct LmdbItemsIter {
    txid: u64,
    desc: bool,
    tab: Atom,
    cur_key: Option<Bin>,
    sender: Sender<Option<Bin>>,
    receiver: Receiver<Option<Bin>>,
    _filter: Filter,
    iter_count:		PrefCounter,	//迭代计数
    iter_byte:		PrefCounter,	//迭代字节
}

impl LmdbItemsIter {
    pub fn new(
        txid: u64,
        desc: bool,
        tab: Atom,
        cur_key: Option<Bin>,
        sender: Sender<Option<Bin>>,
        receiver: Receiver<Option<Bin>>,
        _filter: Filter,
    ) -> Self {
        LmdbItemsIter {
            txid,
            desc,
            tab: tab.clone(),
            cur_key,
            sender,
            receiver,
            _filter,
            iter_count: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + &tab + LMDB_TABLE_ITER_COUNT_SUFFIX), 0).unwrap(),
            iter_byte: GLOBAL_PREF_COLLECT.
                new_dynamic_counter(
                    Atom::from(LMDB_TABLE_PREFIX.to_string() + &tab + LMDB_TABLE_ITER_BYTE_COUNT_SUFFIX), 0).unwrap(),
        }
    }
}

impl Iter for LmdbItemsIter {
    type Item = (Bin, Bin);

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        self.iter_count.sum(1);

        info!("next item: txid: {:?}, tab: {:?}, cur_key: {:?}, descending: {:?}", self.txid, self.tab, self.cur_key, self.desc);

        if self.cur_key.is_none() {
            cb(Ok(None))
        } else {
            let sender = LMDB_SERVICE.lock().unwrap().ro_sender(&self.tab).unwrap();

            let iter_byte = self.iter_byte.clone();

            let _ = sender.send(ReaderMsg::NextItem(
                self.desc,
                self.tab.clone(),
                self.cur_key.clone(),
                Arc::new(move |item| match item {
                    Ok(Some(v)) => {
                        iter_byte.sum(v.0.len() + v.1.len());

                        cb(Ok(Some(v)));
                    }
                    Ok(None) => {
                        cb(Ok(None));
                    }
                    Err(e) => {
                        cb(Err(e.to_string()));
                    }
                }),
                self.sender.clone(),
            ));

            match self.receiver.recv() {
                Ok(v) => self.cur_key = v,
                Err(_) => (),
            }
        }

        None
    }
}

#[derive(Clone)]
pub struct LmdbMetaTxn(Arc<TabTxn>);

impl LmdbMetaTxn {
    //tab_txn 必须是Arc<FileTabTxn>
    fn new(tab_txn: Arc<TabTxn>) -> LmdbMetaTxn {
        LmdbMetaTxn(tab_txn)
    }
}

fn create_table_in_lmdb(tab: &Atom) {
    let env = LMDB_SERVICE.lock().unwrap().get_env();
    OPENED_TABLES
        .write()
        .unwrap()
        .entry(tab.get_hash() as u64)
        .or_insert_with(|| {
            env
            .as_ref()
            .create_db(Some(tab.as_str()), DatabaseFlags::empty())
            .expect("Fatal error: open table failed")
        });
}

impl MetaTxn for LmdbMetaTxn {
    // 创建表、修改指定表的元数据
    fn alter(&self, tab: &Atom, meta: Option<Arc<TabMeta>>, cb: TxCallback) -> DBResult {
        info!("META TXN: alter tab: {:?}", tab);
        create_table_in_lmdb(tab);
        let mut key = WriteBuffer::new();
        tab.encode(&mut key);
        let key = Arc::new(key.unwrap());

        let value = match meta {
            Some(v) => {
                let mut value = WriteBuffer::new();
                v.encode(&mut value);
                Some(Arc::new(value.unwrap()))
            }
            None => None,
        };

        let tabkv = TabKV {
            ware: Atom::from("file"), // hard code
            tab: Atom::from(SINFO), // 元信息写入 SINFO表中
            key: key,
            index: 0,
            value: value,
        };

        self.0.modify(Arc::new(vec![tabkv]), None, false, cb)
    }

    // 快照拷贝表
    fn snapshot(&self, _tab: &Atom, _from: &Atom, _cb: TxCallback) -> DBResult {
        Some(Ok(()))
    }
    // 修改指定表的名字
    fn rename(&self, _tab: &Atom, _new_name: &Atom, _cb: TxCallback) -> DBResult {
        Some(Ok(()))
    }
}

impl Txn for LmdbMetaTxn {
    // 获得事务的状态
    fn get_state(&self) -> TxState {
        self.0.get_state()
    }
    // 预提交一个事务
    fn prepare(&self, timeout: usize, cb: TxCallback) -> DBResult {
        self.0.prepare(timeout, cb)
    }
    // 提交一个事务
    fn commit(&self, cb: TxCallback) -> CommitResult {
        self.0.commit(cb)
    }
    // 回滚一个事务
    fn rollback(&self, cb: TxCallback) -> DBResult {
        self.0.rollback(cb)
    }
}

/**
* Lmdb数据库
*/
#[derive(Clone)]
pub struct DB {
    name: Atom,
    tabs: Arc<RwLock<Tabs<LmdbTable>>>,
}

impl DB {
    /**
    * 构建Lmdb数据库
    * @param name 数据库路径
    * @param db_size 数据库文件的最大大小
    * @returns 返回Lmdb数据库，失败返回原因描述
    */
    pub fn new(name: Atom, db_size: usize) -> Result<Self, String> {
        info!("create new db: {:?}, db_size: {:?}", name, db_size);
        if !Path::new(&name.to_string()).exists() {
            let _ = fs::create_dir(name.to_string());
        }

        if db_size < 1024 * 1024 {
            return Err("DB size must greater than 1M".to_string());
        }

        let env = Arc::new(
            Environment::new()
                .set_max_dbs(MAX_DBS_PER_ENV)
                .set_map_size(db_size)
                .set_flags(EnvironmentFlags::NO_TLS)
                .open(Path::new(&name.to_string()))
                .map_err(|e| e.to_string())?,
        );

        // retrive meta table info of a DB
        let db = match env.open_db(Some(SINFO)) {
            Ok(db) => db,
            Err(_) => env.create_db(Some(SINFO), DatabaseFlags::empty()).expect("Failed to open db to retrive meta table"),
        };

        OPENED_TABLES.write().unwrap().insert(Atom::from(SINFO.to_string()).get_hash() as u64, db);

        let txn = env.begin_ro_txn().map_err(|e| e.to_string())?;
        let mut cursor = txn.open_ro_cursor(db).map_err(|e| e.to_string())?;

        let mut tabs: Tabs<LmdbTable> = Tabs::new();

        LMDB_SERVICE.lock().unwrap().set_env(env.clone());
        LMDB_SERVICE.lock().unwrap().start();

        let rw_sender = LMDB_SERVICE.lock().unwrap().rw_sender().unwrap();

        let _ = thread::spawn(move || {
            info!("start thread serving for the finally commit");
            loop {
                match COMMIT_CHAN.1.recv() {
                    Ok(CommitChan(txid, sndr)) => {
                        info!("receive commit notification for txid: {:?} ", txid.time());
                        match MODS.lock().unwrap().remove(&txid.time()) {
                            Some(v) => {
                                info!("modifications to be committed: {:?}", v);
                                let _ = rw_sender.send(WriterMsg::Commit(Arc::new(v.clone()), Arc::new(move |c| match c {
                                    Ok(_) => {
                                        info!("txid: {:?} finnaly committed", txid.time());
                                        let _ = sndr.send(Arc::new(v.clone()));
                                    }
                                    Err(e) =>{
                                        warn!("txid: {:?} commit failed {:?}", txid.time(), e);
                                    }
                                })));
                            }
                            None => {
                                let _ = rw_sender.send(WriterMsg::Commit(Arc::new(vec![]), Arc::new(move |c| match c {
                                    Ok(_) => {
                                        info!("non write txid: {:?} finnaly committed", txid.time());
                                        let _ = sndr.send(Arc::new(vec![]));
                                    }
                                    Err(e) =>{
                                        warn!("non write txid: {:?} commit failed {:?}", txid.time(), e);
                                    }
                                })));
                            }
                        }
                    }
                    Err(_) => {}
                }
            }
        });

        for kv in cursor.iter() {
            tabs.set_tab_meta(
                Atom::decode(&mut ReadBuffer::new(kv.0, 0)).unwrap(),
                Arc::new(TabMeta::decode(&mut ReadBuffer::new(kv.1, 0)).unwrap()),
            );
        }

        tabs.set_tab_meta(
            Atom::from(SINFO),
            Arc::new(TabMeta::new(EnumType::Str, EnumType::Bool)),
        );

        std::mem::drop(cursor);
        let _ = txn.commit().unwrap();

        LMDB_WARE_CREATE_COUNT.sum(1);

        Ok(DB {
            name: name,
            tabs: Arc::new(RwLock::new(tabs)),
        })
    }
}

impl OpenTab for DB {
    // 打开指定的表，表必须有meta
    fn open<'a, T: Tab>(&self, tab: &Atom, _cb: Box<Fn(SResult<T>) + 'a>) -> Option<SResult<T>> {
        Some(Ok(T::new(tab)))
    }
}

impl Ware for DB {
    // 拷贝全部的表
    fn tabs_clone(&self) -> Arc<Ware> {
        Arc::new(DB {
            name: self.name.clone(),
            tabs: Arc::new(RwLock::new(self.tabs.read().unwrap().clone_map())),
        })
    }
    // 列出全部的表
    fn list(&self) -> Box<Iterator<Item = Atom>> {
        Box::new(self.tabs.read().unwrap().list())
    }
    // 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
    fn timeout(&self) -> usize {
        TIMEOUT
    }
    // 表的元信息
    fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
        self.tabs.read().unwrap().get(tab_name)
    }
    // 获取当前表结构快照
    fn snapshot(&self) -> Arc<WareSnapshot> {
        Arc::new(LmdbSnapshot(
            self.clone(),
            RefCell::new(self.tabs.read().unwrap().snapshot()),
        ))
    }
}

/// LMDB manager
pub struct LmdbSnapshot(DB, RefCell<TabLog<LmdbTable>>);

impl WareSnapshot for LmdbSnapshot {
    // 列出全部的表
    fn list(&self) -> Box<Iterator<Item = Atom>> {
        Box::new(self.1.borrow().list())
    }
    // 表的元信息
    fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
        self.1.borrow().get(tab_name)
    }
    // 检查该表是否可以创建
    fn check(&self, _tab: &Atom, _meta: &Option<Arc<TabMeta>>) -> SResult<()> {
        Ok(())
    }
    // 新增 修改 删除 表
    fn alter(&self, tab_name: &Atom, meta: Option<Arc<TabMeta>>) {
        self.1.borrow_mut().alter(tab_name, meta)
    }
    // 创建指定表的表事务
    fn tab_txn(
        &self,
        tab_name: &Atom,
        id: &Guid,
        writable: bool,
        cb: Box<Fn(SResult<Arc<TabTxn>>)>,
    ) -> Option<SResult<Arc<TabTxn>>> {
        self.1.borrow().build(&self.0, tab_name, id, writable, cb)
    }
    // 创建一个meta事务
    fn meta_txn(&self, id: &Guid) -> Arc<MetaTxn> {
        Arc::new(LmdbMetaTxn::new(
            self.tab_txn(&Atom::from(SINFO), id, true, Box::new(|_r| {}))
                .unwrap()
                .expect("meta_txn"),
        ))
    }
    // 元信息预提交
    fn prepare(&self, id: &Guid) -> SResult<()> {
        (self.0)
            .tabs
            .write()
            .unwrap()
            .prepare(id, &mut self.1.borrow_mut())
    }
    // 元信息提交
    fn commit(&self, id: &Guid) {
        (self.0).tabs.write().unwrap().commit(id)
    }
    // 回滚
    fn rollback(&self, id: &Guid) {
        (self.0).tabs.write().unwrap().rollback(id)
    }

    fn notify(&self, evt: Event) {}
}

lazy_static! {
    static ref LMDB_SERVICE: Arc<Mutex<LmdbService>> = Arc::new(Mutex::new(LmdbService::new(17)));
    static ref MODS: Arc<Mutex<HashMap<u64, Vec<TabKV>>>> = Arc::new(Mutex::new(HashMap::new()));
}
