use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use atom::Atom;
use bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use guid::Guid;
use pi_db::db::{Bin, CommitResult, DBResult, Filter, Iter, IterResult, KeyIterResult, MetaTxn, NextResult,OpenTab, SResult, Tab, TabKV, TabMeta, TabTxn, TxCallback, TxQueryCallback, TxState, Txn, Ware,WareSnapshot};
use sinfo::EnumType;
use pi_db::tabs::{TabLog, Tabs};
use pi_db::mgr::{COMMIT_CHAN, CommitChan};
use lmdb::{ Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use pool::{LmdbService, ReaderMsg, WriterMsg};

const SINFO: &str = "_$sinfo";
const MAX_DBS_PER_ENV: u32 = 1024;
const TIMEOUT: usize = 100;

#[derive(Debug, Clone)]
pub struct LmdbTable {
    name: Atom,
    env_flags: EnvironmentFlags,
    write_flags: WriteFlags,
    db_flags: DatabaseFlags,
}

impl Tab for LmdbTable {
    fn new(db_name: &Atom) -> Self {
        LMDB_SERVICE.lock().unwrap().create_tab(db_name);

        LmdbTable {
            name: db_name.clone(),
            env_flags: EnvironmentFlags::empty(),
            write_flags: WriteFlags::empty(),
            db_flags: DatabaseFlags::empty(),
        }
    }

    fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn> {
        info!("create new txid: {:?}, tab: {:?}, writable: {:?}", id.time(), self.name, writable);
        let t = Arc::new(LmdbTableTxn {
            tab: self.name.clone(),
            id: id.time(),
            writable: writable,
            state: Arc::new(Mutex::new(TxState::Ok)),
        });

        t
    }
}

pub struct LmdbTableTxn {
    tab: Atom,
    id: u64,
    writable: bool,
    state: Arc<Mutex<TxState>>,
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
        Some(Ok(()))
    }

    fn commit(&self, cb: TxCallback) -> CommitResult {
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
        let sender = LMDB_SERVICE.lock().unwrap().ro_sender(&self.tab).unwrap();
        let _ = sender.send(ReaderMsg::Query(
            arr,
            Arc::new(move |q| match q {
                Ok(v) => cb(Ok(v)),
                Err(e) => cb(Err(e.to_string())),
            }),
        ));

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
        _cb: Arc<Fn(IterResult)>,
    ) -> Option<IterResult> {
        info!("create iter for txid: {:?}, tab: {:?}, key: {:?}, descending: {:?}", self.id, self.tab, key, descending);

        let (tx, rx) = bounded(1);
        let sender = LMDB_SERVICE.lock().unwrap().ro_sender(&tab).unwrap();
        let _ = sender.send(ReaderMsg::CreateItemIter(
            descending,
            tab.clone(),
            key.clone(),
            tx.clone(),
        ));

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
            tab,
            cur_key,
            sender,
            receiver,
            _filter,
        }
    }
}

impl Iter for LmdbItemsIter {
    type Item = (Bin, Bin);

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        info!("next item: txid: {:?}, tab: {:?}, cur_key: {:?}, descending: {:?}", self.txid, self.tab, self.cur_key, self.desc);

        if self.cur_key.is_none() {
            cb(Ok(None))
        } else {
            let sender = LMDB_SERVICE.lock().unwrap().ro_sender(&self.tab).unwrap();
            let _ = sender.send(ReaderMsg::NextItem(
                self.desc,
                self.tab.clone(),
                self.cur_key.clone(),
                Arc::new(move |item| match item {
                    Ok(Some(v)) => {
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

impl MetaTxn for LmdbMetaTxn {
    // 创建表、修改指定表的元数据
    fn alter(&self, tab: &Atom, meta: Option<Arc<TabMeta>>, cb: TxCallback) -> DBResult {
        info!("META TXN: alter tab: {:?}", tab);
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

#[derive(Clone)]
pub struct DB {
    name: Atom,
    tabs: Arc<RwLock<Tabs<LmdbTable>>>,
}

impl DB {
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
                    Ok(CommitChan(txid, cb, sndr)) => {
                        info!("receive commit notification for txid: {:?} ", txid.time());
                        match MODS.lock().unwrap().remove(&txid.time()) {
                            Some(v) => {
                                info!("modifications to be committed: {:?}", v);
                                let _ = rw_sender.send(WriterMsg::Commit(Arc::new(v.clone()), Arc::new(move |c| match c {
                                    Ok(_) => {
                                        cb(Ok(()));
                                        info!("txid: {:?} finnaly committed", txid.time());
                                        sndr.send(Arc::new(v.clone()));
                                    }
                                    Err(e) =>{
                                        cb(Err(e.to_string()));
                                        warn!("txid: {:?} commit failed", txid.time());
                                    }
                                })));
                            }

                            None => cb(Ok(())),
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
}

lazy_static! {
    static ref LMDB_SERVICE: Arc<Mutex<LmdbService>> = Arc::new(Mutex::new(LmdbService::new(17)));
    static ref MODS: Arc<Mutex<HashMap<u64, Vec<TabKV>>>> = Arc::new(Mutex::new(HashMap::new()));
}
