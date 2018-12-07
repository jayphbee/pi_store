use std::boxed::FnBox;
use std::cell::RefCell;
use std::fs;
use std::mem::replace;
use std::path::Path;
use std::rc::Rc;
use std::slice::from_raw_parts;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};

use fnv::FnvHashMap;

use pi_base::pi_base_impl::STORE_TASK_POOL;
use pi_base::task::TaskType;

use pi_db::db::{
    Bin, CommitResult, DBResult, Filter, Iter, IterResult, KeyIterResult, MetaTxn, NextResult,
    OpenTab, RwLog, SResult, Tab, TabKV, TabMeta, TabTxn, TxCallback, TxQueryCallback, TxState,
    Txn, Ware, WareSnapshot,
};
use pi_lib::atom::Atom;
use pi_lib::bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use pi_lib::guid::Guid;
use pi_lib::sinfo::EnumType;

use pi_db::tabs::{TabLog, Tabs};

use lmdb::{
    mdb_set_compare, Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Error,
    MDB_cmp_func, MDB_dbi, MDB_txn, MDB_val, RoCursor, RoTransaction, RwCursor, RwTransaction,
    Transaction, WriteFlags,
};

use pool::{LmdbMessage, THREAD_POOL};

const ASYNC_DB_TYPE: TaskType = TaskType::Sync;

const DB_ROOT: &str = "_$lmdb";
const SINFO: &str = "_$sinfo";
const MAX_DBS_PER_ENV: u32 = 1024;

pub const MDB_SET: u32 = 15;
pub const MDB_PREV: u32 = 12;
pub const MDB_NEXT: u32 = 8;
pub const MDB_FIRST: u32 = 0;
pub const MDB_LAST: u32 = 6;

const TIMEOUT: usize = 100;

#[derive(Debug)]
pub struct LmdbTableWrapper {
    env: Arc<Environment>,
    db: Database,
    name: Atom,
    // TODO: add methods to set/get these flags
    env_flags: EnvironmentFlags,
    write_flags: WriteFlags,
    db_flags: DatabaseFlags,
}

#[derive(Debug, Clone)]
pub struct LmdbTable(Arc<LmdbTableWrapper>);

impl Tab for LmdbTable {
    fn new(db_name: &Atom) -> Self {
        if !Path::new(&db_name.to_string()).exists() {
            fs::create_dir(DB_ROOT);
        }

        let env = Arc::new(
            Environment::new()
                // see doc: https://docs.rs/lmdb/0.8.0/lmdb/struct.EnvironmentFlags.html#associatedconstant.NO_TLS
                .set_flags(EnvironmentFlags::NO_TLS)
                .set_max_dbs(MAX_DBS_PER_ENV)
                .open(Path::new(DB_ROOT))
                .expect("Open lmdb environment failed"),
        );
        let db = env
            .create_db(Some(&db_name.to_string()), DatabaseFlags::empty())
            .expect("Open lmdb database failed");

        LmdbTable(Arc::new(LmdbTableWrapper {
            env: env,
            db: db,
            name: db_name.clone(),
            env_flags: EnvironmentFlags::empty(),
            write_flags: WriteFlags::empty(),
            db_flags: DatabaseFlags::empty(),
        }))
    }

    fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn> {
        let sender = match THREAD_POOL.clone().lock() {
            Ok(mut pool) => pool.pop(),
            Err(e) => panic!("get sender error when creating transaction: {:?}", e.to_string())
        };

        if let Some(ref tx) = sender {
            println!("THREAD_POOL full, can't get one from it");
        }

        Arc::new(LmdbTableTxn(Arc::new(RefCell::new(LmdbTableTxnWrapper {
            tab: self.clone(),
            id: id.clone(),
            _writable: writable,
            state: TxState::Ok,
            sender,
        }))))
    }
}

struct LmdbTableTxnWrapper {
    tab: LmdbTable,
    id: Guid,
    _writable: bool,
    state: TxState,
    sender: Option<Sender<LmdbMessage>>,
}

impl Drop for LmdbTableTxnWrapper {
    fn drop(&mut self) {
        match THREAD_POOL.clone().lock() {
            Ok(mut pool) => pool.push(self.sender.take().unwrap()),
            Err(e) => panic!("Give back sender failed: {:?}", e.to_string())
        };
    }
}

#[derive(Clone)]
pub struct LmdbTableTxn(Arc<RefCell<LmdbTableTxnWrapper>>);

impl Txn for LmdbTableTxn {
    fn get_state(&self) -> TxState {
        self.0.borrow().state.clone()
    }

    fn prepare(&self, _timeout: usize, _cb: TxCallback) -> DBResult {
        self.0.borrow_mut().state = TxState::Preparing;
        _cb(Ok(()));
        Some(Ok(()))
    }

    fn commit(&self, cb: TxCallback) -> CommitResult {
        Arc::clone(&self.0).borrow_mut().state = TxState::Committing;
        let lmdb_table_txn = self.0.clone();
        let tab_name = self.0.clone().borrow().tab.0.clone().name.to_string();

        match self.0.clone().borrow().sender.clone() {
            Some(tx) => {
                tx.send(LmdbMessage::Commit(
                    tab_name,
                    Arc::new(move |c| match c {
                        Ok(_) => {
                            lmdb_table_txn.borrow_mut().state = TxState::Commited;
                            cb(Ok(()));
                        }
                        Err(e) => {
                            lmdb_table_txn.borrow_mut().state = TxState::CommitFail;
                            cb(Err(e.to_string()));
                        }
                    }),
                ));
            }
            None => cb(Err("Can't get sender".to_string())),
        }
        None
    }

    fn rollback(&self, cb: TxCallback) -> DBResult {
        let lmdb_table_txn = self.0.clone();
        let tab_name = self.0.clone().borrow().tab.0.clone().name.to_string();

        match self.0.clone().borrow().sender.clone() {
            Some(tx) => {
                tx.send(LmdbMessage::Rollback(
                    tab_name,
                    Arc::new(move |r| match r {
                        Ok(_) => {
                            lmdb_table_txn.borrow_mut().state = TxState::Rollbacked;
                            cb(Ok(()));
                        }
                        Err(e) => {
                            lmdb_table_txn.borrow_mut().state = TxState::RollbackFail;
                            cb(Err(e.to_string()));
                        }
                    }),
                ));
            }
            None => {
                cb(Err("Can't get sender".to_string()));
            }
        }

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
        let tab_name = self.0.clone().borrow().tab.0.clone().name.to_string();

        match self.0.clone().borrow().sender.clone() {
            Some(tx) => {
                tx.send(LmdbMessage::Query(
                    tab_name,
                    arr.clone(),
                    Arc::new(move |q| match q {
                        Ok(value) => cb(Ok(value)),
                        Err(e) => cb(Err(e.to_string())),
                    }),
                ));
            }
            None => {
                cb(Err("Can't get sender".to_string()));
            }
        }

        None
    }

    fn modify(
        &self,
        arr: Arc<Vec<TabKV>>,
        _lock_time: Option<usize>,
        _readonly: bool,
        cb: TxCallback,
    ) -> DBResult {
        let tab_name = self.0.clone().borrow().tab.0.clone().name.to_string();

        match self.0.clone().borrow().sender.clone() {
            Some(tx) => {
                tx.send(LmdbMessage::Modify(
                    tab_name,
                    arr.clone(),
                    Arc::new(move |m| match m {
                        Ok(_) => cb(Ok(())),
                        Err(e) => cb(Err(e.to_string())),
                    }),
                ));
            }
            None => {
                cb(Err("Can't get sender".to_string()));
            }
        }

        None
    }

    fn iter(
        &self,
        key: Option<Bin>,
        descending: bool,
        filter: Filter,
        cb: Arc<Fn(IterResult)>,
    ) -> Option<IterResult> {
        let tab_name = self.0.clone().borrow().tab.0.clone().name.clone();

        match self.0.clone().borrow().sender.clone() {
            Some(tx) => {
                tx.send(LmdbMessage::CreateItemIter(
                    tab_name.clone().to_string(),
                    descending,
                    key,
                ));
                cb(Ok(Box::new(LmdbItemsIter::new(
                    tx.clone(),
                    tab_name,
                    descending,
                    filter,
                ))));
            }
            None => {
                cb(Err("Can't get sender".to_string()));
            }
        }

        None
    }

    fn key_iter(
        &self,
        key: Option<Bin>,
        descending: bool,
        filter: Filter,
        cb: Arc<Fn(KeyIterResult)>,
    ) -> Option<KeyIterResult> {
        let tab_name = self.0.clone().borrow().tab.0.clone().name.clone();

        match self.0.clone().borrow().sender.clone() {
            Some(tx) => {
                tx.send(LmdbMessage::CreateKeyIter(
                    tab_name.clone().to_string(),
                    descending,
                    key,
                ));
                cb(Ok(Box::new(LmdbKeysIter::new(
                    tx.clone(),
                    tab_name,
                    descending,
                    filter,
                ))));
            }
            None => {
                cb(Err("Can't get sender".to_string()));
            }
        }

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
    sender: Sender<LmdbMessage>,
    tab_name: Atom,
    descending: bool,
    _filter: Filter,
}

impl Drop for LmdbItemsIter {
    fn drop(&mut self) {}
}

impl LmdbItemsIter {
    pub fn new(
        sender: Sender<LmdbMessage>,
        tab_name: Atom,
        descending: bool,
        _filter: Filter,
    ) -> Self {
        LmdbItemsIter {
            sender,
            tab_name,
            descending,
            _filter,
        }
    }
}

impl Iter for LmdbItemsIter {
    type Item = (Bin, Bin);

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        self.sender.send(LmdbMessage::NextItem(
            self.tab_name.to_string(),
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
        ));

        None
    }
}

/// lmdb iterator that navigate only keys
pub struct LmdbKeysIter {
    sender: Sender<LmdbMessage>,
    tab_name: Atom,
    descending: bool,
    _filter: Filter,
}

impl Drop for LmdbKeysIter {
    fn drop(&mut self) {}
}

impl LmdbKeysIter {
    pub fn new(
        sender: Sender<LmdbMessage>,
        tab_name: Atom,
        descending: bool,
        _filter: Filter,
    ) -> Self {
        LmdbKeysIter {
            sender,
            tab_name,
            descending,
            _filter,
        }
    }
}

impl Iter for LmdbKeysIter {
    type Item = Bin;

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        self.sender.send(LmdbMessage::NextKey(
            self.tab_name.to_string(),
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
        ));

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
            ware: Atom::from(""),
            tab: Atom::from(""),
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
    // retrive meta table info of a DB
    pub fn new(name: Atom) -> Result<Self, String> {
        if !Path::new(&name.to_string()).exists() {
            fs::create_dir(name.to_string());
        }

        let env = Arc::new(Environment::new()
            .set_max_dbs(MAX_DBS_PER_ENV)
            .open(Path::new(SINFO))
            .map_err(|e| e.to_string())?);

        let db = env
            .create_db(Some(&name.to_string()), DatabaseFlags::empty())
            .map_err(|e| e.to_string())?;
        let txn = env.begin_ro_txn().map_err(|e| e.to_string())?;
        let mut cursor = txn.open_ro_cursor(db).map_err(|e| e.to_string())?;

        let mut tabs: Tabs<LmdbTable> = Tabs::new();

        THREAD_POOL.lock().unwrap().start_pool(32, env.clone());

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
