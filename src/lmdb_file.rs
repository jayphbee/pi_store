use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{atomic::AtomicBool, atomic::Ordering::SeqCst, Arc, Mutex, RwLock};
use std::thread;

use crossbeam_channel::{bounded, Sender};

use atom::Atom;
use bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use guid::Guid;
use pi_db::db::{
    Bin, CommitResult, DBResult, Filter, Iter, IterResult, KeyIterResult, MetaTxn, NextResult,
    OpenTab, SResult, Tab, TabKV, TabMeta, TabTxn, TxCallback, TxQueryCallback, TxState, Txn, Ware,
    WareSnapshot,
};
use sinfo::EnumType;

use pi_db::tabs::{TabLog, Tabs};

use lmdb::{Cursor, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};

use pool::{LmdbMessage, THREAD_POOL, NO_OP_POOL, NopMessage};

const SINFO: &str = "_$sinfo";
const MAX_DBS_PER_ENV: u32 = 1024;

pub const MDB_SET: u32 = 15;
pub const MDB_PREV: u32 = 12;
pub const MDB_NEXT: u32 = 8;
pub const MDB_FIRST: u32 = 0;
pub const MDB_LAST: u32 = 6;

const TIMEOUT: usize = 100;

lazy_static! {
    pub static ref TXN_INDEX: Arc<Mutex<HashMap<Guid, Arc<LmdbTableTxn>>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

#[derive(Debug, Clone)]
pub struct LmdbTable {
    name: Atom,
    env_flags: EnvironmentFlags,
    write_flags: WriteFlags,
    db_flags: DatabaseFlags,
}

impl Tab for LmdbTable {
    fn new(db_name: &Atom) -> Self {
        LmdbTable {
            name: db_name.clone(),
            env_flags: EnvironmentFlags::empty(),
            write_flags: WriteFlags::empty(),
            db_flags: DatabaseFlags::empty(),
        }
    }

    fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn> {
        if let Some(txn) = TXN_INDEX.lock().unwrap().get(&id) {
            return txn.clone();
        }

        println!("create new lmdb txn: {:?}", id);

        let sender = match THREAD_POOL.clone().lock() {
            Ok(mut pool) => pool.pop(),
            Err(e) => panic!(
                "get sender error when creating transaction: {:?}",
                e.to_string()
            ),
        };

        if sender.is_none() {
            println!("THREAD_POOL full, can't get one from it");
        }

        let db_name = self.name.to_string();
        let (tx, rx) = bounded(1);
        let _ = sender
            .clone()
            .unwrap()
            .send(LmdbMessage::CreateDb(db_name, tx));
        if let Err(e) = rx.recv() {
            panic!("Open db error: {:?}", e.to_string());
        }

        let t = Arc::new(LmdbTableTxn {
            id: id.clone(),
            _writable: writable,
            is_meta_txn: AtomicBool::new(false),
            txns: Arc::new(Mutex::new(Vec::new())),
            state: Arc::new(Mutex::new(TxState::Ok)),
            sender,
            no_op_senders: Arc::new(Mutex::new(Vec::new())),
        });

        TXN_INDEX.lock().unwrap().insert(id.clone(), t.clone());

        t
    }
}

pub struct LmdbTableTxn {
    id: Guid,
    _writable: bool,
    is_meta_txn: AtomicBool,
    txns: Arc<Mutex<Vec<(Atom, Atom)>>>,
    state: Arc<Mutex<TxState>>,
    sender: Option<Sender<LmdbMessage>>,
    no_op_senders: Arc<Mutex<Vec<Sender<NopMessage>>>>,
}

impl Drop for LmdbTableTxn {
    fn drop(&mut self) {
        match THREAD_POOL.clone().lock() {
            Ok(mut pool) => {
                if let Some(sender) = self.sender.clone() {
                    pool.push(sender);
                    self.sender = None;
                } else {
                    panic!("Push None value to thread pool");
                }
            }
            Err(e) => panic!("Give back sender failed: {:?}", e.to_string()),
        };

        match NO_OP_POOL.clone().lock() {
            Ok(mut pool) => {
                for sender in self.no_op_senders.lock().unwrap().iter() {
                    pool.push(sender.clone());
                }
            }

            Err(e) => panic!("Give back no op sender failed: {:?}", e.to_string()),
        }
    }
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
        println!(
            "call lmdb commit, {:?} txns left ---------- !!!!!!!!!!!! --------------",
            self.txns.lock().unwrap().len()
        );

        *self.state.lock().unwrap() = TxState::Committing;
        let state = self.state.clone();

        if self.is_meta_txn.load(SeqCst) || self.txns.lock().unwrap().len() <= 1 {
            println!("commit to lmdb: {:?}", self.id);
            match self.sender.clone() {
                Some(tx) => {
                    let _ = tx.send(LmdbMessage::Commit(Arc::new(move |c| match c {
                        Ok(_) => {
                            *state.lock().unwrap() = TxState::Commited;
                            cb(Ok(()));
                            println!("after commit cb");
                        }
                        Err(e) => {
                            *state.lock().unwrap() = TxState::CommitFail;
                            cb(Err(e.to_string()));
                        }
                    })));
                }
                None => {
                    return Some(Err("Can't get sender".to_string()));
                }
            }
            TXN_INDEX.lock().unwrap().remove(&self.id);
            println!("remove committed txn: {:?}", self.id.clone());
        } else {
            let _ = self.txns.lock().unwrap().pop();
            match NO_OP_POOL.lock().unwrap().pop() {
                Some(sender) => {
                    let _ = sender.send(NopMessage::Nop(cb));
                    self.no_op_senders.lock().unwrap().push(sender);
                },

                None => return Some(Err("Can't get no op sender".to_string())),
            }
        }

        None
    }

    fn rollback(&self, cb: TxCallback) -> DBResult {
        println!("call lmdb rollback ---------- !!!!!!!!!!!! --------------");

        *self.state.lock().unwrap() = TxState::Rollbacking;
        let state = self.state.clone();

        if self.txns.lock().unwrap().len() <= 1 {
            println!("rollback to lmdb: {:?}", self.id);

            match self.sender.clone() {
                Some(tx) => {
                    let _ = tx.send(LmdbMessage::Rollback(Arc::new(move |r| match r {
                        Ok(_) => {
                            *state.lock().unwrap() = TxState::Rollbacked;
                            cb(Ok(()));
                        }
                        Err(e) => {
                            *state.lock().unwrap() = TxState::RollbackFail;
                            cb(Err(e.to_string()));
                        }
                    })));
                }
                None => {
                    return Some(Err("Can't get sender".to_string()));
                }
            }
            TXN_INDEX.lock().unwrap().remove(&self.id);
        } else {
            let _ = self.txns.lock().unwrap().pop();
            match self.sender.clone() {
                Some(tx) => {
                    let _ = tx.send(LmdbMessage::NoOp(cb));
                }
                None => return Some(Err("Can't get sender".to_string())),
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
        println!("call lmdb query ---------- !!!!!!!!!!!! --------------");

        for v in arr.clone().iter() {
            if !self
                .txns
                .lock()
                .unwrap()
                .contains(&(v.ware.clone(), v.tab.clone()))
            {
                self.txns
                    .lock()
                    .unwrap()
                    .push((v.ware.clone(), v.tab.clone()));
            }
        }

        println!("accumulated txns: {:?}", self.txns.lock().unwrap().len());

        match self.sender.clone() {
            Some(tx) => {
                let _ = tx.send(LmdbMessage::Query(
                    arr.clone(),
                    Arc::new(move |q| match q {
                        Ok(value) => cb(Ok(value)),
                        Err(e) => cb(Err(e.to_string())),
                    }),
                ));
            }
            None => {
                return Some(Err("Can't get sender".to_string()));
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
        // this is an meta txn
        if arr[0].ware == Atom::from("") && arr[0].tab == Atom::from("") {
            println!("------------ this is a meta txn !!!!! --------- ");
            self.is_meta_txn.store(true, SeqCst);
        }

        println!("call lmdb modify ---------- !!!!!!!!!!!! --------------");
        for v in arr.clone().iter() {
            if !self
                .txns
                .lock()
                .unwrap()
                .contains(&(v.ware.clone(), v.tab.clone()))
                && (arr[0].ware != Atom::from("") && arr[0].tab != Atom::from(""))
            {
                self.txns
                    .lock()
                    .unwrap()
                    .push((v.ware.clone(), v.tab.clone()));
            }
        }
        println!("accumulated txns: {:?}", self.txns.lock().unwrap().len());

        match self.sender.clone() {
            Some(tx) => {
                let _ = tx.send(LmdbMessage::Modify(
                    arr.clone(),
                    Arc::new(move |m| match m {
                        Ok(_) => cb(Ok(())),
                        Err(e) => cb(Err(e.to_string())),
                    }),
                ));
            }
            None => {
                return Some(Err("Can't get sender".to_string()));
            }
        }

        None
    }

    fn iter(
        &self,
        key: Option<Bin>,
        descending: bool,
        filter: Filter,
        _cb: Arc<Fn(IterResult)>,
    ) -> Option<IterResult> {
        if self.txns.lock().unwrap().len() < 1 {
            println!("call lmdb iter ---------- !!!!!!!!!!!! --------------");

            self.txns
                .lock()
                .unwrap()
                .push((Atom::from(""), Atom::from("")));
        }

        match self.sender.clone() {
            Some(tx) => {
                let (inner_tx, inner_rx) = bounded(1);
                let _ = tx.send(LmdbMessage::CreateItemIter(descending, key, inner_tx));
                match inner_rx.recv() {
                    Ok(_) => {
                        return Some(Ok(Box::new(LmdbItemsIter::new(tx.clone(), filter))));
                    }
                    Err(e) => {
                        panic!("Create db item iter in pool failed: {:?}", e.to_string());
                    }
                }
            }
            None => {
                return Some(Err("Can't get sender".to_string()));
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
        if self.txns.lock().unwrap().len() < 1 {
            println!("call lmdb key iter ---------- !!!!!!!!!!!! --------------");

            self.txns
                .lock()
                .unwrap()
                .push((Atom::from(""), Atom::from("")));
        }

        match self.sender.clone() {
            Some(tx) => {
                let (inner_tx, inner_rx) = bounded(1);
                let _ = tx.send(LmdbMessage::CreateKeyIter(descending, key, inner_tx));
                match inner_rx.recv() {
                    Ok(_) => {
                        return Some(Ok(Box::new(LmdbKeysIter::new(tx.clone(), filter))));
                    }
                    Err(e) => {
                        panic!("Create db key iter in pool failed: {:?}", e.to_string());
                    }
                }
            }
            None => {
                return Some(Err("Can't get sender".to_string()));
            }
        }
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
        // todo: duplicate call tab_size
        self.txns
            .lock()
            .unwrap()
            .push((Atom::from(""), Atom::from("")));

        match self.sender.clone() {
            Some(tx) => {
                let _ = tx.send(LmdbMessage::TableSize(Arc::new(move |t| match t {
                    Ok(entries) => cb(Ok(entries)),
                    Err(e) => cb(Err(e.to_string())),
                })));
            }
            None => {
                return Some(Err("Can't get sender".to_string()));
            }
        }
        None
    }
}

/// lmdb iterator that navigate key and value
pub struct LmdbItemsIter {
    sender: Sender<LmdbMessage>,
    _filter: Filter,
}

impl LmdbItemsIter {
    pub fn new(sender: Sender<LmdbMessage>, _filter: Filter) -> Self {
        LmdbItemsIter { sender, _filter }
    }
}

impl Iter for LmdbItemsIter {
    type Item = (Bin, Bin);

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        println!("call lmdb next item ---------- !!!!!!!!!!!! --------------");
        let _ = self
            .sender
            .send(LmdbMessage::NextItem(Arc::new(move |item| match item {
                Ok(Some(v)) => {
                    cb(Ok(Some(v)));
                }
                Ok(None) => {
                    cb(Ok(None));
                }
                Err(e) => {
                    cb(Err(e.to_string()));
                }
            })));

        None
    }
}

/// lmdb iterator that navigate only keys
pub struct LmdbKeysIter {
    sender: Sender<LmdbMessage>,
    _filter: Filter,
}

impl LmdbKeysIter {
    pub fn new(sender: Sender<LmdbMessage>, _filter: Filter) -> Self {
        LmdbKeysIter { sender, _filter }
    }
}

impl Iter for LmdbKeysIter {
    type Item = Bin;

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        let _ = self
            .sender
            .send(LmdbMessage::NextKey(Arc::new(move |item| match item {
                Ok(Some(v)) => {
                    cb(Ok(Some(v)));
                }
                Ok(None) => {
                    cb(Ok(None));
                }
                Err(e) => {
                    cb(Err(e.to_string()));
                }
            })));

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
            ware: Atom::from("file"), // hard code
            tab: tab.clone(),
            key: key,
            index: 0,
            value: value,
        };

        self.0.modify(
            Arc::new(vec![
                // placeholder for meta txn
                TabKV {
                    ware: Atom::from(""),
                    tab: Atom::from(""),
                    key: Arc::new(Vec::new()),
                    index: 0,
                    value: None,
                },
                tabkv,
            ]),
            None,
            false,
            cb,
        )
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
        let db = env
            .create_db(Some(SINFO), DatabaseFlags::empty())
            .map_err(|e| e.to_string())?;
        let txn = env.begin_ro_txn().map_err(|e| e.to_string())?;
        let mut cursor = txn.open_ro_cursor(db).map_err(|e| e.to_string())?;

        let mut tabs: Tabs<LmdbTable> = Tabs::new();

        THREAD_POOL.lock().unwrap().start_pool(32, env.clone());
        NO_OP_POOL.lock().unwrap().start_nop_pool(32);

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
