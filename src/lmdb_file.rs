use std::sync::{ Arc, RwLock };
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::rc::Rc;
use std::path::Path;
use std::mem::replace;
use std::cell::RefCell;
use std::boxed::FnBox;
use std::slice::from_raw_parts;

use fnv::FnvHashMap;

use pi_base::pi_base_impl::STORE_TASK_POOL;
use pi_base::task::TaskType;

use pi_lib::sinfo::{EnumType};
use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::bon::{ReadBuffer, WriteBuffer, Encode, Decode};
use pi_db::db::{
    Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult,
    NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn,
    Tab, OpenTab, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult,
    RwLog, TabMeta
};

use pi_db::tabs::{TabLog, Tabs};

use lmdb::{
    Environment, Database, WriteFlags, Error, Transaction, EnvironmentFlags,
    DatabaseFlags, RwTransaction, RoTransaction, RoCursor, Cursor, RwCursor,

    mdb_set_compare, MDB_txn, MDB_dbi, MDB_val, MDB_cmp_func
};

const ASYNC_DB_TYPE: TaskType = TaskType::Sync;

const DB_ROOT: &str = "_$lmdb";
const SINFO: &str = "_$sinfo";
const MAX_DBS_PER_ENV: u32 = 1024;

const MDB_SET: u32 = 15;
const MDB_PREV: u32 = 12;
const MDB_NEXT: u32 = 8;
const MDB_FIRST: u32 = 0;
const MDB_LAST: u32 = 6;

const TIMEOUT: usize = 100;

lazy_static! {
	pub static ref DB_ASYNC_FILE_INFO: Atom = Atom::from("DB asyn file");
}

#[derive(Debug)]
pub struct LmdbTableWrapper {
    env: Environment,
    db: Database,
    name: Atom,
    // TODO: add methods to set/get these flags
    env_flags: EnvironmentFlags,
    write_flags: WriteFlags,
    db_flags: DatabaseFlags
}


#[derive(Debug, Clone)]
pub struct LmdbTable(Arc<LmdbTableWrapper>);

impl Tab for LmdbTable {
    fn new(db_name: &Atom) -> Self {
        let env = Environment::new()
            .set_max_dbs(1024)
            .open(Path::new(DB_ROOT))
            .expect("Open lmdb environment failed");
        let db = env.create_db(Some(&db_name.to_string()), DatabaseFlags::empty())
            .expect("Open lmdb database failed");

        LmdbTable(Arc::new(LmdbTableWrapper {
            env: env,
            db: db,
            name: db_name.clone(),
            env_flags: EnvironmentFlags::empty(),
            write_flags: WriteFlags::empty(),
            db_flags: DatabaseFlags::empty()
        }))
    }

    fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn> {
        let txn_ptr = if writable {
            let rw_txn = self.0.env.begin_rw_txn().unwrap();
            // set comparator function
            unsafe {
                mdb_set_compare(rw_txn.txn(), self.0.db.dbi(), mdb_cmp_func as *mut MDB_cmp_func);
            }
            Box::into_raw(Box::new(rw_txn)) as usize
        } else {
            let ro_txn = self.0.env.begin_ro_txn().unwrap();
            // set comparator function
            unsafe {
                mdb_set_compare(ro_txn.txn(), self.0.db.dbi(), mdb_cmp_func as *mut MDB_cmp_func);
            }
            Box::into_raw(Box::new(ro_txn)) as usize
        };

		Arc::new(LmdbTableTxn(Arc::new(RefCell::new(LmdbTableTxnWrapper {
            tab: self.clone(),
            // TODO: record all txns built from this env and free them if this env becoames invalidate
            txn_ptr: txn_ptr,
            id: id.clone(),
            _writable: writable,
            state: TxState::Ok
        }))))
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

struct LmdbTableTxnWrapper {
    txn_ptr: usize,
    tab: LmdbTable,
    id: Guid,
	_writable: bool,
	state: TxState
}

impl Drop for LmdbTableTxnWrapper {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.txn_ptr as *mut RwTransaction);
        }
    }
}

#[derive(Clone)]
pub struct LmdbTableTxn(Arc<RefCell<LmdbTableTxnWrapper>>);

impl Txn for LmdbTableTxn {
    fn get_state(&self) -> TxState {
        self.0.borrow().state.clone()
    }

    fn prepare(&self, _timeout:usize, _cb: TxCallback) -> DBResult {
        self.0.borrow_mut().state = TxState::Preparing;
        Some(Ok(()))
    }

    fn commit(&self, cb: TxCallback) -> CommitResult {
        Arc::clone(&self.0).borrow_mut().state = TxState::Committing;
        let lmdb_table_txn = self.0.clone();
        let txn_ptr = self.0.clone().borrow_mut().txn_ptr;
        send_task(Box::new(move || {
            let mut txn = unsafe { Box::from_raw(txn_ptr as *mut RwTransaction)};
            match txn.commit() {
                Ok(_) => {
                    lmdb_table_txn.borrow_mut().state = TxState::Commited;
                    cb(Ok(()))
                },
                Err(e) => {
                    lmdb_table_txn.borrow_mut().state = TxState::CommitFail;
                    cb(Err(e.to_string()))
                }
            }
        }));

        None
    }

    fn rollback(&self, cb: TxCallback) -> DBResult {
        let lmdb_table_txn = self.0.clone();
        let txn_ptr = self.0.clone().borrow_mut().txn_ptr;
        let mut txn = unsafe { Box::from_raw(txn_ptr as *mut RwTransaction) };
        txn.abort();
        lmdb_table_txn.borrow_mut().state = TxState::Rollbacked;

        Some(Ok(()))
    }
}

impl TabTxn for LmdbTableTxn {
    fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool, _cb: TxCallback) -> DBResult {
		None
	}

    fn query(&self,arr: Arc<Vec<TabKV>>,_lock_time: Option<usize>,_readonly: bool, cb: TxQueryCallback,) -> Option<SResult<Vec<TabKV>>> {
        let lmdb_table_txn = self.0.clone();
        let txn_ptr = self.0.clone().borrow_mut().txn_ptr;
        send_task(Box::new(move || {
            let mut value = Vec::new();

            let lmdb_table_txn_wrapper = lmdb_table_txn.borrow_mut();
            let lmdb_table = &lmdb_table_txn_wrapper.tab;
            let lmdb_table_wrapper = lmdb_table.0.clone();

            let mut txn = unsafe { Box::from_raw(txn_ptr as *mut RwTransaction) };

            for kv in arr.iter() {
                if let Ok(v) = txn.get(lmdb_table_wrapper.db, kv.key.as_ref()) {
                    value.push(
                        TabKV {
                            ware: kv.ware.clone(),
                            tab: kv.tab.clone(),
                            key: kv.key.clone(),
                            index: kv.index,
                            value: Some(Arc::new(Vec::from(v)))
                        }
                    );
                } else {
                    return cb(Err("query failed".to_string()));
                }
            }
            cb(Ok(value))
        }));
        None
    }

    fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool, cb: TxCallback) -> DBResult {
        let lmdb_table_txn = self.0.clone();
        let txn_ptr = self.0.clone().borrow_mut().txn_ptr;
        send_task(Box::new(move || {
            let lmdb_table_txn_wrapper = lmdb_table_txn.borrow_mut();
            let lmdb_table = &lmdb_table_txn_wrapper.tab;
            let lmdb_table_wrapper = lmdb_table.0.clone();

            let mut txn = unsafe { Box::from_raw(txn_ptr as *mut RwTransaction) };

            for kv in arr.iter() {
                if let Some(_) = kv.value {
                    match txn.put(lmdb_table_wrapper.db, kv.key.as_ref(), kv.clone().value.unwrap().as_ref(), WriteFlags::empty()) {
                        Ok(_) => (),
                        Err(e) => return cb(Err(e.to_string()))
                    };
                } else {
                    match txn.del(lmdb_table_wrapper.db, kv.key.as_ref(), None) {
                        Ok(_) => (),
                        Err(e) => return cb(Err(e.to_string()))
                    };
                }
            }
        }));
        None
    }

    fn iter(&self,key: Option<Bin>,descending: bool,filter: Filter, cb: Arc<Fn(IterResult)>,) -> Option<IterResult> {
        let lmdb_table_txn = self.0.clone();
        let txn_ptr = self.0.clone().borrow_mut().txn_ptr;

        send_task(Box::new(move || {
            let lmdb_table_txn_wrapper = lmdb_table_txn.borrow_mut();
            let lmdb_table = &lmdb_table_txn_wrapper.tab;
            let lmdb_table_wrapper = lmdb_table.0.clone();

            let mut txn = unsafe { Box::from_raw(txn_ptr as *mut RoTransaction) };
            let mut cursor = txn.open_ro_cursor(lmdb_table_wrapper.db).unwrap();

            if let Some(k) = key.clone() {
                // get mothod has side effect to advance cursor
                cursor.get(Some(k.as_ref()), None, MDB_SET);
            } else {
                if descending {
                    cursor.get(None, None, MDB_FIRST);
                } else {
                    cursor.get(None, None, MDB_LAST);
                }
            }

            let cursor_ptr = unsafe { Box::into_raw(Box::new(cursor)) as usize };

            cb(Ok(Box::new(LmdbItemsIter::new(cursor_ptr, lmdb_table_wrapper.db, descending, filter))))
        }));

        None
    }

    fn key_iter(&self, key: Option<Bin>,descending: bool,filter: Filter, cb: Arc<Fn(KeyIterResult)>,) -> Option<KeyIterResult> {
        let lmdb_table_txn = self.0.clone();
        let txn_ptr = self.0.clone().borrow_mut().txn_ptr;

        send_task(Box::new(move || {
            let lmdb_table_txn_wrapper = lmdb_table_txn.borrow_mut();
            let lmdb_table = &lmdb_table_txn_wrapper.tab;
            let lmdb_table_wrapper = lmdb_table.0.clone();

            let mut txn = unsafe { Box::from_raw(txn_ptr as *mut RoTransaction) };
            let mut cursor = txn.open_ro_cursor(lmdb_table_wrapper.db).unwrap();

            if let Some(k) = key.clone() {
                cursor.get(Some(k.as_ref()), None, MDB_SET);
            } else {
                if descending {
                    cursor.get(None, None, MDB_FIRST);
                } else {
                    cursor.get(None, None, MDB_LAST);
                }
            }

            let cursor_ptr = unsafe { Box::into_raw(Box::new(cursor)) as usize };

            cb(Ok(Box::new(LmdbKeysIter::new(cursor_ptr, lmdb_table_wrapper.db, descending, filter))))
        }));
        None
    }

    fn index(&self,_tab: &Atom,_index_key: &Atom,_key: Option<Bin>,_descending: bool,_filter: Filter,_cb: Arc<Fn(IterResult)>,) -> Option<IterResult> {
        None
    }

    fn tab_size(&self, cb: TxCallback) -> DBResult {
        None
    }
}

/// lmdb iterator that navigate key and value
pub struct LmdbItemsIter {
    cursor_ptr: Arc<AtomicUsize>,
    db: Database,
    descending: bool,
    _filter: Filter
}

impl Drop for LmdbItemsIter {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.cursor_ptr.load(Ordering::SeqCst) as *mut RoTransaction);
        };
    }
}

impl LmdbItemsIter {
    pub fn new(cursor_ptr: usize, db: Database, descending: bool, _filter: Filter) -> Self {
        LmdbItemsIter {
            cursor_ptr: Arc::new(AtomicUsize::new(cursor_ptr)),
            db: db,
            descending: descending,
            _filter: _filter
        }
    }
}

impl Iter for LmdbItemsIter {
    type Item = (Bin, Bin);

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        let descending = self.descending;
        let cursor_ptr = self.cursor_ptr.clone();
        let db = self.db;

        send_task(Box::new(move || {
            // cast cursor_ptr to RoCursor
            let mut cursor = unsafe { Box::from_raw(cursor_ptr.load(Ordering::SeqCst) as *mut RoCursor) };

            if descending {
                match cursor.get(None, None, MDB_NEXT) {
                    Ok(v) => cb(Ok(Some((Arc::new(v.0.unwrap().to_vec()), Arc::new(v.1.to_vec()))))),
                    Err(_) => cb(Ok(None))
                }
            } else {
                match cursor.get(None, None, MDB_PREV) {
                    Ok(v) => cb(Ok(Some((Arc::new(v.0.unwrap().to_vec()), Arc::new(v.1.to_vec()))))),
                    Err(_) => cb(Ok(None))
                }
            }
            // cast back cursor to AtomicUsize
            cursor_ptr.store(unsafe { Box::into_raw(Box::new(cursor)) as usize }, Ordering::SeqCst);
        }));

        None
    }
}

/// lmdb iterator that navigate only keys
pub struct LmdbKeysIter {
    cursor_ptr:  Arc<AtomicUsize>,
    db: Database,
    descending: bool,
    _filter: Filter
}

impl Drop for LmdbKeysIter {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.cursor_ptr.load(Ordering::SeqCst) as *mut RoTransaction);
        };
    }
}

impl LmdbKeysIter {
    pub fn new(cursor_ptr: usize, db: Database, descending: bool, _filter: Filter) -> Self {
        LmdbKeysIter {
            cursor_ptr: Arc::new(AtomicUsize::new(cursor_ptr)),
            db: db,
            descending: descending,
            _filter: _filter
        }
    }
}

impl Iter for LmdbKeysIter {
    type Item = Bin;

    fn next(&mut self, cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>> {
        let descending = self.descending;
        let db = self.db;
        let cursor_ptr = self.cursor_ptr.clone();

        send_task(Box::new(move || {
            // cast cursor_ptr to RoCursor
            let mut cursor = unsafe { Box::from_raw(cursor_ptr.load(Ordering::SeqCst) as *mut RoCursor) };

            if descending {
                match cursor.get(None, None, MDB_NEXT) {
                    Ok(v) => cb(Ok(Some(Arc::new(v.0.unwrap().to_vec())))),
                    Err(_) => cb(Ok(None))
                }
            } else {
                match cursor.get(None, None, MDB_PREV) {
                    Ok(v) => cb(Ok(Some(Arc::new(v.0.unwrap().to_vec())))),
                    Err(_) => cb(Ok(None))
                }
            }
            // cast back cursor to AtomicUsize
            cursor_ptr.store(unsafe { Box::into_raw(Box::new(cursor)) as usize }, Ordering::SeqCst);
        }));
        None
    }
}

#[derive(Clone)]
pub struct LmdbMetaTxn(Arc<TabTxn>);

impl LmdbMetaTxn{
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
            },
            None => None,
        };

        let tabkv = TabKV{
            ware: Atom::from(""),
            tab: Atom::from(""),
            key: key,
            index: 0,
            value: value,
        };
        self.0.modify(Arc::new(vec![tabkv]), None, false, cb)
	}

	// 快照拷贝表
	fn snapshot(&self, _tab: &Atom, _from: &Atom, _cb: TxCallback) -> DBResult{
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
pub struct LmdbWareHouse {
    name: Atom,
    tabs: Arc<RwLock<Tabs<LmdbTable>>>
}

impl LmdbWareHouse {

	pub fn new(name: Atom) -> Result<Self, String> {

        let env = Environment::new().set_max_dbs(MAX_DBS_PER_ENV).open(Path::new(DB_ROOT)).unwrap();
        let db = env.open_db(Some(&name.to_string())).unwrap();
        let txn = env.begin_ro_txn().unwrap();
        let mut cursor = txn.open_ro_cursor(db).unwrap();
        let mut tab_iter = cursor.iter();

        let mut tabs: Tabs<LmdbTable> = Tabs::new();

        while let Some(kv) = tab_iter.next() {
            tabs.set_tab_meta(
                Atom::decode(&mut ReadBuffer::new(kv.0, 0)),
                Arc::new(TabMeta::decode(&mut ReadBuffer::new(kv.1, 0)))
            );
        }

        tabs.set_tab_meta(Atom::from(SINFO), Arc::new(TabMeta::new(EnumType::Str, EnumType::Bool)));

        Ok( LmdbWareHouse {
            name: name,
            tabs: Arc::new(RwLock::new(tabs))
        })
	}
}

impl OpenTab for LmdbWareHouse {
	// 打开指定的表，表必须有meta
	fn open<'a, T: Tab>(&self, tab: &Atom, _cb: Box<Fn(SResult<T>) + 'a>) -> Option<SResult<T>> {
        Some(Ok(T::new(tab)))
	}
}

impl Ware for LmdbWareHouse {
	// 拷贝全部的表
	fn tabs_clone(&self) -> Arc<Ware> {
	    Arc::new(LmdbWareHouse {
            name: self.name.clone(),
            tabs:Arc::new(RwLock::new(self.tabs.read().unwrap().clone_map()))
        })
	}
	// 列出全部的表
	fn list(&self) -> Box<Iterator<Item=Atom>> {
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
		Arc::new(LmdbSnapshot(self.clone(), RefCell::new(self.tabs.read().unwrap().snapshot())))
	}
}

/// LMDB manager
pub struct LmdbSnapshot(
    LmdbWareHouse,
    RefCell<TabLog<LmdbTable>>
);

impl WareSnapshot for LmdbSnapshot {
	// 列出全部的表
	fn list(&self) -> Box<Iterator<Item=Atom>> {
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
	fn tab_txn(&self, tab_name: &Atom, id: &Guid, writable: bool, cb: Box<Fn(SResult<Arc<TabTxn>>)>) -> Option<SResult<Arc<TabTxn>>> {
		self.1.borrow().build(&self.0, tab_name, id, writable, cb)
	}
	// 创建一个meta事务
	fn meta_txn(&self, id: &Guid) -> Arc<MetaTxn> {
        Arc::new(LmdbMetaTxn::new(self.tab_txn(&Atom::from(SINFO), id, true, Box::new(|_r|{})).unwrap().expect("meta_txn")))
	}
	// 元信息预提交
	fn prepare(&self, id: &Guid) -> SResult<()> {
		(self.0).tabs.write().unwrap().prepare(id, &mut self.1.borrow_mut())
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

fn send_task(func: Box<FnBox()>) {
    let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
    let mut task_pool = lock.lock().unwrap();
    (*task_pool).push(ASYNC_DB_TYPE, 20, func, DB_ASYNC_FILE_INFO.clone());
    cvar.notify_one();
}
