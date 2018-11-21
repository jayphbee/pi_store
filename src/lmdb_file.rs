use std::sync::Arc;
use std::rc::Rc;
use std::path::Path;
use std::mem::replace;
use std::cell::RefCell;
use std::boxed::FnBox;

use fnv::FnvHashMap;

use pi_base::pi_base_impl::STORE_TASK_POOL;
use pi_base::task::TaskType;

use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_db::db::{
    Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult,
    NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn,
    Tab, OpenTab, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult,
    RwLog, TabMeta
};

use lmdb::{
    Environment, Database, WriteFlags, Error, Transaction, EnvironmentFlags, 
    DatabaseFlags, RwTransaction, RoTransaction, RoCursor, Cursor
};

const ASYNC_DB_TYPE: TaskType = TaskType::Sync;

const DB_ROOT: &str = "_$lmdb";

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
        let txn_ptr = Box::into_raw(Box::new(self.0.env.begin_rw_txn().unwrap())) as usize;
		Arc::new(LmdbTableTxn(Arc::new(RefCell::new(LmdbTableTxnWrapper {
            tab: self.clone(),
            txn_ptr: txn_ptr,
            id: id.clone(),
            _writable: writable,
            state: TxState::Ok
        }))))
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

            cb(Ok(Box::new(LmdbItemsIter::new(txn_ptr, key, lmdb_table_wrapper.db, descending, filter))))
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

            cb(Ok(Box::new(LmdbKeysIter::new(txn_ptr, key, lmdb_table_wrapper.db, descending, filter))))
        }));
        None
    }

    fn index(&self,_tab: &Atom,_index_key: &Atom,_key: Option<Bin>,_descending: bool,_filter: Filter,_cb: Arc<Fn(IterResult)>,) -> Option<IterResult> {
        None
    }

    fn tab_size(&self, cb: Arc<Fn(SResult<usize>)>) -> Option<SResult<usize>> {
        None
    }
}

/// lmdb iterator that navigate key and value
pub struct LmdbItemsIter {
    it_ptr: usize,
    key: Option<Bin>,
    db: Database,
    descending: bool,
    _filter: Filter
}

impl Drop for LmdbItemsIter {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.it_ptr as *mut RoTransaction);
        };
    }
}

impl LmdbItemsIter {
    pub fn new(it_ptr: usize, key: Option<Bin>, db: Database, descending: bool, _filter: Filter) -> Self {
        LmdbItemsIter {
            it_ptr: it_ptr,
            key: key,
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
        let it_ptr = self.it_ptr;
        let db = self.db;
        let key = self.key.clone();

        send_task(Box::new(move || {
            let mut txn = unsafe { Box::from_raw(it_ptr as *mut RwTransaction) };
            let mut cursor = txn.open_rw_cursor(db).unwrap();
            
            if let Some(k) = key {
                if descending {

                } else {

                }
            } else {
                if descending {

                } else {
                    match cursor.iter_start().next() {
                        Some(v) => cb(Ok(Some((Arc::new(v.0.to_vec()), Arc::new(v.1.to_vec()))))),
                        None => cb(Ok(None))
                    }
                }
            }
            
        }));

        None
    }
}

/// lmdb iterator that navigate only keys
pub struct LmdbKeysIter {
    it_ptr: usize,
    key: Option<Bin>,
    db: Database,
    descending: bool,
    _filter: Filter
}

impl Drop for LmdbKeysIter {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.it_ptr as *mut RoTransaction);
        };
    }
}

impl LmdbKeysIter {
    pub fn new(it_ptr: usize, key: Option<Bin>, db: Database, descending: bool, _filter: Filter) -> Self {
        LmdbKeysIter {
            it_ptr: it_ptr,
            key: key,
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
        let it_ptr = self.it_ptr;
        let key = self.key.clone();
        
        send_task(Box::new(move || {
            let mut txn = unsafe { Box::from_raw(it_ptr as *mut RwTransaction) };
            let mut cursor = txn.open_rw_cursor(db).unwrap();

            if let Some(k) = key {
                if descending {

                } else {

                }

            } else {
                if descending {

                } else {
                    match cursor.iter_start().next() {
                        Some(v) => cb(Ok(Some(Arc::new(v.0.to_vec())))),
                        None => cb(Ok(None))
                    }
                }
            }
        }));
        None
    }
}

fn send_task(func: Box<FnBox()>) {
    let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
    let mut task_pool = lock.lock().unwrap();
    (*task_pool).push(ASYNC_DB_TYPE, 20, func, DB_ASYNC_FILE_INFO.clone());
    cvar.notify_one();
}
