
// use rocksdb::{TXN_DB, TXN, Options, TransactionDBOptions, TransactionOptions, ReadOptions, WriteOptions, DBRawIterator};
use pi_db::db::{Txn, TabTxn, TabKV, TxIterCallback, TxQueryCallback, DBResult, MetaTxn, Tab, TabBuilder, TxCallback, TxState, Cursor, UsizeResult};
use pi_lib::sinfo::{StructInfo};
use pi_lib::atom::{Atom};
use pi_lib::guid::{Guid, GuidGen};
use pi_lib::time::now_nanos;
use pi_lib::bon::{BonBuffer, Encode};

use std::sync::{Arc, Mutex, Condvar};

use std::string::String;
use std::str;
use std::vec::Vec;
use std::usize;
use std::clone::Clone;

use pi_vm::task::TaskType;
use pi_vm::task_pool::TaskPool;

#[derive(Clone)]
pub struct FdbTxn {
    id: Guid,
    txn: TXN,
    state: TxState,
}

//绑定了所有事务方法
#[derive(Clone)]
pub struct MutexFdbTxn(Arc<Mutex<FdbTxn>>);

//表状态
pub enum TabState {
    close = 0,
    open,
}

//迭代器方法
pub struct FDBIterator(DBRawIterator);

//本地方法表基础操作（打开、关闭）
pub struct FDBTab {
    tab: Atom,
    state: TabState,
}

//对应接口的Tab 创建事务
pub struct FDB(TXN_DB);

//系统表 记录所有表信息
pub struct SysDb(TXN_DB);

lazy_static! {
	pub static ref STORE_TASK_POOL: Arc<(Mutex<TaskPool>, Condvar)> = Arc::new((Mutex::new(TaskPool::new(10)), Condvar::new()));
}

/*
* db异步访问任务类型
*/
const ASYNC_DB_TYPE: TaskType = TaskType::Sync;

/*
* db异步访问任务优先级
*/
const DB_PRIORITY: u32 = 20;

/*
* 信息
*/
const DB_ASYNC_FILE_INFO: &str = "DB asyn file";

impl FDBTab {
    fn new(tab: &Atom) -> Self {
        FDBTab {
            tab: tab.clone(),
            state: TabState::close,
        }
    }
    fn open(&mut self) -> TXN_DB {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut txn_db_opts = TransactionDBOptions::default();
        let tab = &*self.tab.clone();
        //打开rocksdb
        let db = TXN_DB::open(&opts, &txn_db_opts, &tab).unwrap();
        db
    }
    fn close(&mut self) {
        // self.db.unwrap().drop();
    }
}

impl Txn for MutexFdbTxn {
    // 获得事务的状态
    fn get_state(&self) -> TxState {
        TxState::Ok
    }
    // 预提交一个事务
    fn prepare(&self, cb: TxCallback) -> UsizeResult {
        let mut fdb_txn = self.0.lock().unwrap();
        fdb_txn.state = TxState::Preparing;
        let txn = fdb_txn.txn.clone();
        let mut state = fdb_txn.state.clone();
        let func = move || {
        match txn.prepare() {
            Ok(()) => {
                state = TxState::PreparOk;
                cb(Ok(1))
                },
            Err(e) => {
                state = TxState::PreparFail;
                cb(Err(e.to_string()))
                },
        }
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
        
    }
    // 提交一个事务
    fn commit(&self, cb: TxCallback) -> UsizeResult {
        let mut fdb_txn = self.0.lock().unwrap();
        fdb_txn.state = TxState::Committing;
        let txn = fdb_txn.txn.clone();
        let mut state = fdb_txn.state.clone();
        let func = move || {
        match txn.commit() {
            Ok(()) => {
                state = TxState::Commited;
                cb(Ok(1))
            },
            Err(e) => {
                state = TxState::CommitFail;
                cb(Err(e.to_string()))
                },
        }
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
    }
    // 回滚一个事务
    fn rollback(&self, cb: TxCallback) -> UsizeResult {
        let mut fdb_txn = self.0.lock().unwrap();
        fdb_txn.state = TxState::Rollbacking;
        let txn = fdb_txn.txn.clone();
        let mut state = fdb_txn.state.clone();
        let func = move || {
        match txn.rollback() {
            Ok(()) => {
                state = TxState::Rollbacked;
                cb(Ok(1))
                },
            Err(e) => {
                state = TxState::RollbackFail;
                cb(Err(e.to_string()))
                },
        }
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
    }
}

impl TabTxn for MutexFdbTxn {
    // 键锁，key可以不存在，根据lock_time的值，大于0是锁，0为解锁。 分为读写锁，读写互斥，读锁可以共享，写锁只能有1个
	fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool, _cb: TxCallback) -> UsizeResult {
		None
	}
    // 查询
	fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool,
		cb: TxQueryCallback,
	) -> Option<DBResult<Vec<TabKV>>> {
        let fdb_txn = self.0.lock().unwrap();
        let txn = fdb_txn.txn.clone();
        let func = move || {
            let mut value_arr = Vec::new();
            for tabkv in arr.iter() {
                let read_opts = ReadOptions::default();
                let mut value = None;
                match txn.get(&read_opts, tabkv.key.as_slice()) {
                            Ok(None) => (),
                            Ok(v) => 
                                {
                                    value = Some(Arc::new(v.unwrap().to_utf8().unwrap().as_bytes().to_vec()));
                                    ()
                                },
                            Err(e) => 
                                {
                                    cb(Err(e.to_string()));
                                    return;
                                },
                        }
                value_arr.push(
                    TabKV{
                    tab: tabkv.tab.clone(),
                    key: tabkv.key.clone(),
                    index: tabkv.index,
                    value: value,
                    }
                )
            }
            cb(Ok(value_arr))
            
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
    }
    // 修改，插入、删除及更新
    fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool, cb: TxCallback) -> UsizeResult {
        let fdb_txn = self.0.lock().unwrap();
        let txn = fdb_txn.clone().txn;
        let func = move || {
            let len = arr.len();
            for tabkv in arr.iter() {
                if tabkv.value == None {
                    match txn.delete(&tabkv.key.as_slice()) {
                    Ok(_) => (),
                    Err(e) => 
                        {
                            cb(Err(e.to_string()));
                            return;
                        },
                    };
                } else {
                    match txn.put(&tabkv.key.as_slice(), &tabkv.value.clone().unwrap().as_slice()) {
                    Ok(_) => (),
                    Err(e) =>
                        {
                            cb(Err(e.to_string()));
                            return;
                        },
                    };
                }
            }
            cb(Ok(len))
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
    }
    // 迭代表
    fn iter(
		&self,
		tab: &Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		_key_only: bool,
		_filter: String,
		cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
        let fdb_txn = self.0.lock().unwrap();
        let txn = fdb_txn.txn.clone();
        let func = move || {
            let read_opts = ReadOptions::default();
            let mut rocksdb_iter = txn.iter(&read_opts);
            if key == None {
                if descending {
                    rocksdb_iter.seek_to_last();
                } else {
                    rocksdb_iter.seek_to_first();
                }
            } else {
                if descending {
                    rocksdb_iter.seek_for_prev(key.unwrap().as_slice());
                } else {
                    rocksdb_iter.seek(key.unwrap().as_slice());
                }
            }
            cb(Ok(Box::new(FDBIterator(rocksdb_iter))))
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None

        // let read_opts = ReadOptions::default();
        // let mut rocksdb_iter = txn.iter(&read_opts);
        
        // if key == None {
        //     if descending {
        //         rocksdb_iter.seek_to_last();
        //     } else {
        //         rocksdb_iter.seek_to_first();
        //     }
        // } else {
        //     if descending {
        //         rocksdb_iter.seek_for_prev(key.unwrap().as_slice());
        //     } else {
        //         rocksdb_iter.seek(key.unwrap().as_slice());
        //     }
        // }
        // Some(Ok(Box::new(FDBIterator(rocksdb_iter))))
    }
    // 迭代索引
	fn index(
		&self,
		_tab: &Atom,
		_key: Option<Vec<u8>>,
		_descending: bool,
		_filter: String,
		_cb: TxIterCallback,
	) -> Option<DBResult<Box<Cursor>>> {
        None
}
    fn tab_size(&self, cb: TxCallback) -> UsizeResult {
        let func = move || {
           cb(Ok(usize::max_value()))
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
    }

}

impl Tab for FDB {
    fn transaction(&self, id: &Guid, _writable: bool, _timeout: usize) -> Arc<TabTxn> {
        let txn_opts = TransactionOptions::default();
        let write_opts = WriteOptions::default();
        let txn_db = &self.0;
        let rocksdb_txn = TXN::begin(txn_db, &write_opts, &txn_opts).unwrap();
        let mut txn_name = String::from("rocksdb_");
        txn_name.push_str(now_nanos().to_string().as_str());
        match rocksdb_txn.set_name(&txn_name) {
            Ok(_) => println!("set_name ok!!!!!"),
            Err(e) => println!("set_name err:{}", e.to_string()),
        };

        let txn = MutexFdbTxn(Arc::new(Mutex::new(
            FdbTxn {
                id: id.clone(),
                txn: rocksdb_txn,
                state: TxState::Ok
            }
        )));
        
        return Arc::new(txn)
    }
}

// 每个TabBuilder的元信息事务
impl MetaTxn for MutexFdbTxn {
	// 创建表、修改指定表的元数据
	fn alter(
		&self,
		tab: &Atom,
		meta: Option<Arc<StructInfo>>,
		_cb: TxCallback,
	) -> UsizeResult {
        let mut value;
		match meta {
			None => value = None,
			Some(m) => {
				let mut meta_buf = BonBuffer::new();
				m.encode(&mut meta_buf);
				value = Some(Arc::new(meta_buf.unwrap()));
			}
		}
		let mut arr = Vec::new();
		let tab_name = &**tab;
		let mut kv = TabKV::new(tab.clone(), tab_name.clone().into_bytes());
		kv.value = value;
		arr.push(kv);
		&self.modify(Arc::new(arr), None, false, Arc::new(|_v|{}));
        Some(Ok(1))
    }
	// 修改指定表的名字
	fn rename(
		&self,
		_tab: &Atom,
		_new_name: &Atom,
		_cb: TxCallback,
	) -> UsizeResult {
        Some(Ok(1))
    }
}

//管理端需要调用new方法生成一张系统表，用于存放所有表信息
impl SysDb {
	pub fn new(tab_name: &Atom) -> Self {
		let mut tab = FDBTab::new(tab_name);
        let db = tab.open();
        SysDb(db)
	}
}

impl TabBuilder for SysDb {
    // 列出全部的表
	fn list(
		&self,
	) -> Vec<(Atom, Arc<StructInfo>)> {
        //TODO Txn和MetaTxn接口中未提供迭代方法，暂不实现
        vec![]
    }
	// 打开指定的表，表必须有meta
	fn open(
		&self,
		tab: &Atom,
		_cb: Box<Fn(DBResult<Arc<Tab>>)>,
	) -> Option<DBResult<Arc<Tab>>> {
        let mut tab = FDBTab::new(tab);
        let db = tab.open();
        Some(Ok(Arc::new(FDB(db))))
    }
	// 检查该表是否可以创建
	fn check(
		&self,
		tab: &Atom,
		meta: &Arc<StructInfo>,
	) -> DBResult<()> {
        Ok(())
    }
	// 创建一个meta事务
	fn transaction(&self, id: &Guid, _timeout: usize) -> Arc<MetaTxn> {
        let txn_opts = TransactionOptions::default();
        let write_opts = WriteOptions::default();
        let txn_db = &self.0;
        let rocksdb_txn = TXN::begin(txn_db, &write_opts, &txn_opts).unwrap();
        let mut txn_name = String::from("rocksdb_");
        txn_name.push_str(now_nanos().to_string().as_str());
        match rocksdb_txn.set_name(&txn_name) {
            Ok(_) => println!("set_name ok!!!!!"),
            Err(e) => println!("set_name err:{}", e.to_string()),
        };
        let txn = MutexFdbTxn(Arc::new(Mutex::new(
            FdbTxn {
                id: id.clone(),
                txn: rocksdb_txn,
                state: TxState::Ok
            }
        )));
        
		return Arc::new(txn)
    }
}

//迭代器方法
impl Cursor for FDBIterator {
    fn state(&self) -> DBResult<bool> {
        Ok(self.0.valid())
    }
    fn key(&self) -> Arc<Vec<u8>> {
        Arc::new(self.0.key().unwrap())
    }
    fn value(&self) -> Option<Arc<Vec<u8>>> {
        Some(Arc::new(self.0.value().unwrap()))
    }
    fn next(&mut self) {
        self.0.next()
    }
}