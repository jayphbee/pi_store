
use rocksdb::{TXN_DB, TXN, Options, TransactionDBOptions, TransactionOptions, ReadOptions, WriteOptions, DBRawIterator};
use pi_db::db::{Txn, DBResult, TabKV, UsizeResult, TxQueryCallback, TxCallback, Cursor, TxIterCallback, TxState};
use pi_db::mgr::{Mgr};
use pi_lib::sinfo::{StructInfo};
use pi_lib::atom::{Atom};

use std::boxed::FnBox;
use std::sync::{Arc, Mutex, Condvar};

use std::string::String;
use std::str;
use std::convert::From;
use std::vec::Vec;
use std::usize;

use pi_vm::task::TaskType;
use pi_vm::task_pool::TaskPool;

pub struct LocalTXN {
    pub txn: TXN,
}

pub struct ASYNC_TXN {
    pub txn: TXN,
}

pub struct LocalDBRawIterator {
    pub iter: DBRawIterator,
}

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

impl Txn for LocalTXN {
    // 获得事务的状态
    fn get_state(&self) -> TxState {
        TxState::Ok
    }
    // 预提交一个事务
    //TODO txn.clone是非安全的
    fn prepare(&mut self, cb: TxCallback) -> UsizeResult {
        let mut txn = self.txn.clone();
        let func = move || {
        match txn.prepare() {
            Ok(()) => cb(Ok(1)),
            Err(e) => cb(Err(e.to_string())),
        }
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
        
    }
    // 提交一个事务
    fn commit(&mut self, cb: TxCallback) -> UsizeResult {
        let mut txn = self.txn.clone();
        let func = move || {
        match txn.commit() {
            Ok(()) => cb(Ok(1)),
            Err(e) => cb(Err(e.to_string())),
        }
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
    }
    // 回滚一个事务
    fn rollback(&mut self, cb: TxCallback) -> UsizeResult {
        let mut txn = self.txn.clone();
        let func = move || {
        match txn.rollback() {
            Ok(()) => cb(Ok(1)),
            Err(e) => cb(Err(e.to_string())),
        }
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
    }
    // 锁
	fn klock(&mut self, arr:Vec<TabKV>, lock_time:usize, cb: TxCallback) -> UsizeResult {
        // (*cb)(Some(Ok(()))) //TODO 临时
        Some(Ok(1))
    }
    // 查询
    fn query(&mut self, arr:Vec<TabKV>, lock_time: Option<usize>, cb: TxQueryCallback) -> Option<DBResult<Vec<TabKV>>> {
        let mut txn = self.txn.clone();
        let func = move || {
            let mut valueArr = Vec::new();
            // let tab = self.txn.path.clone().into_os_string().into_string().unwrap();
            for tabkv in arr {
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
                valueArr.push(
                    TabKV{
                    tab: tabkv.tab,
                    key: tabkv.key.clone(),
                    index: tabkv.index,
                    value: value,
                    }
                )
            }
            // (*cb)(Some(Ok(valueArr)))
            cb(Ok(valueArr))
            
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
        // let mut valueArr = Vec::new();
        // // let tab = self.txn.path.clone().into_os_string().into_string().unwrap();
        // for tabkv in arr {
        //     let read_opts = ReadOptions::default();
        //     let mut value = None;
        //     match self.txn.get(&read_opts, tabkv.key.as_slice()) {
        //                 Ok(None) => (),
        //                 Ok(v) => 
        //                     {
        //                         value = Some(Arc::new(v.unwrap().to_utf8().unwrap().as_bytes().to_vec()));
        //                         ()
        //                     },
        //                 Err(e) => 
        //                     return Some(Err(e.to_string())),
        //             }
        //     valueArr.push(
        //         TabKV{
        //         tab: tabkv.tab,
        //         key: tabkv.key.clone(),
        //         index: tabkv.index,
        //         value: value,
        //         }
        //     )
        // }
        // // (*cb)(Some(Ok(valueArr)))
        // Some(Ok(valueArr))
    }
    // 修改，插入、删除及更新
    fn modify(&mut self, arr: Vec<TabKV>, lock_time:Option<usize>, cb: TxCallback) -> UsizeResult {
        println!("db.modify!!!!!!!!!!!");
        let mut txn = self.txn.clone();
        let func = move || {
            println!("db.modify!!!!!!!!!!!22222222222");
            let len = arr.len();
            for tabkv in arr {
                // let key = &TableKey.key.as_slice();
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
                    match txn.put(&tabkv.key.as_slice(), &tabkv.value.unwrap().as_slice()) {
                    Ok(_) => (),
                    Err(e) =>
                        {
                            cb(Err(e.to_string()));
                            return;
                        },
                    };
                }
            }
            println!("db.modify!!!!!!!!!!!333333333");
            // (*cb)(Ok(()))
            cb(Ok(len))
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
        // let len = arr.len();
        // for tabkv in arr {
        //     // let key = &TableKey.key.as_slice();
        //     if tabkv.value == None {
        //         match self.txn.delete(&tabkv.key.as_slice()) {
        //         Ok(_) => (),
        //         Err(e) => return Some(Err(e.to_string())),
        //         };
        //     } else {
        //         match self.txn.put(&tabkv.key.as_slice(), &tabkv.value.unwrap().as_slice()) {
        //         Ok(_) => (),
        //         Err(e) => return Some(Err(e.to_string())),
        //         };
        //     }
        // }
        // // (*cb)(Ok(()))
        // Some(Ok(len))
    }
    // 迭代表
    fn iter(&mut self, 
    tab: Atom, 
    key: Option<Vec<u8>>, 
    descending: bool, 
    key_only:bool, 
    filter:String, 
    cb: TxIterCallback) -> Option<DBResult<Box<Cursor>>> {
        let mut txn = self.txn.clone();
        let func = move || {
            let read_opts = ReadOptions::default();
            let mut iter = txn.iter(&read_opts);
            if key == None {
                if descending {
                    iter.seek_to_last();
                } else {
                    iter.seek_to_first();
                }
            } else {
                if descending {
                    iter.seek_for_prev(key.unwrap().as_slice());
                } else {
                    iter.seek(key.unwrap().as_slice());
                }
            }
            cb(Ok(
                Box::new(LocalDBRawIterator {
                    iter
                }))
            )
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
        // let read_opts = ReadOptions::default();
        // let mut iter = self.txn.iter(&read_opts);
        // if key == None {
        //     if descending {
        //         iter.seek_to_last();
        //     } else {
        //         iter.seek_to_first();
        //     }
        // } else {
        //     if descending {
        //         iter.seek_for_prev(key.unwrap().as_slice());
        //     } else {
        //         iter.seek(key.unwrap().as_slice());
        //     }
        // }
        // Some(Ok(
        //     Box::new(LocalDBRawIterator {
        //         iter
        //     }))
        // )
    }
    // 迭代索引
	fn index(
        &mut self,
		tab: Atom,
		key: Option<Vec<u8>>,
		descending: bool,
		filter: String,
		cb: TxIterCallback,
    ) -> Option<DBResult<Box<Cursor>>> {
        let mut txn = self.txn.clone();
        let func = move || {
            let read_opts = ReadOptions::default();
            let mut iter = txn.iter(&read_opts);
            if key == None {
                if descending {
                    iter.seek_to_last();
                } else {
                    iter.seek_to_first();
                }
            } else {
                if descending {
                    iter.seek_for_prev(key.unwrap().as_slice());
                } else {
                    iter.seek(key.unwrap().as_slice());
                }
            }
            cb(Ok(
                Box::new(LocalDBRawIterator {
                    iter
                }))
            )
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
        // //TODO 暂不实现
        // let read_opts = ReadOptions::default();
        // let mut iter = self.txn.iter(&read_opts);
        // if key == None {
        //     if descending {
        //         iter.seek_to_last();
        //     } else {
        //         iter.seek_to_first();
        //     }
        // } else {
        //     if descending {
        //         iter.seek_for_prev(key.unwrap().as_slice());
        //     } else {
        //         iter.seek(key.unwrap().as_slice());
        //     }
        // }
        // Some(Ok(
        //     Box::new(LocalDBRawIterator {
        //         iter
        //     }))
        // )
}
    fn tab_size(&mut self, tab: Atom, cb: TxCallback) -> UsizeResult {
        let func = move || {
           cb(Ok(usize::max_value()))
        };
        let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
        let mut task_pool = lock.lock().unwrap();
        (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
        cvar.notify_one();
        None
        // Some(Ok(usize::max_value()))
    }

}

impl Cursor for LocalDBRawIterator {
    fn state(&self) -> DBResult<bool> {
        Ok(self.iter.valid())
    }
    fn key(&self) -> &[u8] {
        unsafe {self.iter.key_inner().unwrap()}
    }
    fn value(&self) -> Option<Arc<Vec<u8>>> {
        Some(Arc::new(self.iter.value().unwrap()))
    }
    fn next(&mut self) {
        self.iter.next()
    }
}

// fn prepare(txn: TXN,  callback: TxCallback) {
//     let func = move || {
//         match txn.prepare() {
//             Ok(()) => callback(Ok(1)),
//             Err(e) => callback(Err(e.to_string())),
//         }
//     };
//     let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
//     let mut task_pool = lock.lock().unwrap();
//     (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
//     cvar.notify_one();
// }

// fn commit(txn: TXN,  callback: Arc<Fn(Result<(), String>)>) {
//     let func = move || {
//         match txn.commit() {
//             Ok(()) => callback(Ok(())),
//             Err(e) => callback(Err(e.to_string())),
//         }
//     };
//     let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
//     let mut task_pool = lock.lock().unwrap();
//     (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
//     cvar.notify_one();
// }

// fn rollback(txn: TXN,  callback: Arc<Fn(Result<(), String>)>) {
//     let func = move || {
//         match txn.rollback() {
//             Ok(()) => callback(Ok(())),
//             Err(e) => callback(Err(e.to_string())),
//         }
//     };
//     let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
//     let mut task_pool = lock.lock().unwrap();
//     (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
//     cvar.notify_one();
// }

// fn get(txn: TXN, opts: &ReadOptions, key: &[u8],  callback: Arc<Fn(Result<Option<DBVector>, Error>)>) {
//     let func = move || {
//         match txn.get(opts, key) {
//             Ok(()) => callback(Ok(())),
//             Err(e) => callback(Err(e.to_string())),
//         }
//     };
//     let &(ref lock, ref cvar) = &**STORE_TASK_POOL;
//     let mut task_pool = lock.lock().unwrap();
//     (*task_pool).push(ASYNC_DB_TYPE, DB_PRIORITY, Box::new(func), DB_ASYNC_FILE_INFO);
//     cvar.notify_one();
// }