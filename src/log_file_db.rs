
use std::sync::{Arc, Mutex, RwLock};
use std::cell::RefCell;
use std::mem;
use std::path::{Path, PathBuf};
use std::fs::{self, read_to_string};
use std::time::Instant;
use fnv::FnvHashMap;
use std::collections::{ BTreeMap, HashMap };
use std::env;
use std::collections::HashSet;
use std::sync::atomic::{ Ordering, AtomicIsize };
use std::io::Result;

use ordmap::ordmap::{OrdMap, Entry, Iter as OIter, Keys};
use ordmap::asbtree::{Tree};
use atom::{Atom};
use guid::Guid;
use hash::XHashMap;

use crossbeam_channel::{unbounded, Receiver, Sender};

use pi_db::db::{EventType, Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult, NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn, Tab, OpenTab, Event, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult, RwLog, Bon, TabMeta, TxCbWrapper};
use pi_db::tabs::{TabLog, Tabs, Prepare};
use pi_db::mgr::SEQ_CHAN;
use crate::log_store::log_file::{PairLoader, LogMethod, LogFile};

use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::lock::spin_lock::SpinLock;

use nodec::rpc::RPCClient;
use bon::{ ReadBuffer, WriteBuffer };
use json;

lazy_static! {
	static ref BACKUP_TABLES: RwLock<HashSet<(String, String)>> =  RwLock::new(HashSet::new());
	pub static ref STORE_RUNTIME: MultiTaskRuntime<()> = {
        let pool = MultiTaskPool::new("File-Runtime".to_string(), 2, 1024 * 1024, 10, Some(10));
        pool.startup(true)
    };
}

const BACKUP_TABLE_PATH: &str = "pi_pt/util/hotback.BackupTable";

/**
* 基于file log的数据库
*/
#[derive(Clone)]
pub struct LogFileDB(Arc<RwLock<Tabs<LogFileTab>>>);

impl LogFileDB {
	/**
	* 构建基于file log的数据库
	* @param db_path 数据库路径
	* @param db_size 数据库文件最大大小
	* @returns 返回基于file log的数据库
	*/
	pub fn new(db_path: Atom, db_size: usize) -> Self {
		env::set_var("DB_PATH", db_path.to_string());
		if !Path::new(&db_path.to_string()).exists() {
            let _ = fs::create_dir(db_path.to_string());
		}

		let projs = env::var("PROJECTS").unwrap().split(" ").map(|s|s.to_string()).collect::<Vec<String>>();
		let proj_root = env::var("PROJECT_ROOT").unwrap();

        let mut backups = BACKUP_TABLES.write().unwrap();
        let mut is_master = false;
		for p in projs {
			let path = Path::new(&proj_root).join(p).join("ptconfig.json");

			if let Ok(content) = read_to_string(path) {
				match json::parse(&content) {
					Ok(jobj) => {
                        if jobj["BackupTables"].members().len() > 0 {
                            is_master = true;
                        }
						for backup in jobj["BackupTables"].members() {
							let ware = backup["ware"].as_str().unwrap().to_string();
							for tab in backup["tabs"].members() {
								// 读取需要备份的表名到全局变量中
								println!("backup ware = {:?}, tab = {:?}", ware, tab);
								backups.insert((ware.clone(), tab.as_str().unwrap().to_string()));
							}
						}
					}
					Err(e) => {
						panic!("please makes sure ptconfig.json is a valid json file, error： {:?}", e);
					}
				}
			}
        }
        // 如果是master节点，加载备份表
        if is_master {
            let db_path = env::var("DB_PATH").unwrap();
            let mut path = PathBuf::new();
            path.push(db_path);
            path.push(BACKUP_TABLE_PATH);
            let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
                match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024).await {
                    Ok(store) => {
                        match store.last_key() {
                            Some(key) => {
                                let mut rb = ReadBuffer::new(&key, 0);
                                let seq = rb.read_u64().unwrap();
                                let _ = SEQ_CHAN.0.send(seq+1);
                            }
                            None => {
                                let _ = SEQ_CHAN.0.send(0);
                            }
                        }
                    }
                    Err(e) => {
                        error!("open back up table failed, error = {:?}", e);
                    }
                }
            });
        }

		LogFileDB(Arc::new(RwLock::new(Tabs::new())))
	}
}

impl OpenTab for LogFileDB {
	// 打开指定的表，表必须有meta
	fn open<'a, T: Tab>(&self, tab: &Atom, _cb: Box<Fn(SResult<T>) + 'a>) -> Option<SResult<T>> {
		let mut table = T::new(tab);
		Some(Ok(table))
	}
}
impl Ware for LogFileDB {
	// 拷贝全部的表
	fn tabs_clone(&self) -> Arc<Ware> {
		Arc::new(LogFileDB(Arc::new(RwLock::new(self.0.read().unwrap().clone_map()))))
	}
	// 列出全部的表
	fn list(&self) -> Box<Iterator<Item=Atom>> {
		Box::new(self.0.read().unwrap().list())
	}
	// 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	fn timeout(&self) -> usize {
		TIMEOUT
	}
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<TabMeta>> {
		self.0.read().unwrap().get(tab_name)
	}
	// 获取当前表结构快照
	fn snapshot(&self) -> Arc<WareSnapshot> {
		Arc::new(DBSnapshot(self.clone(), RefCell::new(self.0.read().unwrap().snapshot())))
	}
}

// 内存库快照
pub struct DBSnapshot(LogFileDB, RefCell<TabLog<LogFileTab>>);

impl WareSnapshot for DBSnapshot {
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
	fn meta_txn(&self, _id: &Guid) -> Arc<MetaTxn> {
		Arc::new(MemeryMetaTxn())
	}
	// 元信息的预提交
	fn prepare(&self, id: &Guid) -> SResult<()>{
		(self.0).0.write().unwrap().prepare(id, &mut self.1.borrow_mut())
	}
	// 元信息的提交
	fn commit(&self, id: &Guid){
		(self.0).0.write().unwrap().commit(id)
	}
	// 回滚
	fn rollback(&self, id: &Guid){
		(self.0).0.write().unwrap().rollback(id)
	}
	// 库修改通知
	fn notify(&self, event: Event) {
		debug!("notify ---- {:?}", event);
		let Event { seq, ware, tab, other} = event;
		match other {
			EventType::Tab { key, value} => {
				// 只备份指定的表, 将备份的数据保存到 BACKUP_TABLE_PATH 中
				if should_backup(ware.clone().as_str(), tab.clone().as_str()) {
					let db_path = env::var("DB_PATH").unwrap();
					let mut path = PathBuf::new();
					path.push(db_path);
					path.push(BACKUP_TABLE_PATH);
					let value1 = value.clone();
					let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
						match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024).await {
							Err(e) => {
								error!("!!!!!!open table = {:?} failed, e: {:?}", BACKUP_TABLE_PATH, e);
							},
							Ok(store) => {
								let mut wb = WriteBuffer::new();
								wb.write_utf8(ware.as_str());
								wb.write_utf8(tab.as_str());
								wb.write_bin(&key, 0..key.len());

								let mut kb = WriteBuffer::new();
								kb.write_u64(seq);
	
								if value1.is_some() {
									wb.write_bin(&value1.clone().unwrap(), 0..value1.clone().unwrap().len());
									let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
										let _ = store.write(kb.get_byte().clone(), wb.get_byte().clone()).await;
									});
								} else {
									let mut bb = WriteBuffer::new();
									bb.write_nil();
									let bin = bb.get_byte();
									wb.write_bin(bin, 0..bin.len());

									let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
										let _ = store.write(kb.get_byte().clone(), wb.get_byte().clone()) .await;
									});
								}
							}
						}
					});
				}
			}

			_ => {}
		}
	}
}

// 内存事务
pub struct FileMemTxn {
	id: Guid,
	writable: bool,
	tab: LogFileTab,
	root: BinMap,
	old: BinMap,
	rwlog: FnvHashMap<Bin, RwLog>,
	state: TxState,
}

// pub type RefMemeryTxn = RefCell<FileMemTxn>;

pub struct RefMemeryTxn(RefCell<FileMemTxn>);

unsafe impl Sync for RefMemeryTxn  {}

impl FileMemTxn {
	//开始事务
	pub fn new(tab: LogFileTab, id: &Guid, writable: bool) -> RefMemeryTxn {
		let root = tab.0.lock().unwrap().root.clone();
		let txn = FileMemTxn {
			id: id.clone(),
			writable: writable,
			root: root.clone(),
			tab: tab,
			old: root,
			rwlog: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			state: TxState::Ok,
		};
		return RefMemeryTxn(RefCell::new(txn))
	}
	//获取数据
	pub fn get(&mut self, key: Bin) -> Option<Bin> {
		match self.root.get(&Bon::new(key.clone())) {
			Some(v) => {
				if self.writable {
					match self.rwlog.get(&key) {
						Some(_) => (),
						None => {
							&mut self.rwlog.insert(key, RwLog::Read);
							()
						}
					}
				}

				return Some(v.clone())
			},
			None => return None
		}
	}
	//插入/修改数据
	pub fn upsert(&mut self, key: Bin, value: Bin) -> SResult<()> {
		self.root.upsert(Bon::new(key.clone()), value.clone(), false);
		self.rwlog.insert(key.clone(), RwLog::Write(Some(value.clone())));

		Ok(())
	}
	//删除
	pub fn delete(&mut self, key: Bin) -> SResult<()> {
		self.root.delete(&Bon::new(key.clone()), false);
		self.rwlog.insert(key, RwLog::Write(None));

		Ok(())
	}

	//预提交
	pub fn prepare1(&mut self) -> SResult<()> {
		let mut tab = self.tab.0.lock().unwrap();
		//遍历事务中的读写日志
		for (key, rw_v) in self.rwlog.iter() {
			//检查预提交是否冲突
			match tab.prepare.try_prepare(key, rw_v) {
				Ok(_) => (),
				Err(s) => return Err(s),
			};
			//检查Tab根节点是否改变
			if tab.root.ptr_eq(&self.old) == false {
				let key = Bon::new(key.clone());
				match tab.root.get(&key) {
					Some(r1) => match self.old.get(&key) {
						Some(r2) if (r1 as *const Bin) == (r2 as *const Bin) => (),
						_ => return Err(String::from("parpare conflicted value diff"))
					},
					_ => match self.old.get(&key) {
						None => (),
						_ => return Err(String::from("parpare conflicted old not None"))
					}
				}
			}
		}
		let rwlog = mem::replace(&mut self.rwlog, FnvHashMap::with_capacity_and_hasher(0, Default::default()));
		//写入预提交
		tab.prepare.insert(self.id.clone(), rwlog);

		return Ok(())
	}

	// 同时异步存储数据到log file 中
	//提交
	pub fn commit1(&mut self, cb: TxCallback) -> CommitResult {
		let txcb_wrapper = TxCbWrapper(cb);
		let mut tab = self.tab.0.lock().unwrap();
		let log = match tab.prepare.remove(&self.id) {
			Some(rwlog) => {
				let root_if_eq = tab.root.ptr_eq(&self.old);
				//判断根节点是否相等
				if root_if_eq == false {
					for (k, rw_v) in rwlog.iter() {
						match rw_v {
							RwLog::Read => (),
							_ => {
								let k = Bon::new(k.clone());
								match rw_v {
									RwLog::Write(None) => {
										tab.root.delete(&k, false);
									},
									RwLog::Write(Some(v)) => {
										tab.root.upsert(k.clone(), v.clone(), false);
									},
									_ => (),
								}
							},
						}
					}
				} else {
					tab.root = self.root.clone();
				}
				rwlog
			},
			None => return Some(Err(String::from("error prepare null")))
		};

		let async_tab = self.tab.1.clone();

		let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
			let mut error = false;
			for (k, rw_v) in log {
				match rw_v {
					RwLog::Read => {},
					_ => {
						match rw_v {
							RwLog::Write(None) => {
								debug!("delete key = {:?}", k);
								if let Err(_) =  async_tab.remove(k.to_vec()).await {
									error = true;
									break;
								}
							}
							RwLog::Write(Some(v)) => {
								debug!("insert k = {:?}, v = {:?}", k, v);
								if let Err(_) = async_tab.write(k.to_vec(), v.to_vec()).await {
									error = true;
									break;
								}
							}
							_ => {}
						}
					}
				}
			}

			if error {
				txcb_wrapper.0(Err("commit failed".to_string()));
			} else {
				txcb_wrapper.0(Ok(()));
			}
		});

		None
	}
	//回滚
	pub fn rollback1(&mut self) -> SResult<()> {
		let mut tab = self.tab.0.lock().unwrap();
		tab.prepare.remove(&self.id);

		Ok(())
	}
}

impl Txn for RefMemeryTxn {
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		self.0.borrow().state.clone()
	}
	// 预提交一个事务
	fn prepare(&self, _timeout: usize, _cb: TxCallback) -> DBResult {
		let mut txn = self.0.borrow_mut();
		txn.state = TxState::Preparing;
		match txn.prepare1() {
			Ok(()) => {
				txn.state = TxState::PreparOk;
				return Some(Ok(()))
			},
			Err(e) => {
				txn.state = TxState::PreparFail;
				return Some(Err(e.to_string()))
			},
		}
	}
	// 提交一个事务
	fn commit(&self, cb: TxCallback) -> CommitResult {
		let mut txn = self.0.borrow_mut();
		txn.state = TxState::Committing;
		match txn.commit1(cb) {
			Some(Ok(log)) => {
				txn.state = TxState::Commited;
				return Some(Ok(log))
			},
			Some(Err(e)) => {
				return Some(Err(e.to_string()))
			}
			None => None
		}
	}
	// 回滚一个事务
	fn rollback(&self, _cb: TxCallback) -> DBResult {
		let mut txn = self.0.borrow_mut();
		txn.state = TxState::Rollbacking;
		match txn.rollback1() {
			Ok(()) => {
				txn.state = TxState::Rollbacked;
				return Some(Ok(()))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}
}

impl TabTxn for RefMemeryTxn {
	// 键锁，key可以不存在，根据lock_time的值决定是锁还是解锁
	fn key_lock(&self, _arr: Arc<Vec<TabKV>>, _lock_time: usize, _readonly: bool, _cb: TxCallback) -> DBResult {
		None
	}
	// 查询
	fn query(
		&self,
		arr: Arc<Vec<TabKV>>,
		_lock_time: Option<usize>,
		_readonly: bool,
		_cb: TxQueryCallback,
	) -> Option<SResult<Vec<TabKV>>> {
		let mut txn = self.0.borrow_mut();
		let mut value_arr = Vec::new();
		for tabkv in arr.iter() {
			let value = match txn.get(tabkv.key.clone()) {
				Some(v) => Some(v),
				_ => None
			};

			value_arr.push(
				TabKV{
				ware: tabkv.ware.clone(),
				tab: tabkv.tab.clone(),
				key: tabkv.key.clone(),
				index: tabkv.index.clone(),
				value: value,
				}
			)
		}
		Some(Ok(value_arr))
	}
	// 修改，插入、删除及更新
	fn modify(&self, arr: Arc<Vec<TabKV>>, _lock_time: Option<usize>, _readonly: bool, _cb: TxCallback) -> DBResult {
		let mut txn = self.0.borrow_mut();
		for tabkv in arr.iter() {
			if tabkv.value == None {
				match txn.delete(tabkv.key.clone()) {
				Ok(_) => (),
				Err(e) => 
					{
						return Some(Err(e.to_string()))
					},
				};
			} else {
				match txn.upsert(tabkv.key.clone(), tabkv.value.clone().unwrap()) {
				Ok(_) => (),
				Err(e) =>
					{
						return Some(Err(e.to_string()))
					},
				};
			}
		}
		Some(Ok(()))
	}
	// 迭代
	fn iter(
		&self,
		tab: &Atom,
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		_cb: Arc<Fn(IterResult)>,
	) -> Option<IterResult> {
		let b = self.0.borrow_mut();
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};

		Some(Ok(Box::new(MemIter::new(tab, b.root.clone(), b.root.iter( key, descending), filter))))
	}
	// 迭代
	fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		_cb: Arc<Fn(KeyIterResult)>,
	) -> Option<KeyIterResult> {
		let b = self.0.borrow_mut();
		let key = match key {
			Some(k) => Some(Bon::new(k)),
			None => None,
		};
		let key = match &key {
			&Some(ref k) => Some(k),
			None => None,
		};
		let tab = b.tab.0.lock().unwrap().tab.clone();
		Some(Ok(Box::new(MemKeyIter::new(&tab, b.root.clone(), b.root.keys(key, descending), filter))))
	}
	// 索引迭代
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
	// 表的大小
	fn tab_size(&self, _cb: Arc<Fn(SResult<usize>)>) -> Option<SResult<usize>> {
		let txn = self.0.borrow();
		Some(Ok(txn.root.size()))
	}
}





//================================ 内部结构和方法
const TIMEOUT: usize = 100;


type BinMap = OrdMap<Tree<Bon, Bin>>;

// 内存表
struct MemeryTab {
	pub prepare: Prepare,
	pub root: BinMap,
	pub tab: Atom,
}

pub struct MemIter{
	_root: BinMap,
	_filter: Filter,
	point: usize,
}

impl Drop for MemIter{
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType)};
    }
}

impl MemIter{
	pub fn new<'a>(tab: &Atom, root: BinMap, it: <Tree<Bon, Bin> as OIter<'a>>::IterType, filter: Filter) -> MemIter{
		MemIter{
			_root: root,
			_filter: filter,
			point: Box::into_raw(Box::new(it)) as usize,
		}
	}
}

impl Iter for MemIter{
	type Item = (Bin, Bin);
	fn next(&mut self, _cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>>{

		let mut it = unsafe{Box::from_raw(self.point as *mut <Tree<Bin, Bin> as OIter<'_>>::IterType)};
		let r = Some(Ok(match it.next() {
			Some(&Entry(ref k, ref v)) => {
				Some((k.clone(), v.clone()))
			},
			None => None,
		}));
		mem::forget(it);
		r
	}
}

pub struct MemKeyIter{
	_root: BinMap,
	_filter: Filter,
	point: usize,
}

impl Drop for MemKeyIter{
	fn drop(&mut self) {
        unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)};
    }
}

impl MemKeyIter{
	pub fn new(tab: &Atom, root: BinMap, keys: Keys<'_, Tree<Bon, Bin>>, filter: Filter) -> MemKeyIter{
		MemKeyIter{
			_root: root,
			_filter: filter,
			point: Box::into_raw(Box::new(keys)) as usize,
		}
	}
}

impl Iter for MemKeyIter{
	type Item = Bin;
	fn next(&mut self, _cb: Arc<Fn(NextResult<Self::Item>)>) -> Option<NextResult<Self::Item>>{
		let it = unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)};
		let r = Some(Ok(match unsafe{Box::from_raw(self.point as *mut Keys<'_, Tree<Bin, Bin>>)}.next() {
			Some(k) => {
				Some(k.clone())
			},
			None => None,
		}));
		mem::forget(it);
		r
	}
}

#[derive(Clone)]
pub struct MemeryMetaTxn();

impl MetaTxn for MemeryMetaTxn {
	// 创建表、修改指定表的元数据
	fn alter(&self, _tab: &Atom, _meta: Option<Arc<TabMeta>>, _cb: TxCallback) -> DBResult{
		Some(Ok(()))
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
impl Txn for MemeryMetaTxn {
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		TxState::Ok
	}
	// 预提交一个事务
	fn prepare(&self, _timeout: usize, _cb: TxCallback) -> DBResult {
		Some(Ok(()))
	}
	// 提交一个事务
	fn commit(&self, _cb: TxCallback) -> CommitResult {
		Some(Ok(FnvHashMap::with_capacity_and_hasher(0, Default::default())))
	}
	// 回滚一个事务
	fn rollback(&self, _cb: TxCallback) -> DBResult {
		Some(Ok(()))
	}
}

#[derive(Clone)]
struct AsyncLogFileStore {
	removed: Arc<SpinLock<XHashMap<Vec<u8>, ()>>>,
	map: Arc<SpinLock<BTreeMap<Vec<u8>, Arc<[u8]>>>>,
	log_file: LogFile
}

unsafe impl Send for AsyncLogFileStore {}
unsafe impl Sync for AsyncLogFileStore {}

impl PairLoader for AsyncLogFileStore {
    fn is_require(&self, key: &Vec<u8>) -> bool {
		!self.removed.lock().contains_key(key) && !self.map.lock().contains_key(key)
    }

    fn load(&mut self, _method: LogMethod, key: Vec<u8>, value: Option<Vec<u8>>) {
		if let Some(value) = value {
			self.map.lock().insert(key, value.into());
		} else {
			self.removed.lock().insert(key, ());
		}
    }
}

impl AsyncLogFileStore {
	async fn open<P: AsRef<Path> + std::fmt::Debug>(path: P, buf_len: usize, file_len: usize) -> Result<Self> {
		match LogFile::open(STORE_RUNTIME.clone(), path, buf_len, file_len).await {
            Err(e) => Err(e),
            Ok(file) => {
                //打开指定路径的日志存储成功
                let mut store = AsyncLogFileStore {
					removed: Arc::new(SpinLock::new(XHashMap::default())),
					map: Arc::new(SpinLock::new(BTreeMap::new())),
					log_file: file.clone()
				};

                if let Err(e) = file.load(&mut store, true).await {
                    Err(e)
                } else {
                    //初始化内存数据成功
                    Ok(store)
                }
            },
        }
	}

	async fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let id = self.log_file.append(LogMethod::PlainAppend, key.as_ref(), value.as_ref());
        if let Err(e) = self.log_file.delay_commit(id, 10).await {
            Err(e)
        } else {
            if let Some(value) = self.map.lock().insert(key, value.into()) {
                //更新指定key的存储数据，则返回更新前的存储数据
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
	}
	
	fn read(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        if let Some(value) = self.map.lock().get(key) {
            return Some(value.clone())
        }

        None
	}

	pub async fn remove(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let id = self.log_file.append(LogMethod::Remove, key.as_ref(), &[]);
        if let Err(e) = self.log_file.delay_commit(id, 10).await {
            Err(e)
        } else {
            if let Some(value) = self.map.lock().remove(&key) {
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    fn last_key(&self) -> Option<Vec<u8>> {
        self.map.lock().iter().last().map(|(k, _)| {
            k.clone()
        })
    }
}

fn should_backup(ware_name: &str, tab_name: &str) -> bool {
	BACKUP_TABLES.read().unwrap().contains(&(ware_name.to_string(), tab_name.to_string()))
}

#[derive(Clone)]
pub struct LogFileTab(Arc<Mutex<MemeryTab>>, AsyncLogFileStore);

unsafe impl Send for LogFileTab {}
unsafe impl Sync for LogFileTab {}

impl Tab for LogFileTab {
	fn new(tab: &Atom) -> Self {
		let mut file_mem_tab = MemeryTab {
			prepare: Prepare::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
			root: OrdMap::<Tree<Bon, Bin>>::new(None),
			tab: tab.clone(),
		};

		let db_path = env::var("DB_PATH").unwrap();
		let mut path = PathBuf::new();
		let tab_name = tab.clone();
		let tab_name_clone = tab.clone();
		path.push(db_path);
		path.push(tab_name.clone().to_string());
		let (s, r) = unbounded();

		// 异步加载数据， 通过channel 同步
		let _ = STORE_RUNTIME.spawn(STORE_RUNTIME.alloc(), async move {
			match AsyncLogFileStore::open(path, 8000, 200 * 1024 * 1024).await {
				Err(e) => {
					error!("!!!!!!open table = {:?} failed, e: {:?}", tab_name, e);
				},
				Ok(store) => {
					// 数据加载完毕，通过channel返回句柄
					let _ = s.send(store);
				}
			}
		});
		
		match r.recv() {
			Ok(store) => {
				let mut root= OrdMap::<Tree<Bon, Bin>>::new(None);
				let mut load_size = 0;
				let start_time = Instant::now();
				let map = store.map.lock();
				for (k, v) in map.iter() {
					load_size += v.len();
					root.upsert(Bon::new(Arc::new(k.clone())), Arc::new(v.to_vec()), false);
				}
				file_mem_tab.root = root;
				debug!("====> load tab: {:?} size: {:?}byte time elapsed: {:?} <====", tab_name_clone, load_size, start_time.elapsed());

				return LogFileTab(Arc::new(Mutex::new(file_mem_tab)), store);
			}
			Err(e) => {
				panic!("LogFileTab::new failed, error = {:?}", e);
			}
		}

	}
	fn transaction(&self, id: &Guid, writable: bool) -> Arc<dyn TabTxn> {
		let txn = Arc::new(FileMemTxn::new(self.clone(), id, writable));
		return txn
	}
}
