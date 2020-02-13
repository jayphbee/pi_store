
use std::sync::{Arc, Mutex, RwLock};
use std::cell::RefCell;
use std::mem;
use std::path::Path;
use std::fs::{self, read_to_string};
use std::thread;
use std::time::Instant;
use fnv::FnvHashMap;
use std::collections::HashMap;
use std::env;
use std::collections::HashSet;
use std::sync::atomic::{ Ordering, AtomicIsize };

use ordmap::ordmap::{OrdMap, Entry, Iter as OIter, Keys};
use ordmap::asbtree::{Tree};
use atom::{Atom};
use guid::Guid;
use handler::GenType;

use crossbeam_channel::{unbounded, Receiver, Sender};

use pi_db::db::{EventType, Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult, NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn, Tab, OpenTab, Event, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult, RwLog, Bon, TabMeta};
use pi_db::tabs::{TabLog, Tabs, Prepare};

use lmdb::{ Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags, Error};

use nodec::rpc::RPCClient;
use bon::WriteBuffer;
use json;

lazy_static! {
	static ref BACKUP_TABLES: RwLock<HashSet<(String, String)>> =  RwLock::new(HashSet::new());
	static ref SLAVE_ADDRS: RwLock<HashMap<String, Arc<AtomicIsize>>> = RwLock::new(HashMap::new());
}

const MAX_DBS_PER_ENV: u32 = 1024;

fn should_backup(ware_name: &str, tab_name: &str) -> bool {
	BACKUP_TABLES.read().unwrap().contains(&(ware_name.to_string(), tab_name.to_string()))
}

#[derive(Clone)]
pub struct FileMemTab(Arc<Mutex<MemeryTab>>, Option<Sender<LmdbMsg>>);
impl Tab for FileMemTab {
	fn new(tab: &Atom) -> Self {
		// 创建一个空数据表，稍后在 set_param 里面填充具体的数据
		let file_mem_tab = MemeryTab {
			prepare: Prepare::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
			root: OrdMap::<Tree<Bon, Bin>>::new(None),
			tab: tab.clone(),
		};

		return FileMemTab(Arc::new(Mutex::new(file_mem_tab)), None);
	}
	fn transaction(&self, id: &Guid, writable: bool) -> Arc<dyn TabTxn> {
		let txn = Arc::new(FileMemTxn::new(self.clone(), id, writable));
		return txn
	}

	// 通过这个函数来得到sender, sender 发送 LmdbMsg::CreateTab 消息给数据库线程，
	// lmdb 把数据读出来之后放到 OrdMap 中, 通过sender返回OrdMap的 root， 覆盖原来的root
	fn set_param(&mut self, t: GenType) {
		match t {
			GenType::USize(x) => {
				let sender = unsafe { *Box::from_raw(x as *mut Sender<LmdbMsg>) };
				self.1 = Some(sender.clone());

				let tab_name = self.0.lock().unwrap().tab.clone();
				let (s, r) = unbounded();
				let _ = sender.send(LmdbMsg::CreateTab(tab_name, s));
				match r.recv() {
					Ok(root) => {
						// 把从lmdb中存储的数据加载到内存表，替换原来的root
						self.0.lock().unwrap().root = root;
					}
					Err(e) => {
						error!("Can't receive OrdMap root, error: {:?}", e);
					}
				}
			}
			_ => {

			}
		};
	}
}

fn load_data_to_mem_tab(env: Arc<Environment>, file_tab: &Atom, root: &mut OrdMap<Tree<Bon, Bin>>) {
	if let Ok(db) = env.clone().open_db(Some(file_tab.as_str())) {
		let lmdb_txn = env.begin_ro_txn().unwrap();
		let mut cursor = lmdb_txn.open_ro_cursor(db).unwrap();
		let mut load_size = 0;
		let start_time = Instant::now();
		for (k, v) in cursor.iter() {
			if v.len() > 0 {
				load_size += v.len();
				root.upsert(Bon::new(Arc::new(k.to_vec())), Arc::new(v.to_vec()), false);
			}
		}

		drop(cursor);
		match lmdb_txn.commit(){
			Ok(_) => {}
			Err(_) => {
				error!("lmdb commit failed for tab: {:?}", file_tab);
			}
		}

		debug!("====> load tab: {:?} size: {:?}byte time elapsed: {:?} <====", file_tab, load_size, start_time.elapsed());
	} else {
		error!("====> load data from unknown tab: {:?} <====", file_tab);
	}
}

/**
* 基于内存的Lmdb数据库
*/
#[derive(Clone)]
pub struct FileMemDB(Arc<RwLock<Tabs<FileMemTab>>>, Sender<LmdbMsg>);

impl FileMemDB {
	/**
	* 构建基于内存的Lmdb数据库
	* @param db_path 数据库路径
	* @param db_size 数据库文件最大大小
	* @returns 返回基于内存的Lmdb数据库
	*/
	pub fn new(db_path: Atom, db_size: usize) -> Self {
		if !Path::new(&db_path.to_string()).exists() {
            let _ = fs::create_dir(db_path.to_string());
		}

		let projs = env::var("PROJECTS").unwrap().split(" ").map(|s|s.to_string()).collect::<Vec<String>>();
		let proj_root = env::var("PROJECT_ROOT").unwrap();

		let mut backups = BACKUP_TABLES.write().unwrap();
		let mut slave_addrs = SLAVE_ADDRS.write().unwrap();
		for p in projs {
			let path = Path::new(&proj_root).join(p).join("ptconfig.json");

			if let Ok(content) = read_to_string(path) {
				match json::parse(&content) {
					Ok(jobj) => {
						for backup in jobj["BackupTables"].members() {
							let ware = backup["ware"].as_str().unwrap().to_string();
							for tab in backup["tabs"].members() {
								// 读取需要备份的表名到全局变量中
								backups.insert((ware.clone(), tab.as_str().unwrap().to_string()));
							}
						}

						// 保存slave地址到全局变量中
						for addr in jobj["SlaveAddrs"].members() {
							slave_addrs.insert(format!("ws://{}", addr.as_str().unwrap().to_string()), Arc::new(AtomicIsize::new(0)));
						}
					}
					Err(e) => {
						panic!("please makes sure ptconfig.json is a valid json file, error： {:?}", e);
					}
				}
			}
		}

		let env = Arc::new(
			Environment::new()
				.set_max_dbs(MAX_DBS_PER_ENV)
				.set_map_size(db_size)
				.set_flags(EnvironmentFlags::NO_TLS)
				.open(Path::new(&db_path.to_string()))
				.expect("create lmdb env failed")
		);

		let (sender, receiver) = unbounded();

        let _  = thread::Builder::new().name("lmdb_writer_thread".to_string()).spawn(move || {
			let mut write_cache: HashMap<(Atom, Atom, Bin), Option<Bin>> = HashMap::new();
			let mut dbs = HashMap::new();

            loop {
                match receiver.recv() {
					Ok(LmdbMsg::CreateTab(tab_name, sender)) => {
						dbs.entry(tab_name.get_hash() as u64)
							.or_insert(
								env.create_db(Some(tab_name.clone().as_str()), DatabaseFlags::empty()).unwrap()
						);
						let mut root = OrdMap::<Tree<Bon, Bin>>::new(None);

						load_data_to_mem_tab(env.clone(), &tab_name, &mut root);
						let _ = sender.send(root);
					}
					Ok(LmdbMsg::SaveData(event)) => {
						debug!("====> save data for tab: {:?} <====", event.tab);
						match event.other {
							EventType::Tab {key, value} => {
								write_cache.insert((event.ware, event.tab, key), value);
							}
							_ => {}
						}
						save_data(&mut dbs, env.clone(), receiver.clone(), &mut write_cache);
					}

					Err(_) => {}
            	}
			}
        });

		FileMemDB(Arc::new(RwLock::new(Tabs::new())), sender)
	}
}

fn save_data(dbs: &mut HashMap<u64, Database>, env: Arc<Environment>, receiver: Receiver<LmdbMsg>, write_cache: &mut HashMap<(Atom, Atom, Bin), Option<Bin>>) {
	loop {
		match receiver.try_recv() {
			Ok(LmdbMsg::CreateTab(tab_name, sender)) => {
				dbs.entry(tab_name.get_hash() as u64)
					.or_insert(
						env.create_db(Some(tab_name.clone().as_str()), DatabaseFlags::empty()).unwrap()
				);
				let mut root = OrdMap::<Tree<Bon, Bin>>::new(None);
				load_data_to_mem_tab(env.clone(), &tab_name, &mut root);
				let _ = sender.send(root);
			}

			Ok(LmdbMsg::SaveData(event)) => {
				match event.other {
					EventType::Tab {key, value} => {
						write_cache.insert((event.ware, event.tab, key), value);
					}
					_ => {}
				}
			}

			Err(e) if e.is_empty() => {
				let start_time = Instant::now();
				let mut write_bytes = 0;
				let mut rw_txn = env.begin_rw_txn().unwrap();
				for ((ware, tab, key), val) in write_cache.iter() {
					if let Some(db) = dbs.get(&(tab.get_hash() as u64)) {
						if val.is_none() {
							match rw_txn.del(*db, key.as_ref(), None) {
								Ok(_) => {}
								Err(Error::NotFound) => {
									warn!("delete failed: key not found, key: {:?}, ware: {:?}, tab: {:?}", key, ware, tab);
								}
								Err(e) => {
									error!("delete failed, error: {:?} key: {:?} , ware: {:?}, tab:{:?}", e, key, ware, tab);
								}
							}
						} else {
							match rw_txn.put(*db, key.as_ref(), val.clone().unwrap().as_ref(), WriteFlags::empty()) {
								Ok(_) => {
									write_bytes += val.as_ref().unwrap().len();
								}
								Err(e) => {
									error!("file mem tab put fail {:?}, key: {:?}, val: {:?}, ware:{:?}, tab:{:?}", e, key, val, ware, tab);
								}
							}
						}
					}
				}

				match rw_txn.commit() {
					Ok(_) => {
						debug!("====> lmdb commit write {:?}bytes time: {:?} <====", write_bytes, start_time.elapsed());
						for ((ware, tab, key), val) in write_cache.drain() {
							if should_backup(ware.clone().as_str(), tab.clone().as_str()) {
								let slave_addrs = SLAVE_ADDRS.read().unwrap();
								for (addr, status) in slave_addrs.iter() {
									let ware1 = ware.clone();
									let tab1 = tab.clone();
									let key1 = key.clone();
									let val1 = val.clone();

									let status3 = status.clone();

									match RPCClient::create(&addr) {
										Err(e) => error!("SYNCDB create rpc client failed, client_name: {:?} e: {:?}", addr, e),
										Ok(client) => {
											info!("SYNCDB create rpc client ok, client_name: {:?}", addr);
											let client_copy = client.clone();
											let client_copy_copy = client.clone();
											let conn_name_copy = addr.clone();
											let conn_name_copy_copy = addr.clone();
											let status1 = status.clone();
											let connect_cb = Arc::new(move |result| {
												let status2 = status1.clone();
												let client_copy2 = client_copy.clone();
												let conn_name_copy = conn_name_copy.clone();
												info!("SYNCDB connect result: {:?}, client_name: {:?}", result, conn_name_copy);
												if let Ok(_) = result {
													// 设置状态为已连接
													status2.store(1, Ordering::SeqCst);
												} else {
													// 连接出错
													status2.store(-1, Ordering::SeqCst);
												}
												let request_cb = Arc::new(move |result| {
													info!("SYNCDB request result: {:?}, client_name: {:?}", result, conn_name_copy);
													if let Err(_) = result {
														// 请求出错则关闭连接
														client_copy2.close();
														// 请求出错也标记为不正常连接
														status2.store(-1, Ordering::SeqCst);
													}
												});
												let mut wb = WriteBuffer::new();
												wb.write_utf8(ware1.as_str());
												wb.write_utf8(tab1.as_str());
												wb.write_bin(&key1, 0..key1.len());

												if val1.is_some() {
													wb.write_bin(&val1.clone().unwrap(), 0..val1.clone().unwrap().len());
												} else {
													let mut bb = WriteBuffer::new();
													bb.write_nil();
													let bin = bb.get_byte();
													wb.write_bin(bin, 0..bin.len());
												}

												let mut wb2 = WriteBuffer::new();
												let bin = wb.get_byte();
												wb2.write_bin(bin, 0..bin.len());

												client_copy.request("pi_pt/util/hotback.syncDB".to_string(), wb2.get_byte().clone(), 10, request_cb.clone());
											});

											let conn_name_copy = addr.clone();
											let closed_cb = Arc::new(move |result| {
												debug!("SYNCDB rpc client closed, client_name: {:?} result: {:?}", &conn_name_copy, result);
											});

											// 第一次连接
											if status.load(Ordering::SeqCst) == 0 {
												client.connect(65535, &addr, 10, connect_cb, closed_cb);
											} else if status.load(Ordering::SeqCst) == 1 { // 已经连接成功了， 直接发送request
												let ware1 = ware.clone();
												let tab1 = tab.clone();
												let key1 = key.clone();
												let val1 = val.clone();
												let client_copy_copy_copy = client_copy_copy.clone();

												let request_cb = Arc::new(move |result| {
													info!("SYNCDB request result: {:?}, client_name: {:?}", result, conn_name_copy_copy);
													if let Err(_) = result {
														// 请求出错则关闭连接
														client_copy_copy.close();
														// 标记为不正常连接
														status3.store(-1, Ordering::SeqCst);
													}
												});
												let mut wb = WriteBuffer::new();
												wb.write_utf8(ware1.as_str());
												wb.write_utf8(tab1.as_str());
												wb.write_bin(&key1, 0..key1.len());

												if val1.is_some() {
													wb.write_bin(&val1.clone().unwrap(), 0..val1.clone().unwrap().len());
												} else {
													let mut bb = WriteBuffer::new();
													bb.write_nil();
													let bin = bb.get_byte();
													wb.write_bin(bin, 0..bin.len());
												}

												let mut wb2 = WriteBuffer::new();
												let bin = wb.get_byte();
												wb2.write_bin(bin, 0..bin.len());

												client_copy_copy_copy.request("pi_pt/util/hotback.syncDB".to_string(), wb2.get_byte().clone(), 10, request_cb.clone());
											}
										}
									}
								}
							}
						}
						break;
					}
					Err(e) => {
						error!("file mem tab commit fail: {:?}", e);
					}
				}
			}

			Err(e) => {
				error!("save data thread unexpected error: {:?}", e);
			}
		}
	}
}

impl OpenTab for FileMemDB {
	// 打开指定的表，表必须有meta
	fn open<'a, T: Tab>(&self, tab: &Atom, _cb: Box<Fn(SResult<T>) + 'a>) -> Option<SResult<T>> {
		let mut table = T::new(tab);
		table.set_param(GenType::USize(Box::into_raw(Box::new(self.1.clone())) as usize));
		Some(Ok(table))
	}
}
impl Ware for FileMemDB {
	// 拷贝全部的表
	fn tabs_clone(&self) -> Arc<Ware> {
		Arc::new(FileMemDB(Arc::new(RwLock::new(self.0.read().unwrap().clone_map())), self.1.clone()))
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
		Arc::new(DBSnapshot(self.clone(), RefCell::new(self.0.read().unwrap().snapshot()), self.1.clone()))
	}
}

// 内存库快照
pub struct DBSnapshot(FileMemDB, RefCell<TabLog<FileMemTab>>, Sender<LmdbMsg>);

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
		if event.ware == Atom::from("file") {
			self.2.send(LmdbMsg::SaveData(event));
		}
	}
}

// 内存事务
pub struct FileMemTxn {
	id: Guid,
	writable: bool,
	tab: FileMemTab,
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
	pub fn new(tab: FileMemTab, id: &Guid, writable: bool) -> RefMemeryTxn {
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
	//提交
	pub fn commit1(&mut self) -> SResult<FnvHashMap<Bin, RwLog>> {
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
										()
									},
									RwLog::Write(Some(v)) => {
										tab.root.upsert(k.clone(), v.clone(), false);
										()
									},
									_ => (),
								}
								()
							},
						}
					}
				} else {
					tab.root = self.root.clone();
				}
				rwlog
			},
			None => return Err(String::from("error prepare null"))
		};

		Ok(log)
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
	fn commit(&self, _cb: TxCallback) -> CommitResult {
		let mut txn = self.0.borrow_mut();
		txn.state = TxState::Committing;
		match txn.commit1() {
			Ok(log) => {
				txn.state = TxState::Commited;
				return Some(Ok(log))
			},
			Err(e) => return Some(Err(e.to_string())),
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

enum LmdbMsg {
	CreateTab(Atom, Sender<OrdMap::<Tree<Bon, Bin>>>),
    SaveData(Event),
}

unsafe impl Send for LmdbMsg {}
