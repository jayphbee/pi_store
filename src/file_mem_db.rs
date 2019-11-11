
use std::sync::{Arc, Mutex, RwLock};
use std::cell::RefCell;
use std::mem;
use std::path::Path;
use std::fs;
use std::thread;
use std::time::Instant;
use fnv::FnvHashMap;
use std::collections::HashMap;

use ordmap::ordmap::{OrdMap, Entry, Iter as OIter, Keys};
use ordmap::asbtree::{Tree};
use atom::{Atom};
use guid::Guid;
use bon::{Decode, Encode, ReadBuffer, WriteBuffer};
use apm::counter::{GLOBAL_PREF_COLLECT, PrefCounter};

use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError};

use pi_db::db::{EventType, Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult, NextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn, Tab, OpenTab, Event, Ware, WareSnapshot, Filter, TxState, Iter, CommitResult, RwLog, Bon, TabMeta};
use pi_db::tabs::{TabLog, Tabs, Prepare};

use lmdb::{ Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};


lazy_static! {
    static ref LMDB_CHAN: (Sender<LmdbMsg>, Receiver<LmdbMsg>) = unbounded();
	static ref LMDB_ENV: Arc<RwLock<Option<Arc<Environment>>>> = Arc::new(RwLock::new(None));
}

const MAX_DBS_PER_ENV: u32 = 1024;

#[derive(Clone)]
pub struct FileMemTab(Arc<Mutex<MemeryTab>>);
impl Tab for FileMemTab {
	fn new(tab: &Atom) -> Self {
		let (s, r) = unbounded();
		LMDB_CHAN.0.clone().send(LmdbMsg::CreateTab(tab.clone(), s));
		match r.recv() {
			Ok(root) => {
				let file_mem_tab = MemeryTab {
					prepare: Prepare::new(FnvHashMap::with_capacity_and_hasher(0, Default::default())),
					root: root,
					tab: tab.clone(),
				};

				return FileMemTab(Arc::new(Mutex::new(file_mem_tab)));
			}
			Err(_) => {
				panic!("create new tab failed");
			}
		}
	}
	fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn> {
		let txn = Arc::new(FileMemTxn::new(self.clone(), id, writable));
		return txn
	}
}

fn load_data_to_mem_tab(file_tab: &Atom, root: &mut OrdMap<Tree<Bon, Bin>>) {
	let env = LMDB_ENV.read().unwrap().clone().unwrap();
	if let Ok(db) = env.open_db(Some(file_tab.as_str())) {
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
				error!("lmdb txn cursor failed");
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
pub struct FileMemDB(Arc<RwLock<Tabs<FileMemTab>>>);

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
		let env = Arc::new(
			Environment::new()
				.set_max_dbs(MAX_DBS_PER_ENV)
				.set_map_size(db_size)
				.set_flags(EnvironmentFlags::NO_TLS)
				.open(Path::new(&db_path.to_string()))
				.expect("create lmdb env failed")
		);

		let mut e = LMDB_ENV.write().unwrap();
		*e = Some(env.clone());

        let _  = thread::Builder::new().name("lmdb_writer_thread".to_string()).spawn(move || {
			let mut write_cache: HashMap<(Atom, Atom, Bin), Option<Bin>> = HashMap::new();
			let mut dbs = HashMap::new();

			let receiver = LMDB_CHAN.1.clone();
            loop {
                match receiver.recv() {
					Ok(LmdbMsg::CreateTab(tab_name, sender)) => {
						dbs.entry(tab_name.get_hash() as u64)
							.or_insert(
								env.create_db(Some(tab_name.clone().as_str()), DatabaseFlags::empty()).unwrap()
						);
						let mut root = OrdMap::<Tree<Bon, Bin>>::new(None);

						load_data_to_mem_tab(&tab_name, &mut root);
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

		FileMemDB(Arc::new(RwLock::new(Tabs::new())))
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
				load_data_to_mem_tab(&tab_name, &mut root);
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
				for ((ware, tab, key), val) in write_cache.drain() {
					if let Some(db) = dbs.get(&(tab.get_hash() as u64)) {
						if val.is_none() {
							match rw_txn.del(*db, key.as_ref(), None) {
								Ok(_) => {}
								Err(e) => {
									error!("file mem tab del fail {:?}, ware: {:?}, tab:{:?}", e, ware, tab);
								}
							}
						} else {
							match rw_txn.put(*db, key.as_ref(), val.clone().unwrap().as_ref(), WriteFlags::empty()) {
								Ok(_) => {
									write_bytes += val.as_ref().unwrap().len();
								}
								Err(e) => {
									error!("file mem tab put fail {:?}, ware:{:?}, tab:{:?}", e, ware, tab);
								}
							}
						}
					}
				}

				match rw_txn.commit() {
					Ok(_) => {
						debug!("====> lmdb commit write {:?}bytes time: {:?} <====", write_bytes, start_time.elapsed());
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
		Some(Ok(T::new(tab)))
	}
}
impl Ware for FileMemDB {
	// 拷贝全部的表
	fn tabs_clone(&self) -> Arc<Ware> {
		Arc::new(FileMemDB(Arc::new(RwLock::new(self.0.read().unwrap().clone_map()))))
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
		Arc::new(DBSnapshot(self.clone(), RefCell::new(self.0.read().unwrap().snapshot()), LMDB_CHAN.0.clone()))
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
