
use std::sync::{Arc, Mutex, RwLock};
use std::cell::RefCell;
use std::mem;

use fnv::FnvHashMap;

use pi_lib::ordmap::{OrdMap, Entry};
use pi_lib::asbtree::{Tree};
use pi_lib::atom::{Atom};
use pi_lib::guid::Guid;
use pi_lib::sinfo::StructInfo;

use pi_db::db::{Bin, TabKV, SResult, DBResult, IterResult, KeyIterResult, NextResult, KeyNextResult, TxCallback, TxQueryCallback, Txn, TabTxn, MetaTxn, Tab, OpenTab, Ware, WareSnapshot, Filter, TxState, Iter, KeyIter};
use pi_db::tabs::{TabLog, Tabs};


#[derive(Clone)]
pub struct XTab(Arc<Mutex<Table>>);
impl Tab for XTab {
	fn new(tab: &Atom) -> Self {
		let tab = Table {
			prepare: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			root: OrdMap::new(None),
			tab: tab.clone(),
		};
		XTab(Arc::new(Mutex::new(tab)))
	}
	fn transaction(&self, id: &Guid, writable: bool) -> Arc<TabTxn> {
		let txn = Tx::new(self.clone(), id, writable);
		return Arc::new(txn)
	}
}

//库
#[derive(Clone)]
pub struct DB(Arc<RwLock<Tabs<XTab>>>);

impl DB {
	pub fn new() -> Self {
		DB(Arc::new(RwLock::new(Tabs::new())))
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
	// fn tabs_clone(&self) -> Self {
	// 	self.clone()
	// }
	// 列出全部的表
	fn list(&self) -> Vec<Atom> {
		self.0.read().unwrap().list()
	}
	// 获取该库对预提交后的处理超时时间, 事务会用最大超时时间来预提交
	fn timeout(&self) -> usize {
		TIMEOUT
	}
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>> {
		self.0.read().unwrap().get(tab_name)
	}
	// 获取当前表结构快照
	fn snapshot(&self) -> Arc<WareSnapshot> {
		Arc::new(DBSnapshot(self.clone(), RefCell::new(self.0.read().unwrap().snapshot())))
	}
}

//库快照
pub struct DBSnapshot(DB, RefCell<TabLog<XTab>>);

impl WareSnapshot for DBSnapshot {
	// 列出全部的表
	fn list(&self) -> Vec<Atom> {
		self.1.borrow().list()
	}
	// 表的元信息
	fn tab_info(&self, tab_name: &Atom) -> Option<Arc<StructInfo>> {
		self.1.borrow().get(tab_name)
	}
	// 检查该表是否可以创建
	fn check(&self, tab: &Atom, meta: &Option<Arc<StructInfo>>) -> SResult<()> {
		Ok(())
	}
	// 新增 修改 删除 表
	fn alter(&self, tab_name: &Atom, meta: Option<Arc<StructInfo>>) {
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

}



//事务
pub struct Tx {
	id: Guid,
	writable: bool,
	tab: XTab,
	root: BinMap,
	old: BinMap,
	rwlog: FnvHashMap<Bin, RwLog>,
	state: TxState,
}

impl Tx {
	//开始事务
	pub fn new(tab: XTab, id: &Guid, writable: bool) -> Tr {
		let root = tab.0.lock().unwrap().root.clone();
		let txn = Tx {
			id: id.clone(),
			writable: writable,
			tab: tab,
			root: root.clone(),
			old: root,
			rwlog: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
			state: TxState::Ok,
		};
		return RefCell::new(txn)
	}
	//获取数据
	pub fn get(&mut self, key: Bin) -> Option<Bin> {
		match self.root.get(&key) {
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
		self.root.upsert(key.clone(), value.clone(), false);
		self.rwlog.insert(key.clone(), RwLog::Write(Some(value.clone())));
		Ok(())
	}
	//删除
	pub fn delete(&mut self, key: Bin) -> SResult<()> {
		self.root.delete(&key, false);
		self.rwlog.insert(key.clone(), RwLog::Write(None));
		Ok(())
	}

	//预提交
	pub fn prepare1(&mut self) -> SResult<()> {
		let mut tab = self.tab.0.lock().unwrap();
		//遍历事务中的读写日志
		for (key, rw_v) in self.rwlog.iter() {
			//检查预提交是否冲突
			for o_rwlog in tab.prepare.values() {
				match o_rwlog.get(key) {
					Some(RwLog::Read) => match rw_v {
						RwLog::Read => (),
						_ => return Err(String::from("parpare conflicted rw"))
					},
					None => (),
					Some(_e) => {
						return Err(String::from("parpare conflicted rw2"))
					},
				}
			}
			//检查Tab根节点是否改变
			if tab.root.ptr_eq(&self.old) == false {
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
	pub fn commit1(&mut self) -> SResult<()> {
		let mut tab = self.tab.0.lock().unwrap();
		match tab.prepare.remove(&self.id) {
			Some(rwlog) => {
				let root_if_eq = tab.root.ptr_eq(&self.old);
				//判断根节点是否相等
				if root_if_eq == false {
					for (k, rw_v) in rwlog.iter() {
						match rw_v {
							RwLog::Read => (),
							_ => {
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
			},
			None => return Err(String::from("error prepare null"))
		}
		Ok(())
	}
	//回滚
	pub fn rollback1(&mut self) -> SResult<()> {
		self.tab.0.lock().unwrap().prepare.remove(&self.id);
		Ok(())
	}
}


pub type Tr = RefCell<Tx>;

impl Txn for Tr {
	// 获得事务的状态
	fn get_state(&self) -> TxState {
		self.borrow().state.clone()
	}
	// 预提交一个事务
	fn prepare(&self, _timeout: usize, _cb: TxCallback) -> DBResult {
		let mut txn = self.borrow_mut();
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
	fn commit(&self, _cb: TxCallback) -> DBResult {
		let mut txn = self.borrow_mut();
		txn.state = TxState::Committing;
		match txn.commit1() {
			Ok(()) => {
				txn.state = TxState::Commited;
				return Some(Ok(()))
			},
			Err(e) => return Some(Err(e.to_string())),
		}
	}
	// 回滚一个事务
	fn rollback(&self, _cb: TxCallback) -> DBResult {
		let mut txn = self.borrow_mut();
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

impl TabTxn for Tr {
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
		let mut txn = self.borrow_mut();
		let mut value_arr = Vec::new();
		for tabkv in arr.iter() {
			let value = match txn.get(tabkv.key.clone()) {
				Some(v) => Some(v),
				_ => return Some(Err(String::from("null")))
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
		let mut txn = self.borrow_mut();
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
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		cb: Arc<Fn(IterResult)>,
	) -> Option<IterResult> {
		Some(Ok(Box::new(MemIter::new(self.borrow_mut().root.clone(), key, descending, filter))))
	}
	// 迭代
	fn key_iter(
		&self,
		key: Option<Bin>,
		descending: bool,
		filter: Filter,
		cb: Arc<Fn(KeyIterResult)>,
	) -> Option<KeyIterResult> {
		Some(Ok(Box::new(MemKeyIter::new(self.borrow_mut().root.clone(), key, descending, filter))))
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
	fn tab_size(&self, _cb: TxCallback) -> DBResult {
		None
	}
}

//================================ 内部结构和方法
const TIMEOUT: usize = 100;

#[derive(Clone, Debug)]
enum RwLog {
	Read,
	Write(Option<Bin>),
}
type BinMap = OrdMap<Tree<Bin, Bin>>;

//表
struct Table {
	pub prepare: FnvHashMap<Guid, FnvHashMap<Bin, RwLog>>,
	pub root: BinMap,
	pub tab: Atom,
}

//迭代器
struct MemIter {
	root: BinMap,
	key: Option<Bin>,
	descending: bool,
	filter: Filter,
	arr: SResult<Vec<(Bin, Bin)>>,
}
impl MemIter{
	fn new(root: BinMap, key: Option<Bin>, descending: bool, filter: Filter) -> Self {
		let mut vec = Vec::new();
		let mut f = |e: &Entry<Bin, Bin>| {
			vec.push((e.0.clone(), e.1.clone()));
		};
		let k1 = match key {
			Some(ref b) => Some(b),
			_ => None
		};
		root.select(k1, descending, &mut f);
		if !descending {
			vec[..].reverse();
		}
		MemIter {
			root: root,
			key: key,
			descending: descending,
			filter: filter,
			arr: Ok(vec),
		}
	}
}
impl Iter for MemIter{
	fn next(&mut self, _cb: Arc<Fn(NextResult)>) -> Option<NextResult> {
		match &mut self.arr {
			&mut Ok(ref mut v) => Some(Ok(v.pop())),
			Err(s) => Some(Err(s.clone()))
		}
	}
}

//迭代器
struct MemKeyIter {
	root: BinMap,
	key: Option<Bin>,
	descending: bool,
	filter: Filter,
	arr: SResult<Vec<Bin>>,
}
impl MemKeyIter{
	fn new(root: BinMap, key: Option<Bin>, descending: bool, filter: Filter) -> Self {
		let mut vec = Vec::new();
		let mut f = |e: &Entry<Bin, Bin>| {
			vec.push(e.0.clone());
		};
		let k1 = match key {
			Some(ref b) => Some(b),
			_ => None
		};
		root.select(k1, descending, &mut f);
		if !descending {
			vec[..].reverse();
		}
		MemKeyIter {
			root: root,
			key: key,
			descending: descending,
			filter: filter,
			arr: Ok(vec),
		}
	}
}
impl KeyIter for MemKeyIter{
	fn next(&mut self, _cb: Arc<Fn(KeyNextResult)>) -> Option<KeyNextResult> {
		match &mut self.arr {
			&mut Ok(ref mut v) => Some(Ok(v.pop())),
			Err(s) => Some(Err(s.clone()))
		}
	}
}
#[derive(Clone)]
pub struct MemeryMetaTxn();

impl MetaTxn for MemeryMetaTxn {
	// 创建表、修改指定表的元数据
	fn alter(&self, tab: &Atom, meta: Option<Arc<StructInfo>>, cb: TxCallback) -> DBResult{
		Some(Ok(()))
	}
	// 快照拷贝表
	fn snapshot(&self, tab: &Atom, from: &Atom, cb: TxCallback) -> DBResult{
		Some(Ok(()))
	}
	// 修改指定表的名字
	fn rename(&self, tab: &Atom, new_name: &Atom, cb: TxCallback) -> DBResult {
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
	fn commit(&self, _cb: TxCallback) -> DBResult {
		Some(Ok(()))
	}
	// 回滚一个事务
	fn rollback(&self, _cb: TxCallback) -> DBResult {
		Some(Ok(()))
	}
}
//================================ 内部静态方法
