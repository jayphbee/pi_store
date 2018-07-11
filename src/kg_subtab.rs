/**
 * 子表
 * 支持创建新的子表，一般是在当前最大ID的子表上递增使用ID，如果溢出，则回环。内存中会克隆sbtree，根文件中追加{长度(2字节), 子表(4字节), [子节点位置(4字节)...]}。
 * 用二级的 cow BTree 存储。
 */


use std::boxed::FnBox;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::result::Result;
use std::cmp::{Ord, PartialOrd, Ordering as Order};
use std::vec::Vec;
use std::fs::{File, DirBuilder, rename, remove_file};
use std::path::{Path, PathBuf};
use std::io::Result as IoResult;
use std::mem;

use fnv::FnvHashMap;

use pi_lib::ordmap::{OrdMap, ActionResult, Entry};
use pi_lib::asbtree::{Tree, new};
use pi_lib::time::now_millis;
use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::data_view::{GetView, SetView};
use pi_lib::bon::{ReadBuffer, WriteBuffer};
use pi_lib::base58::{ToBase58, FromBase58};

use pi_base::file::{AsyncFile, AsynFileOptions};

use log::{Bin, Log, SResult, LogResult, Callback, ReadCallback, Config as LogCfg};
use kg_log::KGLog;
use kg_root::RootLog;
use kg::{BinPos, Tab, ReadGuidCallback};

// 子表
#[derive(Clone)]
pub struct SubTab {
	tab: Tab,
	id: u32,
	root: OrdMap<Tree<Bin, BinPos>>, // 键是子节点的最小值，值为子节点及位置
	wait: OrdMap<Tree<Bin, Guid>>, // 等待落地的键及Guid
}
impl SubTab {
	// 创建指定ID的子表，root参数为该子表的键在每个记录文件中对应的根块位置
	pub fn new(tab: Tab, id: u32, root: OrdMap<Tree<Bin, BinPos>>) -> Self {
		SubTab {
			tab: tab,
			id: id,
			root: root,
			wait: OrdMap::new(new()),
		}
	}
	// 读取键对应的Guid
	pub fn guid(&self, key: Bin, cb: ReadCallback) -> Option<SResult<Guid>> {
		None
	}
	// 读取键值
	pub fn read(&self, key: &Bin, cb: ReadCallback) -> Option<SResult<(Bin, Guid)>> {
		// TODO 改成entrys查询
		let node = self.root.get(key).unwrap();
		//self.tab.records.read(*file, *pos, key, cb);
		// TODO kv_log: Log, 
		None
	}
	// 快速落地
	pub fn write(&self, key: Bin, value: Bin, guid: Guid, cb: Callback) -> SResult<()> {
		Err("guid too old".to_string())
	}
	// 快速落地多条
	pub fn writes(&self, key: Bin, value: Bin, guid: Guid, cb: Callback) -> SResult<()> {
		Err("guid too old".to_string())
	}

}

// 为了快速的寻找和插入删除新的子表，使用OrdMap
#[derive(Clone)]
pub struct SubTabMap(pub Arc<RwLock<OrdMap<Tree<u32, SubTab>>>>);

impl SubTabMap {
	fn new() -> Self {
		SubTabMap(Arc::new(RwLock::new(OrdMap::new(new()))))
	}
	// 创建一个新的子表
	pub fn make(&self, key: Bin, callback: ReadCallback) -> Option<SResult<Bin>> {
		// 要快速找到空闲的子表ID
		None
	}

}
//====================================

//================================ 内部静态方法
