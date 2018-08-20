/**
 * 	1、将叶节点单独存放在一个文件中，按4k一个叶节点。数据格式：[Count(4Byte), {Key, Guid}...]。采用COW，每次修改都重新用一个新的叶节点。空块由子节点计算获得，内存中采用位索引方式记录。
 * 	2、将子节点单独存放在一个文件中，按4k一个子节点。数据格式：[Size(4Byte), Count(4Byte), {Key, 叶节点的位置Pos(4Byte)}...]。采用COW，每次修改都重新用一个新的子节点。空块由超级块计算获得，内存中采用位索引方式记录。所有子节点全部加载进内存。
 * 分裂和合并类似Cow的B+树的方式
 */

use std::boxed::FnBox;
use std::sync::{Arc, Weak, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::result::Result;
use std::collections::hash_map::Entry;
use std::cmp::{Ord, PartialOrd, Ordering as Order};
use std::vec::Vec;
use std::fs::{File, DirBuilder, rename, remove_file};
use std::path::{Path, PathBuf};
use std::io::Result as IoResult;
use std::mem;

use fnv::FnvHashMap;
use fnv::FnvHashSet;

use pi_lib::ordmap::{OrdMap, ActionResult};
use pi_lib::asbtree::{Tree, new};
use pi_lib::time::now_millis;
use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::data_view::{GetView, SetView};
use pi_lib::bon::{ReadBuffer, WriteBuffer};
use pi_lib::base58::{ToBase58, FromBase58};

use pi_base::file::{Shared, SharedFile, AsyncFile, AsynFileOptions};

use log::{Bin, Log, SResult, LogResult, Callback, ReadCallback, Config as LogCfg};
use kg_log::KGLog;
use kg_root::RootLog;
use kg::{BinPos, Tab, ReadGuidCallback};


/*
 * 记录文件
 */
pub struct Record {
	node: NodeFile,
	pub roots: FnvHashMap<u32, Weak<Vec<u8>>>, // Pos为键， 值为子节点的弱引用
	leaf: NodeFile,
	cache: FnvHashMap<u32, (Bin, u64)>, // Pos为键， 值为叶节点及超时时间
	waits: FnvHashMap<usize, Vec<Box<Fn(SResult<AsyncFile>)>>>, // 单个块上的读等待队列
}
impl Record {
	// 初始化记录文件，根据加载起来
	pub fn new(node_file: PathBuf, leaf_file: PathBuf, cb: Box<FnBox(SResult<Self>)>) {
		AsyncFile::open(node_file, AsynFileOptions::ReadAppend(8), Box::new(move |f: IoResult<AsyncFile>| match f {
			Ok(file1) =>{
				AsyncFile::open(leaf_file, AsynFileOptions::ReadAppend(8), Box::new(move |f: IoResult<AsyncFile>| match f {
					Ok(file2) => cb(Ok(Record {
						node: NodeFile::new(Arc::new(file1)),
						roots: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
						leaf: NodeFile::new(Arc::new(file2)),
						cache: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
						waits: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
					})),
					Err(s) => cb(Err(s.to_string()))
				}));
			},
			Err(s) => cb(Err(s.to_string()))
		}));
	}
	// 初始化方法，根据所有子表的子节点集合加载子节点，返回子节点的位置表
	pub fn init(record: Arc<RwLock<Record>>, nodes: Arc<FnvHashMap<u32, FnvHashSet<u32>>>, cb: Box<FnBox(SResult<FnvHashMap<u32, Bin>>)>) {
		let rfile = &record.write().unwrap().node;
		let len = rfile.file_size as usize;
		rfile.file.clone().pread(0, len, Box::new(move |f: SharedFile, r: IoResult<Vec<u8>>| match r {
			Ok(vec_u8) => {
				let mut map = FnvHashMap::with_capacity_and_hasher(0, Default::default());
				for set in nodes.values() {
					for pos in set.iter() {
						match map.entry(*pos) {
							Entry::Occupied(_) => continue,
							er => {
								er.or_insert(Arc::new(Vec::new()));
							}
						}
					}
				}
				cb(Ok(map))
			},
			Err(s) => cb(Err(s.to_string()))
		}));
	}
	// 获取子节点的空块索引数组
	pub fn node_empty(&self) -> Arc<RwLock<Vec<u64>>>{
		self.node.emptys.clone()
	}
	// 分配一个子节点
	pub fn malloc_node(&mut self) -> BinPos {
		BinPos::new(self.node.emptys.clone(), Arc::new(Vec::new()), 0)
	}
	// 读取指定位置的子节点
	pub fn read_node(&mut self, pos: u32) -> Option<BinPos> {
		None
	}
	// 写入多个子节点
	pub fn write_nodes(&self, items: Vec<BinPos>, cb: Callback) {
		
	}
	// 分配一个叶节点
	pub fn malloc_leaf(&self) -> BinPos {
		BinPos::new(self.node.emptys.clone(), Arc::new(Vec::new()), 0)
	}
	// 读取指定位置的叶节点
	pub fn read_leaf(&mut self, pos: u32, cb: ReadGuidCallback) -> Option<SResult<BinPos>> {
		None
	}
	// 写入多个叶节点
	pub fn write_leafs(&self, items: Vec<BinPos>, cb: Callback) {
		
	}

}

//====================================
/*
 * 子节点文件
 */
struct NodeFile {
	file: SharedFile,
	file_size: u64,
	emptys: Arc<RwLock<Vec<u64>>>, //空块索引数组
}
impl NodeFile {
	fn new(file: SharedFile) -> Self {
		let size = file.get_size();
		NodeFile {
			file: file,
			file_size: size,
			emptys: Arc::new(RwLock::new(Vec::new()))
		}
	}
}
//================================ 内部静态方法
fn free<T>(_:T) {}