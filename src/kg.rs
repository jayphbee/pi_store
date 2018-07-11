/**
 * 以日志存储为基础，KG索引存储。K要求定长, G为Guid，也是值的指针，可以到日志存储中查找value。
 * 	用日志整理作为写索引的驱动，批量处理，会所有的涉及到的子表更新子节点，并删除相关的等待表，写一次根文件。因为不同子表有自己独立缓存，如果掉电，需要从日志中恢复是那个子表缓存的，所以每条kv会在日志记录中记录子表(4字节)
 * 
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
use kg_record::{Record};
use kg_root::RootLog;
use kg_subtab::{SubTab, SubTabMap};

pub type ReadGuidCallback = Arc<Fn(SResult<Guid>)>;

// data的目录名
pub const KV_DATA: &str = ".data";
// kv索引的子节点文件名
pub const KV_NODE: &str = "node";
// kv索引的叶节点文件名
pub const KV_LEAF: &str = "leaf";
// kv索引的根日志文件名
pub const KV_RLOG: &str = "rlog";

// 共享的节点位置，如果节点无人引用，则写入到空块索引数组中
#[derive(Clone)]
pub struct BinPos(Arc<RwLock<Vec<u64>>>, Bin, u32);

impl BinPos {
	pub fn new(empty: Arc<RwLock<Vec<u64>>>, bin: Bin, pos: u32) -> Self {
		BinPos(empty, bin, pos)
	}
}
impl Drop for BinPos {
	fn drop(&mut self) {
		if Arc::strong_count(&self.1) == 1 {
			println!("Dropping!");
			// TODO 将空的位置写入到空块索引数组中
		}
	}
}

/*
 * kv表
 */
#[derive(Clone)]
pub struct Tab {
	pub dir: Atom,
	pub cfg: Config,
	pub log_cfg: LogCfg,
	pub stat: Statistics,
	pub root: Arc<Mutex<RootLog>>,
	pub subs: SubTabMap,
	pub record: Arc<RwLock<Record>>,
	pub kg_log: KGLog,
}

impl Tab {
	pub fn new(dir: Atom, cfg: Config, log_cfg: LogCfg, cb: Arc<Fn(SResult<Self>)>) -> Option<SResult<Self>> {
		let path = Path::new(&**dir);
		if !path.exists() {
			DirBuilder::new().recursive(true).create(path).unwrap();
		}else if !path.is_dir() {
			return Some(Err("invalid kg dir".to_string()))
		}
		let data_dir = path.join(KV_DATA);
		if !data_dir.exists() {
			DirBuilder::new().recursive(true).create(data_dir).unwrap();
		}else if !data_dir.is_dir() {
			return Some(Err("invalid kg data dir".to_string()))
		}
		let index_dir = path.join(KV_NODE);
		let idir = Atom::from(index_dir.to_str().unwrap());
		if !index_dir.exists() {
			DirBuilder::new().recursive(true).create(index_dir).unwrap();
		}else if !index_dir.is_dir() {
			return Some(Err("invalid kg index dir".to_string()))
		}
		let mut roots: Vec<(u32, PathBuf)> = Vec::new();
		// 分析目录下所有的根日志文件
		for entry in path.read_dir().expect("read_dir call failed") {
			if let Ok(entry) = entry {
				let file = entry.path();
				if file.is_file() {
					let name = file.to_str().unwrap();
					match name.rfind('.') {
						Some(dot) => {
							let (name_str, modify_str) = name.split_at(dot);
							if name_str != KV_RLOG {
								continue;
							}
							match u32::from_str_radix(modify_str, 16) {
								Ok(r) =>{
									roots.push((r, file));
								}
								_ => {
									remove_file(file).expect("remove failed");
									continue;
								}
							};
						},
						_ => {
							remove_file(file).expect("remove failed");
						}
					}
				}
			}
		}
		roots.as_mut_slice().sort();
		let file = path.join(KV_RLOG);
		// let tab = Tab{
		// 	dir: dir.clone(),
		// 	cfg: cfg,
		// 	log_cfg: log_cfg,
		// 	stat: Statistics::new(),
		// 	root: Arc::new(Mutex::new(RLog::new(file.to_string_lossy().to_string(), 0))),
		// 	subs: SubTabMap::new(),
		// 	records: RecordMap::new(idir),
		// };
		// load_root(roots, file, tab, cb);
		None
	}
	pub fn dir(&self) -> &Atom {
		&self.dir
	}
	pub fn cfg(&self) -> &Config {
		&self.cfg
	}
	pub fn log_cfg(&self) -> &LogCfg {
		&self.log_cfg
	}
	pub fn stat(&self) -> &Statistics {
		&self.stat
	}
	// 列出所有的子表编号
	pub fn list_subs(&self) -> Vec<u16> {
		vec![]
	}
	// TODO 多子表按时间共同落地
	pub fn collect(&self, subs: Vec<(u16, Vec<(Bin, Guid)>)>, log_file: u64, cb: Callback) {
		// 每个子表循环，写入到记录文件中
		// 每子表的根块写入根日志文件
		// 通知日志生成索引
	}

}
// 统计
#[derive(Clone)]
pub struct Config {
	pub key_size: u8, // 键的最大大小
	pub cache_size: usize, // 缓冲大小，字节 默认1M
	pub cache_timeout: usize, // 最长缓冲时间，毫秒，基于最后读。默认30分钟
}
impl Config {
	pub fn new(key_size: u8) -> Self {
		Config {
			key_size: key_size,
			cache_size: 1*1024*1024,
			cache_timeout: 30*60*1000,
		}
	}
}
// 统计
#[derive(Clone)]
pub struct Statistics {
	pub read_count: Arc<AtomicUsize>,
	pub write_count: Arc<AtomicUsize>,
	pub read_time: Arc<AtomicUsize>,
	pub write_time: Arc<AtomicUsize>,
	pub cache_count: Arc<AtomicUsize>,
}
impl Statistics {
	fn new() -> Self {
		Statistics {
			read_count: Arc::new(AtomicUsize::new(0)),
			write_count: Arc::new(AtomicUsize::new(0)),
			read_time: Arc::new(AtomicUsize::new(0)),
			write_time: Arc::new(AtomicUsize::new(0)),
			cache_count: Arc::new(AtomicUsize::new(0)),
		}
	}
}


//====================================


//================================ 内部静态方法
