/**
 * 以日志存储为基础，KG索引存储。K要求定长, G为Guid，也是值的指针，可以到日志存储中查找value。
 * KG, 可以用二级的 cow BTree 存储。
 * 	1、采用外部文件分裂，文件名就是1234，不超过65535，这样单个索引文件的大小在几兆，可以存放十万级的KG。要求每个索引文件管理的键范围不重叠。
 * 	2、在单个文件内部用2级结构，一个根节点{Count(4Byte), [{Key, 叶节点的位置Pos(2B)}...]}，几百个叶节点[{Key, Guid}...]。采用COW，每次修改都重新用一个新的根节点。由于块定长（4,8,16,32,64k），而且2级结构，可以不需要空块记录。内存中采用位索引方式记录。
 * 	3、用一个根日志文件不断追加写，可以记录所有索引文件的根块位置。按2字节作为基础单元，FEFE作为结尾符。
 * 	4、用日志整理作为写BTree的驱动，批量处理，写一次根文件。因为不同子表有自己独立缓存，如果掉电，需要从日志中恢复是那个子表缓存的，所以每条kv会在日志记录中记录子表(2字节)
 * 	5、支持2pc事务。
 * 	6、内存中的索引文件也是用的cow的sbtree。如果当前索引文件不够，需要分裂。分裂时，sbtree 也全局保证COW特性！
 * 	7、支持用指定的key创建新的子表，内存中会克隆sbtree，根文件中追加{子表(2字节), 长度(2字节), [所有索引文件名的根块位置(2字节)...]}。
 * 	8、怎么在运行中安全的收缩？需要考虑
 * 
 * 根日志文件的格式为： [{子表key(2字节), 块数组长度(2字节，实际为块数组长度+1), [{文件ID(2字节), 根块位置(2字节)},...]=块数组, }, ...]
 * 如果块数组长度为0，表示子表被删除
 * 如果块数组长度为FFFF，表示初始化成功，根日志文件必须包含一个块数组长度为FFFF的数据。
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

use log::{Bin, SResult, LogResult, Callback, ReadCallback, Config as LogCfg};

// data的目录名
pub const KV_DATA: &str = "data";
// kv索引的目录名
pub const KV_INDEX: &str = "index";
// kv索引的根文件名
pub const KV_ROOT: &str = "root";

/*
 * kv表
 */
#[derive(Clone)]
pub struct Tab {
	dir: Atom,
	cfg: Config,
	log_cfg: LogCfg,
	stat: Statistics,
	root: Arc<Mutex<RLog>>,
	subs: Arc<RwLock<FnvHashMap<u16, SubTab>>>,
	files: Arc<RwLock<FnvHashMap<usize, Record>>>,
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
		let index_dir = path.join(KV_INDEX);
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
							if name_str != KV_ROOT {
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
		let file = path.join(KV_ROOT);
		let tab = Tab{
			dir: dir.clone(),
			cfg: cfg,
			log_cfg: log_cfg,
			stat: Statistics::new(),
			root: Arc::new(Mutex::new(RLog::new(file.to_string_lossy().to_string(), 0))),
			subs: Arc::new(RwLock::new(FnvHashMap::with_capacity_and_hasher(0, Default::default()))),
			files: Arc::new(RwLock::new(FnvHashMap::with_capacity_and_hasher(0, Default::default()))),
		};
		load_root(roots, file, tab, cb);
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


#[derive(Clone)]
pub struct SubTab {
	key: u16,
	stat: Arc<Statistics>,
	root: Arc<Mutex<RLog>>,
	files: Arc<RwLock<FnvHashMap<u16, Record>>>,
	map: OrdMap<Tree<Bin, (u16, u16)>>,
}
impl SubTab {
	fn new(key: u16,
	stat: Arc<Statistics>,
	root: Arc<Mutex<RLog>>,
	map: OrdMap<Tree<Bin, (usize, u16)>>,
	cb: Arc<Fn(SResult<Self>)>) -> Option<SResult<Self>> {
		None
	}
	pub fn read(&self, key: Bin, callback: ReadCallback) -> Option<SResult<Bin>> {
		None
	}
	pub fn write(&self, key: Bin, value: Bin, guid: Guid, callback: Callback) -> SResult<()> {
		Err("guid too old".to_string())
	}

}
//====================================
// 根日志
struct RLog {
	name: String,
	modify: u32,
	file: SResult<AsyncFile>,

}
impl RLog {
	pub fn new(name: String, modify: u32) -> Self {
		RLog {
			name: name,
			modify: modify,
			file: Err("".to_string()),
		}
	}
	pub fn write(&self, sub_tab: u16, value: Bin, guid: Guid, callback: Callback) -> SResult<()> {
		Err("guid too old".to_string())
	}

}

/*
 * 记录文件
 */
struct Record {
	file: SResult<AsyncFile>,
	file_size: usize,
	emptys: Vec<u64>, //空块索引数组
	roots: FnvHashMap<u16, Bin>, // Pos为键， 值为根块
	buffer: FnvHashMap<u16, (Bin, u64)>, // Pos为键， 值为值块及超时时间
	waits: FnvHashMap<usize, Vec<Box<Fn(SResult<AsyncFile>)>>>, // 单个块上的读等待队列
}

/*
 * 记录文件内的根块，默认4K
 */
struct RBlock {
	pos: u16,
	data: Bin,
}
/*
 * 记录文件内的值块，默认4K
 */
struct VBlock {
	pos: u16,
	data: Bin,
}

//================================ 内部静态方法
// 加载空的根日志文件
fn load_empty<P: AsRef<Path> + Send + 'static>(file: P, tab: Tab, cb: Arc<Fn(SResult<Tab>)>) {
	AsyncFile::open(file, AsynFileOptions::ReadWrite(8), Box::new(move |f: IoResult<AsyncFile>| match f {
		Ok(afile) => {
			{
				let mut root = tab.root.lock().unwrap();
				root.file = Ok(afile);
			}
			cb(Ok(tab))
		},
		Err(s) => cb(Err(s.to_string()))
	}));

}

// 顺序加载根日志文件
fn load_root<P: AsRef<Path> + Send + 'static>(mut vec: Vec<(u32, P)>, file: P, tab: Tab, cb: Arc<Fn(SResult<Tab>)>) {
	match vec.pop() {
		Some((m, path)) => {
			AsyncFile::open(path, AsynFileOptions::ReadAppend(8), Box::new(move |f: IoResult<AsyncFile>| match f {
				Ok(afile) =>{
					let len = afile.get_size();
					afile.read(0, len as usize, Box::new(move |f: AsyncFile, r: IoResult<Vec<u8>>| match r {
						Ok(vec_u8) => {
							let b = {
								let mut root = tab.root.lock().unwrap();
								//read_root(vec_u8),
								root.file = Ok(f);
								root.modify = m;
								true
							};
							if b {
								load_root(vec, file, tab, cb);
							}else{
								cb(Ok(tab))
							}
						},
						Err(s) => cb(Err(s.to_string()))
					}));
				},
				Err(s) => cb(Err(s.to_string()))
			}));
		},
		_ => load_empty(file, tab, cb),
	}

}
