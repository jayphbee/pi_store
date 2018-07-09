/**
 * 以日志存储为基础，KG索引存储。K要求定长, G为Guid，也是值的指针，可以到日志存储中查找value。
 * KG, 可以用二级的 cow BTree 存储。
 * 	1、采用外部文件分裂，文件名就是1234，不超过65535，这样单个索引文件的大小在几兆，可以存放十万级的KG。要求每个索引文件管理的键范围不重叠。
 * 	2、在单个文件内部用2级结构，一个根节点{Count(4Byte), [{Key, 叶节点的位置Pos(2B)}...]}，几百个叶节点[{Key, Guid}...]。采用COW，每次修改都重新用一个新的根节点。由于块定长（4,8,16,32,64k），而且2级结构，可以不需要空块记录。内存中采用位索引方式记录。
 * 	3、用一个根日志文件不断追加写，可以记录所有索引文件的根块位置。按2字节作为基础单元，FEFE作为结尾符。
 * 	4、用日志整理作为写BTree的驱动，批量处理，写一次根文件。因为不同子表有自己独立缓存，如果掉电，需要从日志中恢复是那个子表缓存的，所以每条kv会在日志记录中记录子表(2字节)
 * 	5、支持2pc事务。
 * 	6、内存中的索引文件也是用的cow的sbtree。如果当前索引文件不够，需要分裂。分裂时，sbtree 也全局保证COW特性！
 * 	7、支持用指定的key创建新的子表，内存中会克隆sbtree，根文件中追加{长度(2字节), 子表(4字节), [所有索引文件名的根块位置(2字节)...]}。
 * 	8、怎么在运行中安全的收缩？需要考虑
 * 
 * 根日志文件的格式为： [块数组长度(2字节，实际为块数组长度+1), {子表key(4字节), [{文件ID(2字节), 根块位置(2字节)},...]=块数组, }, ...]
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

use log::{Bin, Log, SResult, LogResult, Callback, ReadCallback, Config as LogCfg};
use kg_log::KGLog;
use kg_root::RootLog;

/*
 * 记录文件
 */
pub struct Record {
	pub id: u16,
	pub file: SResult<AsyncFile>,
	file_size: usize,
	emptys: Vec<u64>, //空块索引数组
	pub roots: FnvHashMap<u16, (Bin, usize)>, // Pos为键， 值为根块及引用次数
	cache: FnvHashMap<u16, (Bin, u64)>, // Pos为键， 值为值块及超时时间
	waits: FnvHashMap<usize, Vec<Box<Fn(SResult<AsyncFile>)>>>, // 单个块上的读等待队列
}
impl Record {
	// 初始化指定的记录文件，打开异步文件，将所有的根块加载起来
	pub fn new(dir: &Atom, id: u16, roots: FnvHashMap<u16, usize>, cb: Arc<Fn(SResult<Self>)>) {
		
	}
	pub fn get_root(&self, pos: u16) -> Bin {
		self.roots.get(&pos).unwrap().0.clone()
	}
}

// 记录文件表
#[derive(Clone)]
pub struct RecordMap{
	pub dir: Atom,
	count: Arc<AtomicUsize>,
	pub map: Arc<RwLock<FnvHashMap<u16, Record>>>,
}
impl RecordMap {
	fn new(dir: Atom) ->Self {
		RecordMap{
			dir: dir,
			count: Arc::new(AtomicUsize::new(0)),
			map: Arc::new(RwLock::new(FnvHashMap::with_capacity_and_hasher(0, Default::default()))),
		}
	}
	pub fn get_root(&self, file:u16, pos: u16) -> Bin {
		self.map.read().unwrap().get(&file).unwrap().get_root(pos)
	}
	// 读取指定文件的指定的根块下的指定键的Guid
	pub fn read(&self, file:u16, root_pos: u16, key: &Bin, cb: ReadCallback) -> Option<SResult<Guid>> {

		None
	}

}
//====================================

//================================ 内部静态方法
