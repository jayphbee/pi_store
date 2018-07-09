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
use kg::Tab;


#[derive(Clone)]
pub struct SubTab {
	tab: Tab,
	id: u32,
	root: OrdMap<Tree<Bin, (u16, u16)>>,
	wait: OrdMap<Tree<Bin, Guid>>, // 等待落地的键及Guid
}
impl SubTab {
	pub fn new(tab: Tab, id: u32, root: OrdMap<Tree<Bin, (u16, u16)>>) -> Self {
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
		let (file, pos) = self.root.get(key).unwrap();
		self.tab.records.read(*file, *pos, key, cb);
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

#[derive(Clone)]
pub struct SubTabList(pub Arc<RwLock<Vec<SubTab>>>);

impl SubTabList {
	fn new() -> Self {
		SubTabList(Arc::new(RwLock::new(Vec::new())))
	}
	// 创建一个新的子表
	pub fn make(&self, key: Bin, callback: ReadCallback) -> Option<SResult<Bin>> {
		// 要快速找到空闲的子表ID
		None
	}

}
//====================================
type MapMap = FnvHashMap<u16, FnvHashMap<u16, u16>>;


// 根日志
struct RLog {
	name: String,
	modify: u32,
	file: SResult<AsyncFile>,
	//wait: Option<FnBox>, //切换根日志文件时的等待函数
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


//================================ 内部静态方法
