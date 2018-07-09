/**
 * 用一个根日志文件不断追加写，记录所有索引文件的根块位置。
 * 以根日志文件写入为准，表示索引记录落地。
 * 支持用指定的key创建新的子表，内存中会克隆sbtree，根文件中追加{长度(2字节), 子表(4字节), [所有索引文件名的根块位置(2字节)...]}。
 * 根日志文件的格式为： [块数组长度(2字节，实际为块数组长度+1), {子表key(4字节), [{文件ID(2字节), 根块位置(2字节)},...]=块数组, }, ...]
 * 如果块数组长度为0，表示子表被删除
 * 如果块数组长度为FFFF，表示初始化成功，根日志文件必须包含一个块数组长度为FFFF的数据。
 */


use std::boxed::FnBox;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::vec::Vec;
use std::fs::{File, DirBuilder, rename, remove_file};
use std::path::{Path, PathBuf};
use std::io::Result as IoResult;

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

use log::{Bin, SResult, Callback};
use kg::Tab;
use kg_record::{Record, RecordMap};
use kg_subtab::SubTab;


// 每个子表的根节点表
type SBMap = FnvHashMap<u32, FnvHashMap<u16, u16>>;
// 每个记录文件中根节点被子表的引用次数表
type RCMap = FnvHashMap<u16, FnvHashMap<u16, usize>>;


// 根日志
pub struct RootLog {
	pub name: String,
	pub modify: u32,
	pub file: SResult<AsyncFile>,
	//wait: Option<FnBox>, //切换根日志文件时的等待函数
}
impl RootLog {
	pub fn new(name: String, modify: u32) -> Self {
		RootLog {
			name: name,
			modify: modify,
			file: Err("".to_string()),
		}
	}
	// 写入一个指定子表的根块列表，如果没有列表表示删除根块
	pub fn write(&self, subtab: u32, roots: Option<Vec<(u16, u16)>>, callback: Callback) -> SResult<()> {
		Err("guid too old".to_string())
	}

}
//====================================



//================================ 内部静态方法
/*
* 子表编号的大小
*/
const ST_SIZE: usize = 4;

/*
* 块的长度
*/
const BLOCK_LEN: usize = 2;

/*
* 特殊的块长度，表示初始化写入成功
*/
const INIT_FLAG: u16 = 0xffff;


// 加载空的根日志文件
fn load_empty<P: AsRef<Path> + Send + 'static>(file: P, tab: Tab, cb: Arc<Fn(SResult<Tab>)>) {
	AsyncFile::open(file, AsynFileOptions::ReadWrite(8), Box::new(move |f: IoResult<AsyncFile>| match f {
		Ok(afile) => {
			{tab.root.lock().unwrap().file = Ok(afile);}
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
							match read_root(vec_u8) {
								Some((subtabs, records)) =>{
									{
										let mut root = tab.root.lock().unwrap();
										root.file = Ok(f);
										root.modify = m;
									}
									//删除vec中剩余的日志文件
									for (_, f) in vec.into_iter() {
										remove_file(f).expect("remove failed");
									}
									// 打开全部的记录文件，并加载全部的根块，然后初始化子表
									next_open(tab, subtabs, records, cb);
								},
								_ =>{
									// 为None表示该根日志文件错误
									// TODO 删除该文件
									load_root(vec, file, tab, cb)
								}
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

// 读取根日志文件，返回所有子表的文件根块表
fn read_root(data: Vec<u8>) -> Option<(SBMap, RCMap)> {
	let mut init = false;
	let size = data.len();
	let slice = data.as_slice();
	let mut pos = 0;
	let mut subtabs: SBMap = FnvHashMap::with_capacity_and_hasher(0, Default::default());
	while pos + BLOCK_LEN + ST_SIZE < size {
		let len = slice.get_lu16(pos);
		pos+=BLOCK_LEN;
		//如果块数组长度为FFFF，表示初始化成功，根日志文件必须包含一个块数组长度为FFFF的数据
		if len == INIT_FLAG {
			init = true;
			continue;
		}
		let key = slice.get_lu32(pos);
		// 如果块数组长度为0，表示子表被删除
		if len == 0 {
			subtabs.remove(&key);
			continue;
		}
		// 找到或创建子表
		let sub = subtabs.entry(key).or_insert(FnvHashMap::with_capacity_and_hasher(0, Default::default()));
		pos+=2;
		while pos < size {
			// 子表记录{文件ID(2字节), 根块位置(2字节)}
			sub.insert(slice.get_lu16(pos), slice.get_lu16(pos+2));
			pos+=4;
		}
	}
	if init {
		// 计算每记录文件的根块的位置和引用次数
		let mut records: RCMap = FnvHashMap::with_capacity_and_hasher(0, Default::default());
		for map in subtabs.values() {
			for (file, root) in map.iter() {
				let roots = records.entry(*file).or_insert(FnvHashMap::with_capacity_and_hasher(0, Default::default()));
				let rc = roots.entry(*root).or_insert(0);
				*rc += 1;
			}
		}
		Some((subtabs, records))
	}else{
		None
	}
}

// 打开全部的记录文件，并加载全部的根块，然后初始化子表
fn next_open(tab:Tab, subtabs: SBMap, records: RCMap, cb: Arc<Fn(SResult<Tab>)>){
	let len = records.len();
	if len == 0 {
		return cb(Ok(tab))
	}
	let dir = tab.records.dir.clone();
	let count = Arc::new(AtomicUsize::new(len));
	let bf = Arc::new(move |r:SResult<Record>| {
		match r {
			Ok(rr) => {
				{tab.records.map.write().unwrap().insert(rr.id, rr);}
				if count.fetch_sub(1, Ordering::SeqCst) == 1 {
					init_subtabs(&tab, &subtabs);
					// TODO 从log中读取还未索引的Key Guid
					cb(Ok(tab.clone()))
				}
			},
			Err(s) => cb(Err(s.to_string()))
		}
	});
	for (file, roots) in records.into_iter() {
		Record::new(&dir, file, roots, bf.clone())
	}
}

// 初始化全部的子表
fn init_subtabs(tab:&Tab, subtabs: &SBMap){
	let mut vec = tab.subs.0.write().unwrap();
	for (id, roots) in subtabs.iter() {
		let mut map = OrdMap::new(new());
		for (file, pos) in roots.iter() {
			let root_data = tab.records.get_root(*file, *pos);
			let key = Arc::new(root_data.get(..(tab.cfg.key_size as usize)).unwrap().to_vec());
			map.insert(key, (*file, *pos));
		}
		vec.push(SubTab::new(tab.clone(), *id, map));
	}
}
