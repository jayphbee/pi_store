/**
 * 用一个根日志文件不断追加写，记录所有子表的子节点位置。
 * 以根日志文件写入为准，表示索引记录落地。
 * 支持用指定的key创建新的子表，内存中会克隆sbtree，根文件中追加{长度(4字节), 子表(4字节), [子节点位置(2字节或3字节)...]}。
 * 根日志文件的格式为： [节点数组长度(4字节，实际为数组长度+1，超过0x7fffffff表示子节点位置为3字节), {子表key(4字节), [{子节点位置(2字节或3字节)},...]=节点数组, }, ...]，子节点位置大于最高位为1时表示删除该子节点
 * 如果块数组长度为0，表示子表被删除，如果块数组长度为1，表示子表被清空
 * 如果块数组长度为0xffffffff，表示初始化成功，根日志文件必须包含一个块数组长度为0xffffffff的数据。
 * 
 * 如果key为32byte，条目数量超过3亿，则子节点位置2字节就不够用，所以采用动态2字节或3字节，
 */


use std::boxed::FnBox;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::vec::Vec;
use std::fs::{File, DirBuilder, rename, remove_file};
use std::path::{Path, PathBuf};
use std::io::Result as IoResult;

use fnv::FnvHashMap;
use fnv::FnvHashSet;

use pi_lib::ordmap::{OrdMap, ActionResult, Entry};
use pi_lib::asbtree::{Tree, new};
use pi_lib::time::now_millis;
use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::data_view::{GetView, SetView};
use pi_lib::bon::{ReadBuffer, WriteBuffer};
use pi_lib::base58::{ToBase58, FromBase58};

use pi_base::file::{AsyncFile, AsynFileOptions};

use log::{Bin, SResult, Callback, read_3byte};
use kg::{BinPos, Tab};
use kg_record::{Record};
use kg_subtab::SubTab;


// 每个子表的子节点表
type SBMap = FnvHashMap<u32, FnvHashSet<u32>>;


// 根日志
pub struct RootLog {
	pub name: String,
	pub modify: u32,
	pub file: SResult<AsyncFile>,
	//wait: Option<FnBox>, //切换根日志文件时的等待函数
}
impl RootLog {
	// 用指定的文件名及后缀（为修改次数）创建根日志
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
* 数组的长度
*/
const ARR_LEN: usize = 4;

/*
* 子表编号的大小
*/
const ST_SIZE: usize = 4;

/*
* 特殊的块长度，表示初始化写入成功
*/
const INIT_FLAG: u32 = 0xffffffff;


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
								Some(subtabs) => {
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
									next_open(tab, Arc::new(subtabs), cb);
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
fn read_root(data: Vec<u8>) -> Option<SBMap> {
	let mut init = false;
	let size = data.len();
	let slice = data.as_slice();
	let mut pos = 0;
	let mut subtabs: SBMap = FnvHashMap::with_capacity_and_hasher(0, Default::default());
	while pos + ARR_LEN + ST_SIZE < size {
		let mut len = slice.get_lu32(pos);
		pos+=ARR_LEN;
		//如果块数组长度为0xffffffff，表示初始化成功，根日志文件必须包含一个块数组长度为0xffffffff的数据
		if len == INIT_FLAG {
			init = true;
			continue;
		}
		let key = slice.get_lu32(pos);
		pos+=ST_SIZE;
		// 如果块数组长度为0，表示子表被删除
		if len == 0 {
			subtabs.remove(&key);
			continue;
		}
		// 找到或创建子表
		let sub = subtabs.entry(key).or_insert(FnvHashSet::with_capacity_and_hasher(0, Default::default()));
		// 如果块数组长度为0，表示子表清空
		if len == 0 {
			sub.clear();
			continue;
		}
		// 超过0x7fffffff表示子节点位置为3字节
		if len > 0x7fffffff {
			let limit = pos + ((len - 0x7fffffff) as usize - 1) * 3;
			if size < limit {
				return None
			}
			while pos < limit {
				let i = read_3byte(slice, pos) as u32;
				if i < 0x7fffff {
					sub.insert(i);
				}else{
					// 子节点位置大于0x7fffff时表示删除该子节点
					sub.remove(&(i - 0x7fffff));
				}
				pos+=3;
			}
		}else{
			let limit = pos + (len as usize - 1) * 2;
			if size < limit {
				return None
			}
			while pos < limit {
				let i = slice.get_lu16(pos) as u32;
				if i < 0x7fff {
					sub.insert(i);
				}else{
					// 子节点位置大于0x7fff时表示删除该子节点
					sub.remove(&(i - 0x7fff));
				}
				pos+=2;
			}
		}
		
	}
	if init {
		Some(subtabs)
	}else{
		None
	}
}

// 打开全部的记录文件，并加载全部的根块，然后初始化子表
fn next_open(tab:Tab, subtabs: Arc<SBMap>, cb: Arc<Fn(SResult<Tab>)>){
	let len = subtabs.len();
	if len == 0 {
		return cb(Ok(tab))
	}
	Record::init(tab.record.clone(), subtabs.clone(), Box::new(move |r:SResult<FnvHashMap<u32, Bin>>| {
		match r {
			Ok(rr) => {
				init_subtabs(&tab, &subtabs, rr);
				// TODO 从log中读取还未索引的Key Guid
				cb(Ok(tab.clone()))
			},
			Err(s) => cb(Err(s.to_string()))
		}
	}))
}

// 初始化全部的子表
fn init_subtabs(tab:&Tab, subtabs: &SBMap, nodes: FnvHashMap<u32, Bin>){
	let mut sub_map = tab.subs.0.write().unwrap();
	let empty = tab.record.read().unwrap().node_empty();
	for (id, pos_set) in subtabs.iter() {
		let mut map = OrdMap::new(new());
		for pos in pos_set.iter() {
			// 获取子节点
			let bin = nodes.get(pos).unwrap();
			// 获取子节点的第一个键
			let key = Arc::new(bin.get(..(tab.cfg.key_size as usize)).unwrap().to_vec());
			map.insert(key, BinPos::new(empty.clone(), bin.clone(), *pos));
		}
		sub_map.insert(*id, SubTab::new(tab.clone(), *id, map));
	}
}
