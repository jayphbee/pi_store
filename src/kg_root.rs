/**
 * 用一个根日志文件不断追加写，记录所有子表的子节点位置。
 * 以根日志文件写入为准，表示索引记录落地。
 * 支持用指定的key创建新的子表，内存中会克隆sbtree，根文件中追加{块大小(4byte), {子表(4字节), 类型(1byte), 长度(3字节), [子节点位置(2字节或3字节)...]}, Crc32(2byte)}。
 * 根日志文件的格式为： [{块大小(4byte), [{子表key(4字节), 类型(1byte), 数组长度(3字节-可选), [{子节点位置(2字节或3字节)},...]=子节点位置数组, },...]-可选, Crc32(4byte)}, ...]，
 * 类型的0+1位为0表示子表是新创建的，1为更新，2为清空，3为删除。类型的2位为1表示子节点位置为3字节，否则为2字节。
 * 如果为创建或更新，则会有子节点位置数组，子节点位置如果大于最高位为1的数时表示删除该子节点
 * 
 * 如果key为32字节，条目数量超过3亿，则子节点位置2字节就不够用，所以采用动态2字节或3字节。
 */

use std::boxed::FnBox;
use std::vec::Vec;
use std::fs::remove_file;
use std::path::Path;
use std::io::Result as IoResult;

use fnv::FnvHashMap;
use fnv::FnvHashSet;


use pi_lib::data_view::{GetView, SetView};

use pi_base::file::{AsyncFile, AsynFileOptions};

use log::{SResult, Callback, read_3byte};
use kg::Tab;


// 每个子表的子节点表及最新的子表

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
	// 初始化加载根文件，返回所有子表的子节点表及最后一次创建的子表ID
	pub fn init<P: AsRef<Path> + Send + 'static>(mut vec: Vec<(u32, P)>, file: P, tab: Tab, cb: Box<FnBox(SResult<(FnvHashMap<u32, FnvHashSet<u32>>, u32)>)>) {
		match vec.pop() {
			Some((m, path)) => {// 顺序加载根日志文件
				AsyncFile::open(path, AsynFileOptions::ReadAppend(8), Box::new(move |f: IoResult<AsyncFile>| match f {
					Ok(afile) =>{
						let len = afile.get_size();
						afile.read(0, len as usize, Box::new(move |f: AsyncFile, r: IoResult<Vec<u8>>| match r {
							Ok(vec_u8) => {
								let r = read_root(vec_u8);
								if r.0.len() > 0 {
									{
										let mut root = tab.root.lock().unwrap();
										root.file = Ok(f);
										root.modify = m;
									}
									//删除vec中剩余的日志文件
									for (_, f) in vec.into_iter() {
										remove_file(f).expect("remove failed");
									}
									cb(Ok(r))
								}else{
									// 为None表示该根日志文件错误
									// TODO 删除该文件
									RootLog::init(vec, file, tab, cb)
								}
							},
							Err(s) => cb(Err(s.to_string()))
						}));
					},
					Err(s) => cb(Err(s.to_string()))
				}));
			},
			_ => { // 加载空的根日志文件
				AsyncFile::open(file, AsynFileOptions::ReadWrite(8), Box::new(move |f: IoResult<AsyncFile>| match f {
					Ok(afile) => {
						{tab.root.lock().unwrap().file = Ok(afile);}
						// 默认子表id为1
						let mut map = FnvHashMap::with_capacity_and_hasher(0, Default::default());
						map.insert(1, FnvHashSet::with_capacity_and_hasher(0, Default::default()));
						cb(Ok((map, 1)))
					},
					Err(s) => cb(Err(s.to_string()))
				}));
			}
		}
	}
	// 写入一组指定子表的子节点列表，如果没有列表表示删除子表，如果列表为空表示清空子表
	pub fn write(&self, subtabs: Vec<(u32, Option<Vec<u32>>)>, cb: Callback) -> SResult<()> {
		Err("guid too old".to_string())
	}

}
//====================================



//================================ 内部静态方法
/*
* 块的大小
*/
const BLOCK_SIZE: usize = 4;
/*
* 类型的大小
*/
const TYPE_LEN: usize = 1;
/*
* 数组的长度
*/
const ARR_LEN: usize = 3;

/*
* 子表编号的大小
*/
const ST_SIZE: usize = 4;

/*
* 校验码大小
*/
const CRC_SIZE: usize = 4;

/*
* 操作类型的掩码
*/
const TYPE_MASK: usize = 3;

const TYPE_MASK_CREATE: usize = 0;
const TYPE_MASK_CLEAR: usize = 2;
const TYPE_MASK_DELETE: usize = 3;

/*
* 元素长度的掩码
*/
const EL_LEN_MASK: usize = 4;

// 读取根日志文件，返回所有子表的子节点表及最后一次创建的子表ID
fn read_root(data: Vec<u8>) -> (FnvHashMap<u32, FnvHashSet<u32>>, u32) {
	let size = data.len();
	let slice = data.as_slice();
	let mut pos = 0;
	let mut last = 0;
	let mut subtabs = FnvHashMap::with_capacity_and_hasher(0, Default::default());
	while pos + BLOCK_SIZE + ST_SIZE + TYPE_LEN + ARR_LEN + CRC_SIZE < size {
		let s = slice.get_lu32(pos) as usize + pos + BLOCK_SIZE - CRC_SIZE;
		pos+=BLOCK_SIZE;
		// TODO 检查crc32
		while pos + ST_SIZE + TYPE_LEN < s {
			let key = slice.get_lu32(pos);
			pos+=ST_SIZE;
			let t = slice.get_u8(pos) as usize;
			pos+=TYPE_LEN;
			let action = t & TYPE_MASK;
			if action == TYPE_MASK_DELETE {
				subtabs.remove(&key);
				continue;
			}
			// 找到或创建子表
			let sub =subtabs.entry(key).or_insert(FnvHashSet::with_capacity_and_hasher(0, Default::default()));
			if action == TYPE_MASK_CLEAR {
				// 子表清空
				sub.clear();
				continue;
			}
			// 记住最新创建的子表
			if action == TYPE_MASK_CREATE {
				last = key;
			}
			let len = read_3byte(slice, pos) as u32;
			pos+=ARR_LEN;
			// 掩码为0表示子节点位置为2字节
			if t & EL_LEN_MASK == 0 {
				for _ in 0..len {
					let i = slice.get_lu16(pos) as u32;
					if i < 0x7fff {
						sub.insert(i);
					}else{
						// 子节点位置大于0x7fff时表示删除该子节点
						sub.remove(&(i - 0x7fff));
					}
					pos+=2;
				}
			}else{
				for _ in 0..len {
					let i = read_3byte(slice, pos) as u32;
					if i < 0x7fffff {
						sub.insert(i);
					}else{
						// 子节点位置大于0x7fffff时表示删除该子节点
						sub.remove(&(i - 0x7fffff));
					}
					pos+=3;
				}
			}
		}
		pos = s + CRC_SIZE;
	}
	(subtabs, last)
}
